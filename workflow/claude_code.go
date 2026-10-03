package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/tool"
)

const (
	// analyzeTimeout bounds one analysis run. Claude Code exploring a codebase
	// is measured in minutes, not seconds — the point of running it as a
	// workflow rather than a plain tool activity, which is capped at 120s.
	analyzeTimeout = 45 * time.Minute
	// analyzeHeartbeat is what turns a stuck run into a failure in two minutes
	// instead of forty-five. The runner beats on every event it parses.
	analyzeHeartbeat = 2 * time.Minute
	// prepareTimeout bounds the clone.
	prepareTimeout = 15 * time.Minute
	// gitHeartbeatTimeout must exceed the interval the git activity beats at.
	gitHeartbeatTimeout = 60 * time.Second
	cleanupTimeout      = 2 * time.Minute

	// analyzePermissionMode is what makes this workflow read-only. It is set
	// here and never taken from the input: an agent asking for an analysis
	// must not be able to talk its way into write access.
	analyzePermissionMode = "plan"
)

// analyzeSystemPrompt tells the run what it is: the CLI otherwise behaves as
// in an interactive session, and ends a report offering to make the changes
// or asking what to do next — which no one will answer, and which the agent
// reading the report repeats to its user as a promise.
const analyzeSystemPrompt = `This is a one-shot, read-only analysis. Nothing you change is kept: the clone is deleted when you finish, and nothing can be committed or pushed.
Your final message is a report read by another agent, not by a person, and no one will reply to it. End with the report: do not offer to make changes, to start on fixes, or to continue, and do not ask questions.
If a command you need is refused, say that it was refused, not that a tool is missing.`

// AnalyzeRepoInput is what the calling agent gets to decide. Everything else —
// permissions, timeouts, where the clone lives, that it is deleted afterwards —
// belongs to the workflow.
type AnalyzeRepoInput struct {
	Repo string `json:"repo"`
	Task string `json:"task"`
	Ref  string `json:"ref,omitempty"`
	// The caller's, never the model's (tool.WithCallContext): where to tell
	// the user that the run waits for a worker.
	tool.CallContext
}

// ClaudeCodeOutput is what a coding workflow returns. Error carries a run that
// failed rather than failing the workflow, so a partial report survives — the
// same reason AgentWorkflow reports its failures in its output.
type ClaudeCodeOutput struct {
	// Content is what the calling agent reads (tool.Result): Summary(), set
	// once the run is over.
	Content string `json:"content"`

	Report string `json:"report"`
	Error  string `json:"error,omitempty"`

	Repo   string `json:"repo,omitempty"`
	Ref    string `json:"ref,omitempty"`
	Commit string `json:"commit,omitempty"`

	// Set by the workflows that write. Branch is named before the run starts,
	// so it is reported even when nothing was pushed to it.
	Branch  string                `json:"branch,omitempty"`
	Commits []activity.CommitInfo `json:"commits,omitempty"`
	Pushed  bool                  `json:"pushed,omitempty"`
	// Dirty reports changes the run left uncommitted; they died with the clone.
	Dirty bool `json:"dirty,omitempty"`

	CostUSD float64 `json:"cost_usd,omitempty"`
	// PaidBy says what paid the run: "subscription" or "api" when the
	// worker's choice and the CLI's word agree, else empty.
	PaidBy     string         `json:"paid_by,omitempty"`
	DurationMS int64          `json:"duration_ms,omitempty"`
	NumTurns   int            `json:"num_turns,omitempty"`
	ToolUses   map[string]int `json:"tool_uses,omitempty"`
}

// AnalyzeRepoWorkflow reads a repository and answers a question about it. It
// clones, runs Claude Code with no write access, and deletes the clone, all on
// the one worker whose disk holds the clone (openRun).
//
// It is not a sub-agent: no LLM decides the steps. The calling agent says what
// to look at and what to find out; the code decides how, which is what keeps
// "read-only" a property of the system rather than a promise in a prompt.
func AnalyzeRepoWorkflow(ctx workflow.Context, rawInput json.RawMessage) (ClaudeCodeOutput, error) {
	return withContent(analyzeRepo(ctx, rawInput))
}

// withContent fills in what the calling agent reads of a coding run's output.
func withContent(out ClaudeCodeOutput, err error) (ClaudeCodeOutput, error) {
	if err == nil {
		out.Content = out.Summary()
	}
	return out, err
}

func analyzeRepo(ctx workflow.Context, rawInput json.RawMessage) (ClaudeCodeOutput, error) {
	var input AnalyzeRepoInput
	if err := json.Unmarshal(rawInput, &input); err != nil {
		return ClaudeCodeOutput{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("invalid analyze_repo input: %v", err), "InvalidInput", nil)
	}
	if strings.TrimSpace(input.Repo) == "" || strings.TrimSpace(input.Task) == "" {
		return ClaudeCodeOutput{}, temporal.NewNonRetryableApplicationError(
			"analyze_repo requires repo and task", "InvalidInput", nil)
	}

	out := ClaudeCodeOutput{Repo: input.Repo, Ref: input.Ref}
	var ccAct *activity.ClaudeCodeActivities

	r, err := openRun(ctx, analyzeSessionTimeout, input.CallContext)
	if err != nil {
		out.Error = err.Error()
		return out, nil
	}
	defer r.complete()

	// The run id names the workspace: unique per execution, and stable across
	// a replay, so a retried PrepareWorkspace reuses the same directory.
	name := activity.RunWorkspacePrefix + workflow.GetInfo(ctx).WorkflowExecution.RunID

	var prepared activity.PrepareWorkspaceOutput
	err = workflow.ExecuteActivity(
		r.step(workflow.ActivityOptions{
			StartToCloseTimeout: prepareTimeout,
			HeartbeatTimeout:    gitHeartbeatTimeout,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: prepareAttempts},
		}),
		ccAct.PrepareWorkspace,
		activity.PrepareWorkspaceInput{Name: name, Repo: input.Repo, Ref: input.Ref},
	).Get(r.ctx, &prepared)
	if err != nil {
		if r.failed(err) {
			out.Error = r.lostAt("while it cloned the repository", "nothing was done")
		} else {
			out.Error = fmt.Sprintf("could not prepare the workspace: %v", err)
		}
		return out, nil
	}
	out.Commit = prepared.Commit
	defer r.cleanup(prepared.Dir)

	var result claudeCodeResult
	err = workflow.ExecuteActivity(
		r.step(workflow.ActivityOptions{
			StartToCloseTimeout: analyzeTimeout,
			HeartbeatTimeout:    analyzeHeartbeat,
			// Never retried: a run costs real money and has already changed
			// the workspace by the time it fails.
			RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
		}),
		ccAct.RunClaudeCode,
		activity.RunClaudeCodeInput{
			Dir:                prepared.Dir,
			Task:               input.Task,
			PermissionMode:     analyzePermissionMode,
			AppendSystemPrompt: analyzeSystemPrompt,
		},
	).Get(r.ctx, &result)
	if err != nil {
		if r.failed(err) {
			out.Error = r.lostAt("before the analysis finished", "there is no report")
		} else {
			out.Error = fmt.Sprintf("the analysis did not complete: %v", err)
		}
		return out, nil
	}

	out.Report = result.Report
	out.CostUSD = result.CostUSD
	out.PaidBy = result.PaidBy
	out.DurationMS = result.DurationMS
	out.NumTurns = result.NumTurns
	out.ToolUses = result.ToolUses
	if result.IsError {
		out.Error = fmt.Sprintf("the run reported a failure (%s)", result.Subtype)
	}
	return out, nil
}

// claudeCodeResult mirrors the fields of claudecode.Result this package reads.
// Declaring it here keeps the workflow package free of a dependency on the
// process-running one, which a workflow must never reach for.
type claudeCodeResult struct {
	Report     string         `json:"report"`
	IsError    bool           `json:"is_error"`
	Subtype    string         `json:"subtype"`
	NumTurns   int            `json:"num_turns"`
	DurationMS int64          `json:"duration_ms"`
	CostUSD    float64        `json:"cost_usd"`
	ToolUses   map[string]int `json:"tool_uses"`
	// PaidBy is "subscription" or "api" when the worker's way of
	// authenticating and the CLI's word agree (claudecode.Result.Payer).
	PaidBy string `json:"paid_by"`
}

// Summary renders the output for the calling agent: the report, then the facts
// it cannot see from the text alone.
func (o ClaudeCodeOutput) Summary() string {
	var sb strings.Builder
	if o.Error != "" {
		sb.WriteString(o.Error)
		sb.WriteString("\n\n")
	}
	if o.Report != "" {
		sb.WriteString(o.Report)
		sb.WriteString("\n\n")
	}
	sb.WriteString("---\n")
	if o.Commit != "" {
		ref := o.Ref
		if ref == "" {
			ref = "default branch"
		}
		fmt.Fprintf(&sb, "repo: %s (%s, %s)\n", o.Repo, ref, shortCommit(o.Commit))
	}
	if o.Branch != "" {
		if o.Pushed {
			fmt.Fprintf(&sb, "branch: %s (pushed, %d commits)\n", o.Branch, len(o.Commits))
		} else {
			fmt.Fprintf(&sb, "branch: %s (not pushed)\n", o.Branch)
		}
		for _, c := range o.Commits {
			fmt.Fprintf(&sb, "  %s %s\n", shortCommit(c.SHA), c.Subject)
		}
	}
	if o.Dirty {
		sb.WriteString("note: the run left uncommitted changes, which were discarded with the clone\n")
	}
	fmt.Fprintf(&sb, "run: %d turns, %s, $%.4f",
		o.NumTurns, (time.Duration(o.DurationMS) * time.Millisecond).Round(time.Second), o.CostUSD)
	switch o.PaidBy {
	case "subscription":
		sb.WriteString(" (an estimate: paid by a Claude subscription, not billed)")
	case "api":
		sb.WriteString(" (billed to the Anthropic API)")
	}
	return sb.String()
}

func shortCommit(c string) string {
	if len(c) > 8 {
		return c[:8]
	}
	return c
}
