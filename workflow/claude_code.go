package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
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

// AnalyzeRepoInput is what the calling agent gets to decide. Everything else —
// permissions, timeouts, where the clone lives, that it is deleted afterwards —
// belongs to the workflow.
type AnalyzeRepoInput struct {
	Repo  string `json:"repo"`
	Task  string `json:"task"`
	Ref   string `json:"ref,omitempty"`
	Model string `json:"model,omitempty"`
}

// ClaudeCodeOutput is what a coding workflow returns. Error carries a run that
// failed rather than failing the workflow, so a partial report survives — the
// same reason AgentWorkflow reports its failures in its output.
type ClaudeCodeOutput struct {
	Report string `json:"report"`
	Error  string `json:"error,omitempty"`

	Repo   string `json:"repo,omitempty"`
	Ref    string `json:"ref,omitempty"`
	Commit string `json:"commit,omitempty"`

	CostUSD    float64        `json:"cost_usd,omitempty"`
	DurationMS int64          `json:"duration_ms,omitempty"`
	NumTurns   int            `json:"num_turns,omitempty"`
	ToolUses   map[string]int `json:"tool_uses,omitempty"`
}

// AnalyzeRepoWorkflow reads a repository and answers a question about it. It
// clones, runs Claude Code with no write access, and deletes the clone.
//
// It is not a sub-agent: no LLM decides the steps. The calling agent says what
// to look at and what to find out; the code decides how, which is what keeps
// "read-only" a property of the system rather than a promise in a prompt.
func AnalyzeRepoWorkflow(ctx workflow.Context, rawInput json.RawMessage) (ClaudeCodeOutput, error) {
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

	// The run id names the workspace: unique per execution, and stable across
	// a replay, so a retried PrepareWorkspace reuses the same directory.
	name := "run-" + workflow.GetInfo(ctx).WorkflowExecution.RunID

	var prepared activity.PrepareWorkspaceOutput
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: prepareTimeout,
			HeartbeatTimeout:    gitHeartbeatTimeout,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
		}),
		ccAct.PrepareWorkspace,
		activity.PrepareWorkspaceInput{Name: name, Repo: input.Repo, Ref: input.Ref},
	).Get(ctx, &prepared)
	if err != nil {
		out.Error = fmt.Sprintf("could not prepare the workspace: %v", err)
		return out, nil
	}
	out.Commit = prepared.Commit

	// Deleting the clone is the one step that must happen on every path out,
	// including a cancelled workflow — and a cancelled workflow cannot start
	// an activity on its own context.
	defer func() {
		cleanupCtx, cancel := workflow.NewDisconnectedContext(ctx)
		defer cancel()
		_ = workflow.ExecuteActivity(
			workflow.WithActivityOptions(cleanupCtx, workflow.ActivityOptions{
				StartToCloseTimeout: cleanupTimeout,
				RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
			}),
			ccAct.CleanupWorkspace,
			activity.CleanupWorkspaceInput{Dir: prepared.Dir},
		).Get(cleanupCtx, nil)
	}()

	var result claudeCodeResult
	err = workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: analyzeTimeout,
			HeartbeatTimeout:    analyzeHeartbeat,
			// Never retried: a run costs real money and has already changed
			// the workspace by the time it fails.
			RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1},
		}),
		ccAct.RunClaudeCode,
		activity.RunClaudeCodeInput{
			Dir:            prepared.Dir,
			Task:           input.Task,
			Model:          input.Model,
			PermissionMode: analyzePermissionMode,
		},
	).Get(ctx, &result)
	if err != nil {
		out.Error = fmt.Sprintf("the analysis did not complete: %v", err)
		return out, nil
	}

	out.Report = result.Report
	out.CostUSD = result.CostUSD
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
	fmt.Fprintf(&sb, "run: %d turns, %s, $%.4f",
		o.NumTurns, (time.Duration(o.DurationMS) * time.Millisecond).Round(time.Second), o.CostUSD)
	return sb.String()
}

func shortCommit(c string) string {
	if len(c) > 8 {
		return c[:8]
	}
	return c
}
