package workflow

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/tool"
)

const (
	// implementTimeout bounds one coding run. Writing takes longer than
	// reading: the run builds, tests, and comes back from its own mistakes.
	implementTimeout = 2 * time.Hour
	inspectTimeout   = 2*time.Minute + activity.DefaultRunEndWait
	pushTimeout      = 10*time.Minute + activity.DefaultRunEndWait
	inspectAttempts  = 2
	pushAttempts     = 2

	// What a run may do is set here, the same on a user's machine
	// (machine.Implement*): never taken from the input.
	implementPermissionMode = machine.ImplementPermissionMode
	implementSystemPrompt   = machine.ImplementSystemPrompt

	branchPrefix = machine.BranchPrefix
	maxSlugLen   = 32
)

// implementGitTools are the git commands the run needs to commit its own
// work, and implementDeniedTools what keeps publishing out of its hands
// (machine.ImplementAllowedTools, ImplementDeniedTools).
var (
	implementGitTools    = machine.ImplementAllowedTools
	implementDeniedTools = machine.ImplementDeniedTools
)

// ImplementFeatureInput is what the calling agent decides: which repository,
// from which base, and what to do. The branch, the permissions and whether
// anything is published belong to the workflow.
type ImplementFeatureInput struct {
	Repo  string `json:"repo"`
	Task  string `json:"task"`
	Base  string `json:"base,omitempty"`
	Title string `json:"title,omitempty"`
	// MaxBudgetUSD stops the run once it has spent this much. It can only
	// lower the worker's own cap (CLAUDE_CODE_MAX_BUDGET_USD); zero = that
	// cap alone.
	MaxBudgetUSD float64 `json:"max_budget_usd,omitempty"`
	// The caller's, never the model's (tool.WithCallContext): where to tell
	// the user that the run waits for a worker.
	tool.CallContext
}

// ImplementFeatureWorkflow makes a change to a repository and publishes it as
// a branch: clone, branch, run Claude Code, check what it actually produced,
// push, delete the clone. Every step runs on the one worker whose disk holds
// the clone (openRun): a push from another would find no commit to publish.
//
// The run writes the commits; the workflow does the clone, the branch and the
// push. That split is not stylistic: the push is the only step that holds a
// credential, and it is the one step no LLM takes part in.
func ImplementFeatureWorkflow(ctx workflow.Context, rawInput json.RawMessage) (ClaudeCodeOutput, error) {
	return withContent(implementFeature(ctx, rawInput, nil))
}

// ImplementFallbackWorkflow is ImplementFeatureWorkflow as ImplementRunWorkflow
// starts it on its fallback queue, after its probe: the queue is not asked
// again.
func ImplementFallbackWorkflow(ctx workflow.Context, in ImplementFallbackInput) (ClaudeCodeOutput, error) {
	return withContent(implementFeature(ctx, in.Input, &in.Probe))
}

func implementFeature(ctx workflow.Context, rawInput json.RawMessage, probed *activity.ProbeRunWorkerOutput) (ClaudeCodeOutput, error) {
	var input ImplementFeatureInput
	if err := json.Unmarshal(rawInput, &input); err != nil {
		return ClaudeCodeOutput{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("invalid implement_feature input: %v", err), "InvalidInput", nil)
	}
	if strings.TrimSpace(input.Repo) == "" || strings.TrimSpace(input.Task) == "" {
		return ClaudeCodeOutput{}, temporal.NewNonRetryableApplicationError(
			"implement_feature requires repo and task", "InvalidInput", nil)
	}

	info := workflow.GetInfo(ctx)
	name := activity.RunWorkspacePrefix + info.WorkflowExecution.RunID
	branch := branchName(input.Title, input.Task, info.WorkflowExecution.RunID)

	out := ClaudeCodeOutput{Repo: input.Repo, Ref: input.Base, Branch: branch}
	var ccAct *activity.ClaudeCodeActivities

	r, err := openRun(ctx, implementSessionTimeout, input.CallContext, probed)
	if err != nil {
		out.Error = err.Error()
		return out, nil
	}
	defer r.complete()

	var prepared activity.PrepareWorkspaceOutput
	err = workflow.ExecuteActivity(
		r.step(workflow.ActivityOptions{
			StartToCloseTimeout: prepareTimeout,
			HeartbeatTimeout:    gitHeartbeatTimeout,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: prepareAttempts},
		}),
		ccAct.PrepareWorkspace,
		activity.PrepareWorkspaceInput{Name: name, Repo: input.Repo, Ref: input.Base, Branch: branch},
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
	runStarted := workflow.Now(ctx)
	runErr := workflow.ExecuteActivity(
		r.step(workflow.ActivityOptions{
			StartToCloseTimeout: implementTimeout,
			HeartbeatTimeout:    claudeCodeHeartbeat,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		}),
		ccAct.RunClaudeCode,
		activity.RunClaudeCodeInput{
			Dir:                prepared.Dir,
			Task:               input.Task,
			PermissionMode:     implementPermissionMode,
			AllowedTools:       implementGitTools,
			DisallowedTools:    implementDeniedTools,
			AppendSystemPrompt: implementSystemPrompt,
			MaxBudgetUSD:       input.MaxBudgetUSD,
			Outputs:            true,
		},
	).Get(r.ctx, &result)
	ran := workflow.Now(ctx).Sub(runStarted)
	if runErr == nil || !r.failed(runErr) {
		out.publishOutputs(ctx, r, prepared.Dir, input.CallContext)
	}

	// The commits are in the clone, on the lost worker's disk: no other
	// worker can inspect or push them.
	if r.failed(runErr) {
		out.Error = r.lostAt("before the run finished", "nothing was pushed")
		out.interrupted(runErr, ran)
		return out, nil
	}

	// A run that failed may still have committed something worth keeping, so
	// inspect the tree either way and let the commits decide.
	if runErr != nil {
		out.Error = "the run did not complete: " + whyEnded(runErr, implementTimeout)
		out.interrupted(runErr, ran)
	} else {
		out.Report = result.Report
		out.CostUSD = result.CostUSD
		out.PaidBy = result.PaidBy
		out.DurationMS = result.DurationMS
		out.NumTurns = result.NumTurns
		out.ToolUses = result.ToolUses
		if result.IsError {
			out.Error = fmt.Sprintf("the run reported a failure (%s)", result.Subtype)
		}
	}

	var inspected activity.InspectWorkspaceOutput
	if err := workflow.ExecuteActivity(
		r.step(workflow.ActivityOptions{
			StartToCloseTimeout: inspectTimeout,
			HeartbeatTimeout:    gitHeartbeatTimeout,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: inspectAttempts},
		}),
		ccAct.InspectWorkspace,
		activity.InspectWorkspaceInput{Dir: prepared.Dir, Base: prepared.Commit, Branch: branch},
	).Get(r.ctx, &inspected); err != nil {
		if r.failed(err) {
			out.Error = joinErrors(out.Error, r.lostAt("before the commits were checked", "nothing was pushed"))
		} else if hasErrorType(err, activity.ErrRunStillActive) {
			out.Error = joinErrors(out.Error, stillActive)
		} else {
			out.Error = joinErrors(out.Error, fmt.Sprintf("could not inspect the workspace: %v", err))
		}
		return out, nil
	}
	out.Commits = inspected.Commits
	out.Dirty = inspected.Dirty

	// The run has no reason to touch the repository's git configuration, and
	// what it could put there redirects or rides on the push.
	if inspected.GitConfigChanged {
		out.Error = joinErrors(out.Error,
			"the run changed the repository's git configuration (.git/config), so nothing was pushed")
		return out, nil
	}

	// A run that changed nothing is a failure, not a quiet success. Silently
	// doing nothing is the most expensive outcome to diagnose, and we know it
	// happens: a denied git command leaves exactly this state.
	if len(inspected.Commits) == 0 {
		out.Error = joinErrors(out.Error, "the run produced no commit, so nothing was pushed")
		return out, nil
	}
	if inspected.Branch != branch {
		out.Error = joinErrors(out.Error, fmt.Sprintf(
			"the run left HEAD on %q instead of %q, so nothing was pushed", inspected.Branch, branch))
		return out, nil
	}

	if err := workflow.ExecuteActivity(
		r.step(workflow.ActivityOptions{
			StartToCloseTimeout: pushTimeout,
			HeartbeatTimeout:    gitHeartbeatTimeout,
			// A push either lands or it does not; retrying a rejected one
			// just repeats the rejection.
			RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: pushAttempts},
		}),
		ccAct.PushBranch,
		// The newest commit the inspection listed, not the branch as it
		// stands at the push: what is published is what out.Commits says.
		activity.PushBranchInput{
			Dir:    prepared.Dir,
			Remote: input.Repo,
			Branch: branch,
			Commit: inspected.Commits[0].SHA,
		},
	).Get(r.ctx, nil); err != nil {
		if r.failed(err) {
			// The push may have reached the remote before the worker went.
			out.Error = joinErrors(out.Error, r.lostAt("during the push",
				fmt.Sprintf("the branch may or may not have been published; check %s on the remote", branch)))
		} else if hasErrorType(err, activity.ErrRunStillActive) {
			out.Error = joinErrors(out.Error, stillActive)
		} else {
			out.Error = joinErrors(out.Error, fmt.Sprintf("the commits were not pushed: %v", err))
		}
		return out, nil
	}
	out.Pushed = true
	return out, nil
}

// stillActive is what the output says of a run whose CLI still ran on its
// worker when its commits were to be checked or pushed, past the wait for it
// (activity.ErrRunStillActive): what it would still write is unknown.
const stillActive = "the run was still active on its worker after it was given up on, so nothing was pushed"

func joinErrors(existing, add string) string {
	if existing == "" {
		return add
	}
	return existing + "; " + add
}

// branchName builds "agent/<slug>-<id>". The id keeps two runs of the same
// task apart, so a push never collides with an earlier attempt.
func branchName(title, task, runID string) string {
	slug := slugify(title)
	if slug == "" {
		slug = slugify(task)
	}
	if slug == "" {
		slug = "run"
	}
	id := strings.ReplaceAll(runID, "-", "")
	if len(id) > 8 {
		id = id[:8]
	}
	return branchPrefix + slug + "-" + id
}

// slugify reduces free text to what a git ref accepts: lowercase words joined
// by dashes, cut at a word boundary.
func slugify(s string) string {
	var b strings.Builder
	lastDash := true
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', unicode.IsDigit(r):
			b.WriteRune(r)
			lastDash = false
		case !lastDash && b.Len() < maxSlugLen:
			b.WriteByte('-')
			lastDash = true
		}
		if b.Len() >= maxSlugLen {
			break
		}
	}
	return strings.Trim(b.String(), "-")
}
