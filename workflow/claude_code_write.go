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
)

const (
	// implementTimeout bounds one coding run. Writing takes longer than
	// reading: the run builds, tests, and comes back from its own mistakes.
	implementTimeout = 2 * time.Hour
	inspectTimeout   = 2 * time.Minute
	pushTimeout      = 10 * time.Minute
	inspectAttempts  = 2
	pushAttempts     = 2

	// implementPermissionMode auto-accepts edits. It does not cover Bash,
	// which is why the git commands below are named explicitly: without them
	// the run edits files and its commit is denied, leaving changes that die
	// with the workspace.
	implementPermissionMode = "acceptEdits"

	branchPrefix = "agent/"
	maxSlugLen   = 32
)

// implementGitTools are the git commands the run needs to do its own
// committing. Splitting the changes and writing the messages is the part worth
// paying a coding agent for; publishing them is the workflow's job.
var implementGitTools = []string{
	"Bash(git add:*)",
	"Bash(git commit:*)",
	"Bash(git status:*)",
	"Bash(git diff:*)",
	"Bash(git log:*)",
	"Bash(git show:*)",
}

// implementDeniedTools keeps publishing out of the run's hands. It is a guard
// rail, not a wall: a run that can execute commands can reach a mounted key by
// other means. What actually bounds the damage is the key's own scope.
var implementDeniedTools = []string{
	"Bash(git push:*)",
	"Bash(git remote:*)",
	"Bash(git config:*)",
}

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
	return withContent(implementFeature(ctx, rawInput))
}

func implementFeature(ctx workflow.Context, rawInput json.RawMessage) (ClaudeCodeOutput, error) {
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

	runCtx, err := openRun(ctx, implementSessionTimeout)
	if err != nil {
		out.Error = err.Error()
		return out, nil
	}
	defer workflow.CompleteSession(runCtx)

	var prepared activity.PrepareWorkspaceOutput
	err = workflow.ExecuteActivity(
		onRunWorker(runCtx, workflow.ActivityOptions{
			StartToCloseTimeout: prepareTimeout,
			HeartbeatTimeout:    gitHeartbeatTimeout,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: prepareAttempts},
		}),
		ccAct.PrepareWorkspace,
		activity.PrepareWorkspaceInput{Name: name, Repo: input.Repo, Ref: input.Base, Branch: branch},
	).Get(runCtx, &prepared)
	if err != nil {
		if workerLost(runCtx, err) {
			out.Error = workerStopped("while it cloned the repository", "nothing was done")
		} else {
			out.Error = fmt.Sprintf("could not prepare the workspace: %v", err)
		}
		return out, nil
	}
	out.Commit = prepared.Commit
	defer cleanupWorkspace(runCtx, prepared.Dir)

	var result claudeCodeResult
	runErr := workflow.ExecuteActivity(
		onRunWorker(runCtx, workflow.ActivityOptions{
			StartToCloseTimeout: implementTimeout,
			HeartbeatTimeout:    analyzeHeartbeat,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		}),
		ccAct.RunClaudeCode,
		activity.RunClaudeCodeInput{
			Dir:             prepared.Dir,
			Task:            input.Task,
			PermissionMode:  implementPermissionMode,
			AllowedTools:    implementGitTools,
			DisallowedTools: implementDeniedTools,
			MaxBudgetUSD:    input.MaxBudgetUSD,
		},
	).Get(runCtx, &result)

	// The commits are in the clone, on the lost worker's disk: no other
	// worker can inspect or push them.
	if workerLost(runCtx, runErr) {
		out.Error = workerStopped("before the run finished", "nothing was pushed")
		return out, nil
	}

	out.Report = result.Report
	out.CostUSD = result.CostUSD
	out.PaidBy = result.PaidBy
	out.DurationMS = result.DurationMS
	out.NumTurns = result.NumTurns
	out.ToolUses = result.ToolUses

	// A run that failed may still have committed something worth keeping, so
	// inspect the tree either way and let the commits decide.
	if runErr != nil {
		out.Error = fmt.Sprintf("the run did not complete: %v", runErr)
	} else if result.IsError {
		out.Error = fmt.Sprintf("the run reported a failure (%s)", result.Subtype)
	}

	var inspected activity.InspectWorkspaceOutput
	if err := workflow.ExecuteActivity(
		onRunWorker(runCtx, workflow.ActivityOptions{
			StartToCloseTimeout: inspectTimeout,
			HeartbeatTimeout:    gitHeartbeatTimeout,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: inspectAttempts},
		}),
		ccAct.InspectWorkspace,
		activity.InspectWorkspaceInput{Dir: prepared.Dir, Base: prepared.Commit, Branch: branch},
	).Get(runCtx, &inspected); err != nil {
		if workerLost(runCtx, err) {
			out.Error = joinErrors(out.Error, workerStopped("before the commits were checked", "nothing was pushed"))
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
		onRunWorker(runCtx, workflow.ActivityOptions{
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
	).Get(runCtx, nil); err != nil {
		if workerLost(runCtx, err) {
			// The push may have reached the remote before the worker went.
			out.Error = joinErrors(out.Error, workerStopped("during the push",
				fmt.Sprintf("the branch may or may not have been published; check %s on the remote", branch)))
		} else {
			out.Error = joinErrors(out.Error, fmt.Sprintf("the commits were not pushed: %v", err))
		}
		return out, nil
	}
	out.Pushed = true
	return out, nil
}

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
