package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/tool"
)

type implementEnv struct {
	env      *testsuite.TestWorkflowEnvironment
	queues   *activityQueues
	run      *activity.RunClaudeCodeInput
	inspects int
	// duringRun, when set, is what the run does instead of returning at once.
	duringRun func(ctx context.Context) error
	// inspectErr, when set, is what the inspection fails with.
	inspectErr error
	pushed     *activity.PushBranchInput
	cleaned    []string
	inspected  activity.InspectWorkspaceOutput
	// outputs is what PublishOutputs returns; published, what it was asked.
	outputs   activity.PublishOutputsOutput
	published *activity.PublishOutputsInput
	// prepareErr, when set, is what the preparation fails with.
	prepareErr error
	// bundle and bundleErr are what BundleBranch returns; bundled, what it
	// was asked.
	bundle    tool.FileRef
	bundleErr error
	bundled   *activity.BundleBranchInput
}

func newImplementEnv(t *testing.T, result claudeCodeResult, runErr error, inspected activity.InspectWorkspaceOutput, pushErr error) *implementEnv {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	e := &implementEnv{env: suite.NewTestWorkflowEnvironment(), inspected: inspected}
	e.queues = asRunWorker(e.env)

	e.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PrepareWorkspaceInput) (activity.PrepareWorkspaceOutput, error) {
		if e.prepareErr != nil {
			return activity.PrepareWorkspaceOutput{}, e.prepareErr
		}
		return activity.PrepareWorkspaceOutput{Dir: "/work/" + in.Name, Commit: "base0000", Branch: in.Branch}, nil
	}, sdkactivity.RegisterOptions{Name: "PrepareWorkspace"})

	e.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.RunClaudeCodeInput) (claudeCodeResult, error) {
		e.run = &in
		if e.duringRun != nil {
			return result, e.duringRun(ctx)
		}
		return result, runErr
	}, sdkactivity.RegisterOptions{Name: "RunClaudeCode"})

	e.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.InspectWorkspaceInput) (activity.InspectWorkspaceOutput, error) {
		e.inspects++
		if e.inspectErr != nil {
			return activity.InspectWorkspaceOutput{}, e.inspectErr
		}
		out := e.inspected
		if out.Branch == "" {
			out.Branch = in.Branch
		}
		return out, nil
	}, sdkactivity.RegisterOptions{Name: "InspectWorkspace"})

	e.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PushBranchInput) error {
		e.pushed = &in
		return pushErr
	}, sdkactivity.RegisterOptions{Name: "PushBranch"})

	e.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.CleanupWorkspaceInput) error {
		e.cleaned = append(e.cleaned, in.Dir)
		return nil
	}, sdkactivity.RegisterOptions{Name: "CleanupWorkspace"})

	e.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PublishOutputsInput) (activity.PublishOutputsOutput, error) {
		e.published = &in
		return e.outputs, nil
	}, sdkactivity.RegisterOptions{Name: "PublishOutputs"})

	e.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.BundleBranchInput) (tool.FileRef, error) {
		e.bundled = &in
		return e.bundle, e.bundleErr
	}, sdkactivity.RegisterOptions{Name: "BundleBranch"})

	return e
}

func (e *implementEnv) run_(t *testing.T, input ImplementFeatureInput) ClaudeCodeOutput {
	t.Helper()
	raw, _ := json.Marshal(input)
	e.env.ExecuteWorkflow(ImplementFeatureWorkflow, json.RawMessage(raw))
	if !e.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := e.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var out ClaudeCodeOutput
	if err := e.env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	return out
}

func oneCommit() activity.InspectWorkspaceOutput {
	return activity.InspectWorkspaceOutput{
		Commits: []activity.CommitInfo{{SHA: "abcdef1234", Subject: "feat: the thing"}},
	}
}

func TestImplementFeatureWorkflow_HappyPath(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "Done.", Subtype: "success", CostUSD: 0.42}, nil, oneCommit(), nil)

	out := e.run_(t, ImplementFeatureInput{Repo: "git@host:org/repo.git", Task: "add the thing", Title: "Add The Thing", Base: "main"})

	if out.Error != "" {
		t.Errorf("Error = %q, want none", out.Error)
	}
	if !out.Pushed || len(out.Commits) != 1 {
		t.Errorf("Pushed = %v, commits = %v", out.Pushed, out.Commits)
	}
	if !strings.HasPrefix(out.Branch, "agent/add-the-thing-") {
		t.Errorf("Branch = %q, want it derived from the title", out.Branch)
	}
	// The push takes its URL from the input, never from the working tree the
	// run had write access to.
	if e.pushed.Remote != "git@host:org/repo.git" || e.pushed.Branch != out.Branch {
		t.Errorf("push = %+v", e.pushed)
	}
	// Nor the commit, which is the one the inspection reported.
	if e.pushed.Commit != "abcdef1234" {
		t.Errorf("pushed commit = %q, want the inspected one", e.pushed.Commit)
	}
	if len(e.cleaned) != 1 {
		t.Errorf("cleaned = %v, want the workspace deleted", e.cleaned)
	}
}

// The run does its own committing, so it needs the git commands for it —
// acceptEdits does not cover Bash. Publishing stays out of its reach.
func TestImplementFeatureWorkflow_GitPermissions(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "ok", Subtype: "success"}, nil, oneCommit(), nil)
	e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	allowed := strings.Join(e.run.AllowedTools, " ")
	for _, want := range []string{"git add", "git commit"} {
		if !strings.Contains(allowed, want) {
			t.Errorf("AllowedTools %v should cover %q", e.run.AllowedTools, want)
		}
	}
	denied := strings.Join(e.run.DisallowedTools, " ")
	if !strings.Contains(denied, "git push") {
		t.Errorf("DisallowedTools %v should keep the run from pushing", e.run.DisallowedTools)
	}
	if e.run.PermissionMode != "acceptEdits" {
		t.Errorf("PermissionMode = %q", e.run.PermissionMode)
	}
}

// A run that changed nothing is a failure. Silently doing nothing is the most
// expensive outcome to diagnose, and a denied git command produces exactly it.
func TestImplementFeatureWorkflow_NoCommitIsAFailure(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "I edited the file.", Subtype: "success"}, nil,
		activity.InspectWorkspaceOutput{Dirty: true}, nil)

	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	if e.pushed != nil {
		t.Error("nothing should be pushed when there is no commit")
	}
	if !strings.Contains(out.Error, "no commit") {
		t.Errorf("Error = %q", out.Error)
	}
	if !out.Dirty {
		t.Error("the caller should be told the run left uncommitted changes")
	}
	if out.Report != "I edited the file." {
		t.Errorf("Report = %q, want the run's account kept", out.Report)
	}
}

// The branch is checked against the tree, not against the run's report: a run
// that wandered off must not have its commits published from somewhere else.
func TestImplementFeatureWorkflow_WrongBranchIsNotPushed(t *testing.T) {
	inspected := oneCommit()
	inspected.Branch = "main"
	e := newImplementEnv(t, claudeCodeResult{Report: "ok", Subtype: "success"}, nil, inspected, nil)

	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	if e.pushed != nil {
		t.Error("a run that left HEAD elsewhere must not be pushed")
	}
	if !strings.Contains(out.Error, "main") {
		t.Errorf("Error = %q, want it to name where HEAD ended up", out.Error)
	}
}

// A failed run that still committed something is worth publishing: the commits
// are on their own branch, and throwing them away helps nobody.
func TestImplementFeatureWorkflow_FailedRunWithCommitsStillPushes(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{
		Report: "Ran out of turns.", Subtype: "error_max_turns", IsError: true,
	}, nil, oneCommit(), nil)

	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	if !out.Pushed {
		t.Error("commits from a failed run should still reach their branch")
	}
	if !strings.Contains(out.Error, "error_max_turns") {
		t.Errorf("Error = %q, want the failure reported alongside", out.Error)
	}
}

func TestImplementFeatureWorkflow_PushFailureIsReported(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "ok", Subtype: "success"}, nil, oneCommit(),
		errors.New("permission denied (publickey)"))

	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	if out.Pushed {
		t.Error("Pushed should stay false when the push failed")
	}
	if !strings.Contains(out.Error, "publickey") {
		t.Errorf("Error = %q, want the git failure", out.Error)
	}
	if len(e.cleaned) != 1 {
		t.Error("the workspace should be deleted even when the push failed")
	}
	// Not git push's own failure: nothing is kept.
	if e.bundled != nil || out.Bundle != nil || strings.Contains(out.Content, "git fetch") {
		t.Errorf("bundled %+v, output %+v", e.bundled, out)
	}
}

// A push git itself fails keeps the commits: a bundle of the branch,
// published for the call, with the steps the user takes to push it; then the
// clone goes.
func TestImplementFeatureWorkflow_PushFailedKeepsTheCommits(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "ok", Subtype: "success"}, nil, oneCommit(),
		temporal.NewApplicationError("git push of agent/x to r: exit status 1: ! [remote rejected] (pre-receive hook declined)", activity.ErrPushFailed))
	e.bundle = tool.FileRef{ID: "f-9", Name: "agent-do-it.bundle", Size: 900}
	call := tool.CallContext{UserID: "u-1", CallID: "call-1", AgentChain: []string{"jarvis"}, NotifyQueue: "agent",
		Turn: &tool.TurnRef{SessionID: "s-1", TurnKey: "m3.jarvis"}}
	out := e.run_(t, ImplementFeatureInput{Repo: "https://git.example.com/app.git", Task: "do it", CallContext: call})

	if e.bundled == nil || e.bundled.Dir != e.pushed.Dir || e.bundled.Commit != "abcdef1234" || e.bundled.Base != "base0000" ||
		e.bundled.Branch != out.Branch || e.bundled.Call.CallID != "call-1" {
		t.Fatalf("bundled %+v", e.bundled)
	}
	if out.Pushed || out.Bundle == nil || out.Bundle.ID != "f-9" || len(out.Files) != 1 || len(e.cleaned) != 1 ||
		!strings.Contains(out.Error, "pre-receive hook declined") || strings.Contains(out.Error, "activity error") {
		t.Fatalf("output %+v, cleaned %v", out, e.cleaned)
	}
	for _, want := range []string{"agent-do-it.bundle (900 B, id f-9)", "kept in agent-do-it.bundle (id f-9", "pass these steps on to them",
		"in a clone of https://git.example.com/app.git that has base0000 (git clone https://git.example.com/app.git)",
		"git fetch /path/to/agent-do-it.bundle " + out.Branch + ":" + out.Branch, "git push origin " + out.Branch} {
		if !strings.Contains(out.Content, want) {
			t.Errorf("content lacks %q: %s", want, out.Content)
		}
	}

	// The bundle could not be made: said, the work never claimed kept.
	e = newImplementEnv(t, claudeCodeResult{Report: "ok", Subtype: "success"}, nil, oneCommit(),
		temporal.NewApplicationError("git push: refused", activity.ErrPushFailed))
	e.bundleErr = temporal.NewNonRetryableApplicationError("the bundle is 30.0 MB, over the 20.0 MB a published file may be", activity.ErrBundleFailed, nil)
	out = e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it", CallContext: call})
	if out.Bundle != nil || len(out.Files) != 0 || strings.Contains(out.Content, "git fetch") ||
		!strings.Contains(out.Content, "could not be kept either (the bundle is 30.0 MB, over the 20.0 MB") {
		t.Errorf("output %+v", out)
	}
}

// A branch that could not be pushed does not start the run: said as the
// preparation says it, nothing paid.
func TestImplementFeatureWorkflow_PushCheckFailed(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "ok", Subtype: "success"}, nil, oneCommit(), nil)
	e.prepareErr = temporal.NewNonRetryableApplicationError("this worker may not push to r (git push --dry-run, before the run): denied; nothing was run",
		activity.ErrPushCheckFailed, nil)
	out := e.run_(t, ImplementFeatureInput{Repo: "r", Task: "do it"})
	if e.run != nil || e.pushed != nil || out.Pushed || out.Error != "this worker may not push to r (git push --dry-run, before the run): denied; nothing was run" {
		t.Errorf("run %+v, output %+v", e.run, out)
	}
}

func TestImplementFeatureWorkflow_CleansUpWhenTheRunItselfFails(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{}, errors.New("CLI exited without reporting a result"),
		activity.InspectWorkspaceOutput{}, nil)

	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	if len(e.cleaned) != 1 {
		t.Errorf("cleaned = %v, want the workspace deleted", e.cleaned)
	}
	if !strings.Contains(out.Error, "did not complete") {
		t.Errorf("Error = %q", out.Error)
	}
}

func TestBranchName(t *testing.T) {
	cases := []struct {
		name, title, task, want string
	}{
		{"from title", "Add rate limiting", "whatever", "agent/add-rate-limiting-"},
		{"falls back to task", "", "Fix the flaky store test", "agent/fix-the-flaky-store-test-"},
		{"strips punctuation", "Fix: the #1 bug!", "", "agent/fix-the-1-bug-"},
		{"accents and spaces", "  Corriger l'entrée  ", "", "agent/corriger-l-entr-e-"},
		{"nothing usable", "###", "***", "agent/run-"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := branchName(tc.title, tc.task, "0e2f5f4a-1111-4000-8000-000000000000")
			if !strings.HasPrefix(got, tc.want) {
				t.Errorf("branchName = %q, want prefix %q", got, tc.want)
			}
			if strings.ContainsAny(got, " ~^:?*[\\") || strings.Contains(got, "..") {
				t.Errorf("branchName = %q is not a valid git ref", got)
			}
		})
	}

	// Two runs of the same task must not collide on the remote.
	a := branchName("same", "", "aaaaaaaa-0000-4000-8000-000000000000")
	b := branchName("same", "", "bbbbbbbb-0000-4000-8000-000000000000")
	if a == b {
		t.Errorf("two runs produced the same branch %q", a)
	}
}

// A run that changed .git/config may be steering the push: nothing is pushed,
// and the caller is told why.
func TestImplementFeatureWorkflow_ChangedGitConfigIsNotPushed(t *testing.T) {
	inspected := oneCommit()
	inspected.GitConfigChanged = true
	e := newImplementEnv(t, claudeCodeResult{Report: "Done.", Subtype: "success"}, nil, inspected, nil)

	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	if e.pushed != nil || out.Pushed {
		t.Error("pushed a tree whose git configuration the run changed")
	}
	if !strings.Contains(out.Error, ".git/config") {
		t.Errorf("Error = %q", out.Error)
	}
	if len(e.cleaned) != 1 {
		t.Errorf("cleaned = %v, want the workspace deleted", e.cleaned)
	}
}

// With several commits, the push publishes the newest the inspection listed
// (git log lists newest first): the branch as the report describes it.
func TestImplementFeatureWorkflow_PushesTheNewestInspectedCommit(t *testing.T) {
	inspected := oneCommit()
	inspected.Commits = []activity.CommitInfo{{SHA: "newest", Subject: "b"}, {SHA: "oldest", Subject: "a"}}
	e := newImplementEnv(t, claudeCodeResult{Report: "ok", Subtype: "success"}, nil, inspected, nil)

	out := e.run_(t, ImplementFeatureInput{Repo: "git@host:org/repo.git", Task: "x"})

	if !out.Pushed || e.pushed == nil || e.pushed.Commit != "newest" {
		t.Errorf("Pushed = %v, push = %+v, want the newest commit", out.Pushed, e.pushed)
	}
}

// An implementation that ends without the CLI's result still has its commits
// checked and pushed, once its CLI is gone from its worker, and tells the
// agent what it did and that its cost is unknown. A CLI still there past the
// wait (activity.ErrRunStillActive) gets nothing pushed.
func TestImplementFeatureWorkflow_InterruptedRunSaysWhatItDid(t *testing.T) {
	progress := runProgress{Events: 80, ToolCalls: 21, LastTool: "Edit"}
	for _, c := range []struct {
		name       string
		inspectErr error
		pushed     bool
		why        string
	}{
		{"run gone", nil, true, ""},
		{"run still active", temporal.NewNonRetryableApplicationError("a command of the run still runs",
			activity.ErrRunStillActive, nil), false, stillActive},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newImplementEnv(t, claudeCodeResult{}, nil, oneCommit(), nil)
			e.inspectErr = c.inspectErr
			e.env.OnActivity("RunClaudeCode", mock.Anything, mock.Anything).After(7*time.Minute).
				Return(claudeCodeResult{}, temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_HEARTBEAT, nil, progress))

			out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

			if want := "the run did not complete: its worker stopped answering (no heartbeat for 2m0s)"; !strings.HasPrefix(out.Error, want) {
				t.Errorf("Error = %q, want %q", out.Error, want)
			}
			if !strings.Contains(out.Error, c.why) {
				t.Errorf("Error = %q, want %q", out.Error, c.why)
			}
			if out.Pushed != c.pushed || (e.pushed != nil) != c.pushed {
				t.Errorf("Pushed = %v, push %+v; want pushed %v", out.Pushed, e.pushed, c.pushed)
			}
			for _, want := range []string{
				"run: interrupted after 7m0s; 21 tool calls (last: Edit), 80 events; cost unknown (run interrupted)",
				"running it again starts over from scratch, and is paid again",
			} {
				if !strings.Contains(out.Content, want) {
					t.Errorf("content lacks %q:\n%s", want, out.Content)
				}
			}
			if strings.Contains(out.Content, "0 turns") || strings.Contains(out.Content, "activity error") {
				t.Errorf("content:\n%s", out.Content)
			}
		})
	}
}

// What an implementation left in its outputs is published on its worker,
// for the call's turn, and listed for the agent; what was not, said.
func TestImplementFeatureWorkflow_PublishesItsOutputs(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "Done.", Subtype: "success"}, nil, oneCommit(), nil)
	e.outputs = activity.PublishOutputsOutput{Files: []tool.FileRef{{ID: "f-1", Name: "diagram.svg", Size: 2048}},
		Unpublished: []string{"key: a link, not published"}}
	call := tool.CallContext{UserID: "u-1", CallID: "call-1", AgentChain: []string{"jarvis"}, NotifyQueue: "agent",
		Turn: &tool.TurnRef{SessionID: "s-1", TurnKey: "m3.jarvis"}}
	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it", CallContext: call})
	if !e.run.Outputs || e.published == nil || e.published.Call.CallID != "call-1" || e.published.Call.Turn.TurnKey != "m3.jarvis" ||
		e.published.Dir != e.run.Dir {
		t.Fatalf("run %+v, published %+v", e.run, e.published)
	}
	for _, want := range []string{"files published", "diagram.svg (2.0 KB, id f-1)", "files not published", "key: a link"} {
		if !strings.Contains(out.Content, want) {
			t.Errorf("content lacks %q: %s", want, out.Content)
		}
	}
}
