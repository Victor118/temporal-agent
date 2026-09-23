package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
)

type implementEnv struct {
	env       *testsuite.TestWorkflowEnvironment
	run       *activity.RunClaudeCodeInput
	pushed    *activity.PushBranchInput
	cleaned   []string
	inspected activity.InspectWorkspaceOutput
}

func newImplementEnv(t *testing.T, result claudeCodeResult, runErr error, inspected activity.InspectWorkspaceOutput, pushErr error) *implementEnv {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	e := &implementEnv{env: suite.NewTestWorkflowEnvironment(), inspected: inspected}

	e.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PrepareWorkspaceInput) (activity.PrepareWorkspaceOutput, error) {
		return activity.PrepareWorkspaceOutput{Dir: "/work/" + in.Name, Commit: "base0000", Branch: in.Branch}, nil
	}, sdkactivity.RegisterOptions{Name: "PrepareWorkspace"})

	e.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.RunClaudeCodeInput) (claudeCodeResult, error) {
		e.run = &in
		return result, runErr
	}, sdkactivity.RegisterOptions{Name: "RunClaudeCode"})

	e.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.InspectWorkspaceInput) (activity.InspectWorkspaceOutput, error) {
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
