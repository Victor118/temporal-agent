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

// analyzeEnv registers stand-ins for the three activities and reports what the
// workflow asked them to do.
type analyzeEnv struct {
	env      *testsuite.TestWorkflowEnvironment
	prepared *activity.PrepareWorkspaceInput
	run      *activity.RunClaudeCodeInput
	cleaned  []string
}

func newAnalyzeEnv(t *testing.T, prepareErr error, result claudeCodeResult, runErr error) *analyzeEnv {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	a := &analyzeEnv{env: suite.NewTestWorkflowEnvironment()}

	a.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PrepareWorkspaceInput) (activity.PrepareWorkspaceOutput, error) {
		a.prepared = &in
		if prepareErr != nil {
			return activity.PrepareWorkspaceOutput{}, prepareErr
		}
		return activity.PrepareWorkspaceOutput{Dir: "/work/" + in.Name, Commit: "1234567890abcdef"}, nil
	}, sdkactivity.RegisterOptions{Name: "PrepareWorkspace"})

	a.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.RunClaudeCodeInput) (claudeCodeResult, error) {
		a.run = &in
		return result, runErr
	}, sdkactivity.RegisterOptions{Name: "RunClaudeCode"})

	a.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.CleanupWorkspaceInput) error {
		a.cleaned = append(a.cleaned, in.Dir)
		return nil
	}, sdkactivity.RegisterOptions{Name: "CleanupWorkspace"})

	return a
}

func (a *analyzeEnv) run_(t *testing.T, input AnalyzeRepoInput) ClaudeCodeOutput {
	t.Helper()
	raw, _ := json.Marshal(input)
	a.env.ExecuteWorkflow(AnalyzeRepoWorkflow, json.RawMessage(raw))
	if !a.env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := a.env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var out ClaudeCodeOutput
	if err := a.env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	return out
}

func TestAnalyzeRepoWorkflow_HappyPath(t *testing.T) {
	a := newAnalyzeEnv(t, nil, claudeCodeResult{
		Report: "The entrypoint is cmd/agent.", Subtype: "success",
		NumTurns: 2, DurationMS: 7498, CostUSD: 0.0561,
		ToolUses: map[string]int{"Bash": 1},
	}, nil)

	out := a.run_(t, AnalyzeRepoInput{Repo: "/src/repo", Task: "where is the entrypoint?", Ref: "main"})

	if out.Error != "" {
		t.Errorf("Error = %q, want none", out.Error)
	}
	if out.Report != "The entrypoint is cmd/agent." {
		t.Errorf("Report = %q", out.Report)
	}
	if out.Commit != "1234567890abcdef" || out.CostUSD != 0.0561 || out.NumTurns != 2 {
		t.Errorf("commit/cost/turns = %q/%v/%d", out.Commit, out.CostUSD, out.NumTurns)
	}
	if a.prepared.Repo != "/src/repo" || a.prepared.Ref != "main" {
		t.Errorf("prepared = %+v", a.prepared)
	}
	if a.run.Dir != "/work/"+a.prepared.Name || a.run.Task != "where is the entrypoint?" {
		t.Errorf("run = %+v", a.run)
	}
}

// The permission mode is the whole point of this workflow: it is set by the
// code, so an agent asking for an analysis cannot end up with write access.
func TestAnalyzeRepoWorkflow_IsReadOnly(t *testing.T) {
	a := newAnalyzeEnv(t, nil, claudeCodeResult{Report: "ok", Subtype: "success"}, nil)
	a.run_(t, AnalyzeRepoInput{Repo: "/src/repo", Task: "look around"})

	if a.run.PermissionMode != "plan" {
		t.Errorf("PermissionMode = %q, want plan", a.run.PermissionMode)
	}
	if len(a.run.AllowedTools) != 0 {
		t.Errorf("AllowedTools = %v, want none: an analysis pre-authorizes nothing", a.run.AllowedTools)
	}
}

// Whatever happens to the run, the clone must not be left behind: the
// workspace volume would fill up one abandoned checkout at a time.
func TestAnalyzeRepoWorkflow_AlwaysCleansUp(t *testing.T) {
	cases := []struct {
		name   string
		result claudeCodeResult
		runErr error
	}{
		{"success", claudeCodeResult{Report: "ok", Subtype: "success"}, nil},
		{"reported failure", claudeCodeResult{Report: "ran out", Subtype: "error_max_turns", IsError: true}, nil},
		{"activity failure", claudeCodeResult{}, errors.New("CLI exited without reporting a result")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newAnalyzeEnv(t, nil, tc.result, tc.runErr)
			a.run_(t, AnalyzeRepoInput{Repo: "/src/repo", Task: "look"})
			if len(a.cleaned) != 1 {
				t.Fatalf("cleaned %v, want exactly one workspace", a.cleaned)
			}
			if a.cleaned[0] != "/work/"+a.prepared.Name {
				t.Errorf("cleaned %q, want the prepared workspace", a.cleaned[0])
			}
		})
	}
}

// A failed run comes back as a result the agent can read, not as a workflow
// failure — the partial report is usually the most useful thing it produced.
func TestAnalyzeRepoWorkflow_FailureKeepsTheReport(t *testing.T) {
	a := newAnalyzeEnv(t, nil, claudeCodeResult{
		Report: "Got as far as the store layer.", Subtype: "error_max_turns", IsError: true,
	}, nil)

	out := a.run_(t, AnalyzeRepoInput{Repo: "/src/repo", Task: "look"})

	if out.Report != "Got as far as the store layer." {
		t.Errorf("Report = %q, want the partial report kept", out.Report)
	}
	if !strings.Contains(out.Error, "error_max_turns") {
		t.Errorf("Error = %q, want it to name the failure", out.Error)
	}
}

// A clone that fails must not leave a cleanup for a directory that was never
// created, and must still tell the agent what went wrong.
func TestAnalyzeRepoWorkflow_PrepareFailure(t *testing.T) {
	a := newAnalyzeEnv(t, errors.New("repository not found"), claudeCodeResult{}, nil)
	out := a.run_(t, AnalyzeRepoInput{Repo: "/nope", Task: "look"})

	if a.run != nil {
		t.Error("the run should not start when the workspace could not be prepared")
	}
	if len(a.cleaned) != 0 {
		t.Errorf("cleaned %v, want nothing", a.cleaned)
	}
	if !strings.Contains(out.Error, "repository not found") {
		t.Errorf("Error = %q", out.Error)
	}
}

func TestAnalyzeRepoWorkflow_RejectsIncompleteInput(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
	}{
		{"no repo", `{"task":"look"}`},
		{"no task", `{"repo":"/src/repo"}`},
		{"blank task", `{"repo":"/src/repo","task":"  "}`},
		{"not an object", `"nope"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.ExecuteWorkflow(AnalyzeRepoWorkflow, json.RawMessage(tc.input))
			if err := env.GetWorkflowError(); err == nil {
				t.Error("expected the workflow to refuse the input")
			}
		})
	}
}

// The agent reads the summary, not the JSON of the output.
func TestClaudeCodeOutputSummary(t *testing.T) {
	out := ClaudeCodeOutput{
		Report: "The entrypoint is cmd/agent.",
		Repo:   "/src/repo", Ref: "main", Commit: "1234567890abcdef",
		NumTurns: 2, DurationMS: 7498, CostUSD: 0.0561,
	}
	s := out.Summary()
	for _, want := range []string{"The entrypoint is cmd/agent.", "/src/repo", "main", "12345678", "2 turns", "$0.0561"} {
		if !strings.Contains(s, want) {
			t.Errorf("summary missing %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "1234567890abcdef") {
		t.Error("summary should shorten the commit")
	}

	failed := ClaudeCodeOutput{Error: "the run reported a failure (error_max_turns)"}
	if !strings.Contains(failed.Summary(), "error_max_turns") {
		t.Error("a failed run's summary should lead with the failure")
	}
}
