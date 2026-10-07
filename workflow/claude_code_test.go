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
)

// analyzeEnv registers stand-ins for the three activities and reports what the
// workflow asked them to do.
type analyzeEnv struct {
	env      *testsuite.TestWorkflowEnvironment
	queues   *activityQueues
	prepared *activity.PrepareWorkspaceInput
	run      *activity.RunClaudeCodeInput
	cleaned  []string
	// duringRun, when set, is what the run does instead of returning at once.
	duringRun func(ctx context.Context) error
}

func newAnalyzeEnv(t *testing.T, prepareErr error, result claudeCodeResult, runErr error) *analyzeEnv {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	a := &analyzeEnv{env: suite.NewTestWorkflowEnvironment()}
	a.queues = asRunWorker(a.env)

	a.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.PrepareWorkspaceInput) (activity.PrepareWorkspaceOutput, error) {
		a.prepared = &in
		if prepareErr != nil {
			return activity.PrepareWorkspaceOutput{}, prepareErr
		}
		return activity.PrepareWorkspaceOutput{Dir: "/work/" + in.Name, Commit: "1234567890abcdef"}, nil
	}, sdkactivity.RegisterOptions{Name: "PrepareWorkspace"})

	a.env.RegisterActivityWithOptions(func(ctx context.Context, in activity.RunClaudeCodeInput) (claudeCodeResult, error) {
		a.run = &in
		if a.duringRun != nil {
			return result, a.duringRun(ctx)
		}
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
		ToolUses: map[string]int{"Bash": 1}, PaidBy: "subscription",
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
	if out.PaidBy != "subscription" || !strings.Contains(out.Content, "paid by a Claude subscription") {
		t.Errorf("PaidBy = %q, content:\n%s", out.PaidBy, out.Content)
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
	// And the run is told: otherwise its report ends offering to fix things.
	if !strings.Contains(a.run.AppendSystemPrompt, "read-only") {
		t.Errorf("AppendSystemPrompt = %q, want the run told it is read-only", a.run.AppendSystemPrompt)
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

	// What paid the run, as the activity settled it: an agent must not take
	// a subscription's estimate for a bill, nor the other way round.
	for paidBy, want := range map[string]string{
		"subscription": "paid by a Claude subscription, not billed",
		"api":          "billed to the Anthropic API",
	} {
		o := out
		o.PaidBy = paidBy
		if !strings.Contains(o.Summary(), want) {
			t.Errorf("paid by %s: summary lacks %q", paidBy, want)
		}
	}
	if strings.Contains(s, "billed") {
		t.Error("an unknown payer should not be named")
	}

	failed := ClaudeCodeOutput{Error: "the run reported a failure (error_max_turns)"}
	if !strings.Contains(failed.Summary(), "error_max_turns") {
		t.Error("a failed run's summary should lead with the failure")
	}
}

// interruptedAfter makes the run end with err after it has run for d.
func (a *analyzeEnv) interruptedAfter(d time.Duration, err error) {
	a.env.OnActivity("RunClaudeCode", mock.Anything, mock.Anything).After(d).Return(claudeCodeResult{}, err)
}

// A run that ends without the CLI's result tells the agent what it did and
// that its cost is unknown, never "0 turns, $0": it ran, and was paid for.
// And that a new run starts over, paid again: nothing resumes it.
func TestAnalyzeRepoWorkflow_InterruptedRunSaysWhatItDid(t *testing.T) {
	progress := runProgress{Events: 134, ToolCalls: 34, LastTool: "Bash"}
	for _, c := range []struct {
		name, why string
		err       error
	}{
		{"heartbeat timeout", "its worker stopped answering (no heartbeat for 2m0s)",
			temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_HEARTBEAT, nil, progress)},
		{"stalled", "the CLI wrote nothing for 12m0s (stuck?), so the run was ended",
			// As the activity has it: the runner's error is the cause.
			temporal.NewNonRetryableApplicationError("claude code: the run was ended as stuck", activity.ErrRunStalled,
				errors.New("claudecode: the CLI wrote nothing for 12m0s (stuck?), so the run was ended"), progress)},
		{"no result", "CLI exited with exit status 137 and reported nothing: out of memory",
			temporal.NewNonRetryableApplicationError("claude code: the run ended without the CLI's result", activity.ErrRunFailed,
				errors.New("claudecode: CLI exited with exit status 137 and reported nothing: out of memory"), progress)},
	} {
		t.Run(c.name, func(t *testing.T) {
			a := newAnalyzeEnv(t, nil, claudeCodeResult{}, nil)
			a.interruptedAfter(5*time.Minute, c.err)

			out := a.run_(t, AnalyzeRepoInput{Repo: "/src/repo", Task: "look"})

			if want := "the analysis did not complete: " + c.why; !strings.HasPrefix(out.Error, want) {
				t.Errorf("Error = %q, want %q", out.Error, want)
			}
			if !out.Interrupted || out.Progress == nil || *out.Progress != progress {
				t.Errorf("Interrupted = %v, Progress = %+v, want %+v", out.Interrupted, out.Progress, progress)
			}
			for _, want := range []string{
				"run: interrupted after 5m0s; 34 tool calls (last: Bash), 134 events; cost unknown (run interrupted)",
				"running it again starts over from scratch, and is paid again",
			} {
				if !strings.Contains(out.Content, want) {
					t.Errorf("content lacks %q:\n%s", want, out.Content)
				}
			}
			for _, wrong := range []string{"0 turns", "$0.0000", "activity error"} {
				if strings.Contains(out.Content, wrong) {
					t.Errorf("content says %q:\n%s", wrong, out.Content)
				}
			}
			if len(a.cleaned) != 1 {
				t.Errorf("cleaned %v, want the workspace deleted", a.cleaned)
			}
		})
	}
}

// A run that reached its own bound says so, in words.
func TestAnalyzeRepoWorkflow_RunOutOfTime(t *testing.T) {
	a := newAnalyzeEnv(t, nil, claudeCodeResult{}, nil)
	a.interruptedAfter(time.Minute, temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil))

	out := a.run_(t, AnalyzeRepoInput{Repo: "/src/repo", Task: "look"})

	if want := "the analysis did not complete: it reached its time limit (45m0s)"; out.Error != want {
		t.Errorf("Error = %q, want %q", out.Error, want)
	}
	if !strings.Contains(out.Content, "progress unknown; cost unknown (run interrupted)") {
		t.Errorf("content:\n%s", out.Content)
	}
}
