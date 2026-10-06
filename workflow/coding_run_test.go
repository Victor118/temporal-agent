package workflow

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/tool"
)

// codingRunCase is what the stubs of a CodingRunWorkflow run do.
type codingRunCase struct {
	route      activity.CodingRouting
	pick       activity.PickMachineOutput
	run        func(activity.RunOnMachineInput) (machine.Result, error)
	probe      error
	probeOut   activity.ProbeRunWorkerOutput
	picks      []activity.PickMachineInput
	child      []json.RawMessage
	childProbe activity.ProbeRunWorkerOutput
	childOn    string
}

func runCodingRun(t *testing.T, c *codingRunCase, in AnalyzeRepoInput) (ClaudeCodeOutput, error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(CodingRunWorkflow)
	env.RegisterActivityWithOptions(func(context.Context) (activity.CodingRouting, error) { return c.route, nil },
		sdkactivity.RegisterOptions{Name: "CodingRoute"})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.PickMachineInput) (activity.PickMachineOutput, error) {
		c.picks = append(c.picks, in)
		return c.pick, nil
	}, sdkactivity.RegisterOptions{Name: "PickMachine"})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.RunOnMachineInput) (machine.Result, error) {
		if c.run == nil {
			t.Error("ran on a machine")
			return machine.Result{}, nil
		}
		return c.run(in)
	}, sdkactivity.RegisterOptions{Name: "RunOnMachine"})
	env.RegisterActivityWithOptions(func(context.Context) (activity.ProbeRunWorkerOutput, error) {
		return c.probeOut, c.probe
	}, sdkactivity.RegisterOptions{Name: "ProbeRunWorker"})
	env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in AnalyzeFallbackInput) (ClaudeCodeOutput, error) {
		c.child = append(c.child, in.Input)
		c.childProbe = in.Probe
		c.childOn = sdkworkflow.GetInfo(ctx).TaskQueueName
		return ClaudeCodeOutput{Report: "from the fallback", Repo: "r"}, nil
	}, sdkworkflow.RegisterOptions{Name: "AnalyzeFallbackWorkflow"})
	raw, _ := tool.WithCallContext(mustJSON(map[string]any{"repo": in.Repo, "task": in.Task, "ref": in.Ref}), in.CallContext)
	env.ExecuteWorkflow(CodingRunWorkflow, raw)
	var out ClaudeCodeOutput
	err := env.GetWorkflowError()
	if err == nil {
		env.GetWorkflowResult(&out)
	}
	return out, err
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

var analyzeCall = AnalyzeRepoInput{Repo: "git@github.com:me/app", Task: "Where is the handler?", Ref: "main",
	CallContext: tool.CallContext{UserID: "u-alice", Agent: "Jarvis"}}

func TestCodingRun_OnTheMachine(t *testing.T) {
	report, _ := json.Marshal(machine.CodingOutput{Report: "In main.go.", Commit: "abc123", NumTurns: 3, CostUSD: 0.2, PaidBy: "subscription"})
	c := &codingRunCase{route: activity.CodingRouting{Machines: true, AnalyzeQueue: "fallback"},
		pick: activity.PickMachineOutput{DirectiveID: "d-1", MachineName: "maison"},
		run:  func(activity.RunOnMachineInput) (machine.Result, error) { return machine.Result{Output: report}, nil }}
	out, err := runCodingRun(t, c, analyzeCall)
	if err != nil || out.Report != "In main.go." || out.Machine != "maison" || out.Error != "" || len(c.child) != 0 {
		t.Fatalf("output %+v %v", out, err)
	}
	if !strings.Contains(out.Content, `machine: "maison"`) || !strings.Contains(out.Content, "paid by a Claude subscription") {
		t.Errorf("content %q", out.Content)
	}
	p := c.picks[0]
	var in machine.AnalyzeInput
	json.Unmarshal(p.Input, &in)
	if p.UserID != "u-alice" || p.Capability != machine.CapClaudeCode || p.Kind != machine.KindAnalyzeRepo || in.Repo != analyzeCall.Repo ||
		in.Ref != "main" || p.Agent != "Jarvis" || p.Timeout != analyzeTimeout {
		t.Errorf("pick %+v %+v", p, in)
	}
}

func TestCodingRun_MachineFailureKeepsWhatItSaid(t *testing.T) {
	partial, _ := json.Marshal(machine.CodingOutput{Report: "Halfway.", Interrupted: true, ToolCalls: 7, LastTool: "Grep", Error: "stuck"})
	c := &codingRunCase{route: activity.CodingRouting{Machines: true},
		pick: activity.PickMachineOutput{DirectiveID: "d-1", MachineName: "maison"},
		run: func(activity.RunOnMachineInput) (machine.Result, error) {
			return machine.Result{}, temporal.NewNonRetryableApplicationError("the CLI wrote nothing", machine.ErrTypeFailed, nil,
				machine.Result{Output: partial})
		}}
	out, err := runCodingRun(t, c, analyzeCall)
	if err != nil || out.Report != "Halfway." || !out.Interrupted || out.Progress == nil || out.Progress.ToolCalls != 7 ||
		!strings.Contains(out.Error, `on machine "maison"`) || !strings.Contains(out.Error, "not run again") {
		t.Fatalf("output %+v %v", out, err)
	}
}

func TestCodingRun_Fallback(t *testing.T) {
	c := &codingRunCase{route: activity.CodingRouting{Machines: true, AnalyzeQueue: "tools-claude-code-ro"},
		pick: activity.PickMachineOutput{NoMachine: "none"}}
	out, err := runCodingRun(t, c, analyzeCall)
	if err != nil || out.Report != "from the fallback" || len(c.child) != 1 || c.childOn != "tools-claude-code-ro" {
		t.Fatalf("output %+v %v, child on %q", out, err, c.childOn)
	}
	var in AnalyzeRepoInput
	if json.Unmarshal(c.child[0], &in) != nil || in.Repo != analyzeCall.Repo || in.UserID != "u-alice" || in.Agent != "Jarvis" {
		t.Errorf("the child's input: %s", c.child[0])
	}

	// No worker on the fallback queue: said, nothing run.
	c = &codingRunCase{route: activity.CodingRouting{Machines: true, AnalyzeQueue: "tools-claude-code-ro"},
		pick:  activity.PickMachineOutput{NoMachine: "none"},
		probe: temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, nil)}
	out, err = runCodingRun(t, c, analyzeCall)
	if err != nil || len(c.child) != 0 || !strings.Contains(out.Error, "no machine of yours") || !strings.Contains(out.Error, "fallback") {
		t.Errorf("no worker: %+v %v", out, err)
	}

	// Machines off: the fallback alone, no pick.
	c = &codingRunCase{route: activity.CodingRouting{AnalyzeQueue: "q"}}
	if out, err := runCodingRun(t, c, analyzeCall); err != nil || len(c.picks) != 0 || out.Report != "from the fallback" {
		t.Errorf("machines off: %+v %v", out, err)
	}
}

func TestCodingRun_Nowhere(t *testing.T) {
	c := &codingRunCase{route: activity.CodingRouting{Machines: true}, pick: activity.PickMachineOutput{NoMachine: "none"}}
	out, err := runCodingRun(t, c, analyzeCall)
	if err != nil || !strings.Contains(out.Error, "no machine of yours is connected") || !strings.Contains(out.Error, "agent connect") {
		t.Errorf("no machine, no fallback: %+v %v", out, err)
	}
	noUser := analyzeCall
	noUser.UserID = ""
	c = &codingRunCase{route: activity.CodingRouting{Machines: true}}
	if out, _ := runCodingRun(t, c, noUser); len(c.picks) != 0 || !strings.Contains(out.Error, "no user") {
		t.Errorf("no user: %+v", out)
	}
}

// A machine that turns the run down before anything ran: the same run goes
// to the fallback, saying why; the probe's answer goes to the child.
func TestCodingRun_RefusedThenFallback(t *testing.T) {
	c := &codingRunCase{route: activity.CodingRouting{Machines: true, AnalyzeQueue: "fallback"},
		pick: activity.PickMachineOutput{DirectiveID: "d-1", MachineName: "maison"},
		run: func(activity.RunOnMachineInput) (machine.Result, error) {
			return machine.Result{}, temporal.NewNonRetryableApplicationError(`repository "x" is not one this machine may use`, machine.ErrTypeRefused, nil)
		}}
	c.probeOut = activity.ProbeRunWorkerOutput{QueueWaitSeconds: 42}
	out, err := runCodingRun(t, c, analyzeCall)
	if err != nil || out.Report != "from the fallback" || out.Machine != "" || out.Interrupted ||
		!strings.Contains(out.Note, `your machine "maison" turned it down`) || !strings.Contains(out.Content, "note: ") {
		t.Fatalf("output %+v %v", out, err)
	}
	if c.childProbe.QueueWaitSeconds != 42 {
		t.Errorf("the child's probe: %+v", c.childProbe)
	}
}

// A machine lost before its CLI started: no "interrupted run" (no cost to
// tell, nothing that a retry would pay again).
func TestCodingRun_LostBeforeTheCLI(t *testing.T) {
	c := &codingRunCase{route: activity.CodingRouting{Machines: true},
		pick: activity.PickMachineOutput{DirectiveID: "d-1", MachineName: "maison"},
		run: func(activity.RunOnMachineInput) (machine.Result, error) {
			return machine.Result{}, temporal.NewNonRetryableApplicationError("clone failed", machine.ErrTypeFailed, nil,
				machine.Result{Progress: machine.CloneProgress})
		}}
	out, err := runCodingRun(t, c, analyzeCall)
	if err != nil || out.Interrupted || strings.Contains(out.Content, "cost unknown") || !strings.Contains(out.Error, "clone failed") {
		t.Errorf("output %+v %v", out, err)
	}
}

// Given the probe's answer (AnalyzeFallbackWorkflow), the analysis does not
// ask the queue again; AnalyzeRepoWorkflow, the tool's, always does.
func TestAnalyzeFallbackWorkflow_ProbedOnce(t *testing.T) {
	for _, probed := range []bool{false, true} {
		a := newAnalyzeEnv(t, nil, claudeCodeResult{Report: "r", Subtype: "success"}, nil)
		probes := 0
		a.env.OnActivity(probeActivity, mock.Anything).Run(func(mock.Arguments) { probes++ }).
			Return(activity.ProbeRunWorkerOutput{QueueWaitSeconds: 60}, nil)
		raw := mustJSON(AnalyzeRepoInput{Repo: "/src/repo", Task: "why"})
		if probed {
			a.env.ExecuteWorkflow(AnalyzeFallbackWorkflow, AnalyzeFallbackInput{Input: raw, Probe: activity.ProbeRunWorkerOutput{QueueWaitSeconds: 60}})
		} else {
			a.env.ExecuteWorkflow(AnalyzeRepoWorkflow, raw)
		}
		var out ClaudeCodeOutput
		if err := a.env.GetWorkflowResult(&out); err != nil || out.Report != "r" || (probes == 1) == probed {
			t.Errorf("probed %v: %d probes, %+v %v", probed, probes, out, err)
		}
	}
}
