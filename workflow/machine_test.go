package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/machine"
)

func machineEchoEnv(t *testing.T, pick activity.PickMachineOutput, run func(activity.RunOnMachineInput) (machine.Result, error)) (*testsuite.TestWorkflowEnvironment, *[]activity.PickMachineInput) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	picks := &[]activity.PickMachineInput{}
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.PickMachineInput) (activity.PickMachineOutput, error) {
		*picks = append(*picks, in)
		return pick, nil
	}, sdkactivity.RegisterOptions{Name: "PickMachine"})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.RunOnMachineInput) (machine.Result, error) {
		return run(in)
	}, sdkactivity.RegisterOptions{Name: "RunOnMachine"})
	return env, picks
}

func TestMachineEchoWorkflow(t *testing.T) {
	env, picks := machineEchoEnv(t, activity.PickMachineOutput{DirectiveID: "d-1", MachineName: "maison"},
		func(in activity.RunOnMachineInput) (machine.Result, error) {
			if in.DirectiveID != "d-1" {
				t.Errorf("directive %q", in.DirectiveID)
			}
			out, _ := json.Marshal(machine.EchoOutput{Text: "bonjour", Progresses: 3})
			return machine.Result{Output: out, Progress: "echo: 3s of 3s"}, nil
		})
	env.ExecuteWorkflow(MachineEchoWorkflow, MachineEchoInput{UserID: "u-1", Text: "bonjour", Duration: 3 * time.Second, ProgressEvery: time.Second})
	var out MachineEchoOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatal(err)
	}
	if out.Text != "bonjour" || out.Machine != "maison" || out.Progresses != 3 {
		t.Errorf("output %+v", out)
	}
	p := (*picks)[0]
	var in machine.EchoInput
	if json.Unmarshal(p.Input, &in) != nil || in.DurationMS != 3000 || p.UserID != "u-1" || p.Capability != machine.KindEcho || p.Timeout != 3*time.Second+machineEchoMargin {
		t.Errorf("pick %+v", p)
	}
}

func TestMachineEchoWorkflow_NoMachine(t *testing.T) {
	env, _ := machineEchoEnv(t, activity.PickMachineOutput{NoMachine: "none connected"}, func(activity.RunOnMachineInput) (machine.Result, error) {
		t.Error("run without a machine")
		return machine.Result{}, nil
	})
	env.ExecuteWorkflow(MachineEchoWorkflow, MachineEchoInput{UserID: "u-1", Text: "x"})
	var out MachineEchoOutput
	if err := env.GetWorkflowResult(&out); err != nil || out.NoMachine != "none connected" {
		t.Errorf("%+v %v", out, err)
	}
}

func TestMachineEchoWorkflow_Failure(t *testing.T) {
	env, _ := machineEchoEnv(t, activity.PickMachineOutput{DirectiveID: "d-1", MachineName: "maison"}, func(activity.RunOnMachineInput) (machine.Result, error) {
		return machine.Result{}, temporal.NewNonRetryableApplicationError("machine revoked", machine.ErrTypeRevoked, nil)
	})
	env.ExecuteWorkflow(MachineEchoWorkflow, MachineEchoInput{UserID: "u-1", Text: "x", Duration: time.Second})
	err := env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), `machine "maison" was revoked`) || !strings.Contains(err.Error(), "not run again") {
		t.Errorf("error %v", err)
	}
}

// A directive that ended without its result is said in words, never as the
// SDK's chain, and never as something run again.
func TestMachineError(t *testing.T) {
	for _, c := range []struct {
		err  error
		want string
	}{
		{temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_HEARTBEAT, nil), `machine "maison" stopped answering during the directive (nothing heard for 5m0s)`},
		{temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_START_TO_CLOSE, nil), "reached its time limit (1m0s)"},
		{temporal.NewApplicationError("x", machine.ErrTypeStopping), "agent connect ended"},
		{temporal.NewApplicationError("x", machine.ErrTypeLost), "lost the directive"},
		{temporal.NewApplicationError("disk full", machine.ErrTypeFailed), `on machine "maison": disk full`},
	} {
		err := machineError(c.err, "maison", time.Minute, 0)
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) || appErr.Type() != ErrTypeMachineDirective || !strings.Contains(appErr.Message(), c.want) ||
			!strings.Contains(appErr.Message(), "not run again") {
			t.Errorf("%v: %v", c.err, err)
		}
	}
	canceled := temporal.NewCanceledError()
	if err := machineError(canceled, "maison", time.Minute, 0); !temporal.IsCanceledError(err) {
		t.Errorf("a cancellation became %v", err)
	}
}

func TestRunOnMachineOptions(t *testing.T) {
	o := runOnMachineOptions(time.Hour, 0)
	if o.RetryPolicy.MaximumAttempts != 1 || !o.WaitForCancellation || o.HeartbeatTimeout != DefaultMachineHeartbeatTimeout || o.StartToCloseTimeout != time.Hour {
		t.Errorf("options %+v", o)
	}
}
