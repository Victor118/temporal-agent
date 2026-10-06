package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/machine"
)

// DefaultMachineHeartbeatTimeout is how long a directive's activity lives
// without a heartbeat from the gateway: five minutes, past a restart of the
// server. A laptop closed for longer loses its run (design §6, "un choix
// assumé").
const DefaultMachineHeartbeatTimeout = 5 * time.Minute

// ErrTypeMachineDirective is the error of a directive that ended without
// its result, in words for the caller (machineError).
const ErrTypeMachineDirective = "MachineDirective"

// pickMachineOptions: PickMachine is a transaction, made again safely (its
// directive is keyed by the run and the call).
var pickMachineOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 30 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
}

// runOnMachineOptions are a directive's (design §6): one attempt, never
// replayed (a run is paid for, and done twice would not do the same);
// timeout is how long it may run; the heartbeats are the gateway's, while
// the machine answers; a cancellation waits for the machine's last word.
func runOnMachineOptions(timeout, heartbeat time.Duration) workflow.ActivityOptions {
	if heartbeat <= 0 {
		heartbeat = DefaultMachineHeartbeatTimeout
	}
	return workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		HeartbeatTimeout:    heartbeat,
		WaitForCancellation: true,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}
}

// machineError says why a directive ended without its result, in words for
// the caller, and that nothing runs it again. A cancellation stays one.
func machineError(err error, machineName string, timeout, heartbeat time.Duration) error {
	if heartbeat <= 0 {
		heartbeat = DefaultMachineHeartbeatTimeout
	}
	var timeoutErr *temporal.TimeoutError
	var appErr *temporal.ApplicationError
	var why string
	switch {
	case temporal.IsCanceledError(err):
		return err
	case errors.As(err, &timeoutErr) && timeoutErr.TimeoutType() == enumspb.TIMEOUT_TYPE_HEARTBEAT:
		why = fmt.Sprintf("machine %q stopped answering during the directive (nothing heard for %s)", machineName, heartbeat)
	case errors.As(err, &timeoutErr):
		why = fmt.Sprintf("the directive reached its time limit (%s) on machine %q", timeout, machineName)
	case errors.As(err, &appErr):
		switch appErr.Type() {
		case machine.ErrTypeRevoked:
			why = fmt.Sprintf("machine %q was revoked during the directive", machineName)
		case machine.ErrTypeStopping:
			why = fmt.Sprintf("machine %q stopped (agent connect ended) during the directive", machineName)
		case machine.ErrTypeLost:
			why = fmt.Sprintf("machine %q lost the directive (it restarted during it)", machineName)
		default:
			why = fmt.Sprintf("on machine %q: %s", machineName, appErr.Message())
		}
	default:
		why = fmt.Sprintf("on machine %q: %v", machineName, err)
	}
	return temporal.NewNonRetryableApplicationError(why+"; it was not run again, and running it again starts over",
		ErrTypeMachineDirective, err)
}

// MachineEchoInput is an echo directive for one of UserID's machines.
type MachineEchoInput struct {
	UserID        string        `json:"user_id"`
	Text          string        `json:"text"`
	Duration      time.Duration `json:"duration"`
	ProgressEvery time.Duration `json:"progress_every,omitempty"`
	// HeartbeatTimeout: 0 = DefaultMachineHeartbeatTimeout (the tests
	// shorten it).
	HeartbeatTimeout time.Duration `json:"heartbeat_timeout,omitempty"`
}

// MachineEchoOutput is the echo's result, or why no machine took it.
type MachineEchoOutput struct {
	Machine    string `json:"machine,omitempty"`
	Text       string `json:"text,omitempty"`
	Progresses int    `json:"progresses,omitempty"`
	Progress   string `json:"progress,omitempty"`
	NoMachine  string `json:"no_machine,omitempty"`
}

// machineEchoMargin is what an echo's time limit adds to its duration: the
// handoff, and a machine reconnecting.
const machineEchoMargin = time.Minute

// MachineEchoWorkflow runs an echo on a machine of the user's: phase 0's
// directive, which proves the mechanism end to end (PickMachine, then
// RunOnMachine, completed by the gateway). `agent machine-echo` starts it,
// to try it by hand.
func MachineEchoWorkflow(ctx workflow.Context, in MachineEchoInput) (MachineEchoOutput, error) {
	echo := machine.EchoInput{Text: in.Text, DurationMS: in.Duration.Milliseconds(), ProgressEveryMS: in.ProgressEvery.Milliseconds()}
	if err := echo.Check(); err != nil {
		return MachineEchoOutput{}, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeMachineDirective, nil)
	}
	input, err := json.Marshal(echo)
	if err != nil {
		return MachineEchoOutput{}, err
	}
	timeout := in.Duration + machineEchoMargin
	var acts *activity.MachineActivities
	var pick activity.PickMachineOutput
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, pickMachineOptions), acts.PickMachine,
		activity.PickMachineInput{UserID: in.UserID, Capability: machine.KindEcho, Kind: machine.KindEcho, Input: input,
			CallKey: "echo", Timeout: timeout}).Get(ctx, &pick); err != nil {
		return MachineEchoOutput{}, err
	}
	if pick.NoMachine != "" {
		return MachineEchoOutput{NoMachine: pick.NoMachine}, nil
	}
	out := MachineEchoOutput{Machine: pick.MachineName}
	var res machine.Result
	err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, runOnMachineOptions(timeout, in.HeartbeatTimeout)),
		acts.RunOnMachine, activity.RunOnMachineInput{DirectiveID: pick.DirectiveID}).Get(ctx, &res)
	if err != nil {
		return out, machineError(err, pick.MachineName, timeout, in.HeartbeatTimeout)
	}
	var echoed machine.EchoOutput
	if err := json.Unmarshal(res.Output, &echoed); err != nil {
		return out, temporal.NewNonRetryableApplicationError(fmt.Sprintf("machine %q answered no echo: %v", pick.MachineName, err),
			ErrTypeMachineDirective, nil)
	}
	out.Text, out.Progresses, out.Progress = echoed.Text, echoed.Progresses, res.Progress
	return out, nil
}
