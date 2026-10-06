package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/machine"
)

// codingRouteOptions: the route is the worker's configuration, read at once.
var codingRouteOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 10 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
}

// CodingRunWorkflow is analyze_repo as the main worker publishes it (design
// docs/design/machines.md §4): the run goes to a machine of the turn's
// author when one is online with Claude Code (PickMachine, then
// RunOnMachine: their CLI, their subscription or key); else to the fallback
// queue of the tool, where AnalyzeRepoWorkflow runs unchanged, as a child;
// else it says that there is nowhere to run it. Which of these the
// installation allows is the worker's configuration (CodingRoute).
//
// Phase 1: analyze_repo only. implement_feature keeps its own workflow on its
// coding queue until phase 2.
func CodingRunWorkflow(ctx workflow.Context, rawInput json.RawMessage) (ClaudeCodeOutput, error) {
	return withContent(codingRun(ctx, rawInput))
}

func codingRun(ctx workflow.Context, rawInput json.RawMessage) (ClaudeCodeOutput, error) {
	var input AnalyzeRepoInput
	if err := json.Unmarshal(rawInput, &input); err != nil {
		return ClaudeCodeOutput{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("invalid analyze_repo input: %v", err), "InvalidInput", nil)
	}
	if strings.TrimSpace(input.Repo) == "" || strings.TrimSpace(input.Task) == "" {
		return ClaudeCodeOutput{}, temporal.NewNonRetryableApplicationError(
			"analyze_repo requires repo and task", "InvalidInput", nil)
	}
	// Probe is the workflow's to set for its fallback, never the model's.
	input.Probe = nil
	out := ClaudeCodeOutput{Repo: input.Repo, Ref: input.Ref}

	var mAct *activity.MachineActivities
	var route activity.CodingRouting
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, codingRouteOptions), mAct.CodingRoute).Get(ctx, &route); err != nil {
		out.Error = fmt.Sprintf("could not find where to run the analysis: %v; nothing was done", err)
		return out, nil
	}

	var why string
	switch {
	case !route.Machines:
	case input.UserID == "":
		why = "the call has no user whose machine could run it"
	default:
		ran, refused, err := analyzeOnMachine(ctx, input, &out)
		if ran || err != nil {
			return out, err
		}
		why = refused
		if why == "" {
			why = "no machine of yours is connected with Claude Code (logged in) and a run to spare"
		}
		out.Machine = ""
	}
	if route.AnalyzeQueue != "" {
		return analyzeOnFallback(ctx, input, route.AnalyzeQueue, why, out)
	}
	switch {
	case why != "":
		out.Error = why + ", and this installation has no fallback for analyze_repo: start agent connect on your machine (« Mes machines »); nothing was done"
	default:
		out.Error = "analyze_repo has nowhere to run on this installation (no machines, no fallback queue); nothing was done"
	}
	return out, nil
}

// analyzeOnMachine runs the analysis on a machine of the turn's author, if
// one takes it (ran). One that turns it down before anything ran (a
// repository it does not allow, its login refused, a handoff that failed)
// is refused, with why: the run goes elsewhere. A cancelled workflow is its
// error: the rest goes into out.
func analyzeOnMachine(ctx workflow.Context, input AnalyzeRepoInput, out *ClaudeCodeOutput) (ran bool, refused string, err error) {
	raw, err := json.Marshal(machine.AnalyzeInput{Repo: input.Repo, Ref: input.Ref, Task: input.Task})
	if err != nil {
		return false, "", err
	}
	wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
	sessionID, _ := SessionOf(wfID)
	participant, _ := ParticipantOf(wfID)
	var mAct *activity.MachineActivities
	var pick activity.PickMachineOutput
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, pickMachineOptions), mAct.PickMachine,
		activity.PickMachineInput{UserID: input.UserID, Capability: machine.CapClaudeCode, Kind: machine.KindAnalyzeRepo,
			Input: raw, CallKey: "analyze", Timeout: analyzeTimeout,
			SessionID: sessionID, Participant: participant, Agent: input.Agent}).Get(ctx, &pick); err != nil {
		// The machines' database away: the fallback may still answer.
		workflow.GetLogger(ctx).Warn("No machine could be picked for an analysis", "error", err)
		return false, "", nil
	}
	if pick.NoMachine != "" {
		return false, "", nil
	}
	out.Machine = pick.MachineName
	started := workflow.Now(ctx)
	var res machine.Result
	err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, runOnMachineOptions(analyzeTimeout, 0)),
		mAct.RunOnMachine, activity.RunOnMachineInput{DirectiveID: pick.DirectiveID}).Get(ctx, &res)
	switch {
	case err == nil:
		out.fromMachine(res.Output)
		return true, "", nil
	case temporal.IsCanceledError(err):
		return true, "", err
	case hasErrorType(err, machine.ErrTypeRefused):
		var appErr *temporal.ApplicationError
		errors.As(err, &appErr)
		return false, fmt.Sprintf("your machine %q turned it down (%s)", pick.MachineName, appErr.Message()), nil
	}
	// What the machine said along with its failure: a partial report, how
	// far it got.
	var appErr *temporal.ApplicationError
	var partial machine.Result
	if errors.As(err, &appErr) && appErr.HasDetails() && appErr.Details(&partial) == nil {
		out.fromMachine(partial.Output)
	}
	var why *temporal.ApplicationError
	if errors.As(machineError(err, pick.MachineName, analyzeTimeout, 0), &why) {
		out.Error = "the analysis did not complete: " + why.Message()
	}
	// Interrupted — its cost unknown, a retry starting over — only if the
	// CLI ran: what the machine or its last heartbeat says.
	if !out.Interrupted && cliRan(err, partial) {
		out.Interrupted = true
		out.DurationMS = workflow.Now(ctx).Sub(started).Milliseconds()
	}
	return true, "", nil
}

// cliRan tells whether a directive that failed with err had started its CLI:
// its last progress, in the machine's last word (partial) or in the last
// heartbeat of a timed out activity, went past the clone.
func cliRan(err error, partial machine.Result) bool {
	progress := partial.Progress
	var timeoutErr *temporal.TimeoutError
	if progress == "" && errors.As(err, &timeoutErr) && timeoutErr.HasLastHeartbeatDetails() {
		var hb machine.Heartbeat
		if timeoutErr.LastHeartbeatDetails(&hb) == nil {
			progress = hb.Progress
		}
	}
	return progress != "" && progress != machine.CloneProgress
}

// fromMachine fills o with what a machine's run says of itself (untrusted:
// text and numbers, nothing acted upon).
func (o *ClaudeCodeOutput) fromMachine(raw json.RawMessage) {
	var c machine.CodingOutput
	if len(raw) == 0 || json.Unmarshal(raw, &c) != nil {
		return
	}
	o.Report, o.Commit = c.Report, c.Commit
	o.CostUSD, o.PaidBy, o.DurationMS, o.NumTurns, o.ToolUses = c.CostUSD, c.PaidBy, c.DurationMS, c.NumTurns, c.ToolUses
	switch {
	case c.Interrupted:
		o.Interrupted = true
		o.Progress = &runProgress{Events: c.Events, ToolCalls: c.ToolCalls, LastTool: c.LastTool}
		if c.Error != "" {
			o.Error = "the analysis did not complete: " + c.Error
		}
	case c.Error != "":
		o.Error = c.Error
	case c.IsError:
		o.Error = fmt.Sprintf("the run reported a failure (%s)", c.Subtype)
	}
}

// analyzeOnFallback runs the analysis on the installation's queue for it:
// AnalyzeRepoWorkflow as it always was, a child whose ID keeps the session's
// prefix (SessionOf, query_workflow), with the same input, call context
// included. A probe first: with no worker on the queue, the child would
// never start, and its own probe never run; its answer goes to the child,
// which does not ask again. noMachine says why no machine of the user's ran
// it, for the agent.
func analyzeOnFallback(ctx workflow.Context, input AnalyzeRepoInput, queue, noMachine string, out ClaudeCodeOutput) (ClaudeCodeOutput, error) {
	prefix := ""
	if noMachine != "" {
		prefix = noMachine + ", and "
	}
	var ccAct *activity.ClaudeCodeActivities
	var probe activity.ProbeRunWorkerOutput
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue:              queue,
			ScheduleToStartTimeout: runProbeTimeout,
			StartToCloseTimeout:    10 * time.Second,
			RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
		}),
		ccAct.ProbeRunWorker,
	).Get(ctx, &probe)
	switch {
	case isScheduleToStartTimeout(err):
		out.Error = prefix + fmt.Sprintf("no worker of the installation's fallback (%q) is available; nothing was done", queue)
		return out, nil
	case hasErrorType(err, activity.ErrNoClaudeCLI):
		out.Error = prefix + fmt.Sprintf("a worker of the installation's fallback (%q) cannot run Claude Code (no CLI); nothing was done", queue)
		return out, nil
	case err != nil:
		out.Error = prefix + fmt.Sprintf("could not reach the installation's fallback (%q): %v; nothing was done", queue, err)
		return out, nil
	}
	input.Probe = &probe
	raw, err := json.Marshal(input)
	if err != nil {
		return out, err
	}
	child := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID + ":fallback",
		TaskQueue:  queue,
	})
	var res ClaudeCodeOutput
	if err := workflow.ExecuteChildWorkflow(child, AnalyzeRepoWorkflow, json.RawMessage(raw)).Get(ctx, &res); err != nil {
		return res, err
	}
	if noMachine != "" {
		res.Note = noMachine + "; it ran on the installation's coding workers instead"
	}
	return res, nil
}
