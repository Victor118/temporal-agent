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
	"github.com/victor/temporal-agent/tool"
)

// codingRouteOptions: the route is the worker's configuration, read at once.
var codingRouteOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 10 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
}

// implementMachineTimeout bounds an implementation on a machine: the run,
// and around it the clone, the inspection, the push and the outputs, which
// a worker runs as steps of their own.
const implementMachineTimeout = implementTimeout + 30*time.Minute

// CodingRunWorkflow is analyze_repo as the main worker publishes it (design
// docs/design/machines.md §4): the run goes to a machine of the turn's
// author when one is online with Claude Code (PickMachine, then
// RunOnMachine: their CLI, their subscription or key); else to the fallback
// queue of the tool, where AnalyzeRepoWorkflow runs unchanged, as a child;
// else it says that there is nowhere to run it. Which of these the
// installation allows is the worker's configuration (CodingRoute).
func CodingRunWorkflow(ctx workflow.Context, rawInput json.RawMessage) (ClaudeCodeOutput, error) {
	return withContent(codingRun(ctx, analyzeTool, rawInput))
}

// ImplementRunWorkflow is implement_feature as the main worker publishes it,
// routed as CodingRunWorkflow routes an analysis: to a machine of the turn's
// author that has Claude Code and lets its runs push (--allow-push: the
// machine pushes the branch with its owner's git identity), else to the
// fallback queue of the tool, where ImplementFeatureWorkflow runs as a
// child, else nowhere, said so.
func ImplementRunWorkflow(ctx workflow.Context, rawInput json.RawMessage) (ClaudeCodeOutput, error) {
	return withContent(codingRun(ctx, implementTool, rawInput))
}

// codingTool is what tells the routing of one coding tool from the other's.
type codingTool struct {
	name string // the tool's
	kind string // its directive's (machine.Kind*)
	// what names the run in the agent's words: "the analysis".
	what string
	// timeout bounds its directive on a machine.
	timeout time.Duration
	// queue is its fallback queue in the worker's route ("" = none).
	queue func(activity.CodingRouting) string
	// parse reads the tool's input: what the output starts from, the
	// directive's input, and the call's context.
	parse func(ctx workflow.Context, raw json.RawMessage) (codingCall, error)
	// fallback starts the tool's workflow on its fallback queue, the
	// probe's answer given.
	fallback func(ctx workflow.Context, raw json.RawMessage, probe activity.ProbeRunWorkerOutput) workflow.ChildWorkflowFuture
	// noMachine says why no machine of the user's took it.
	noMachine string
}

// codingCall is a call of a coding tool, read.
type codingCall struct {
	out       ClaudeCodeOutput
	directive any
	call      tool.CallContext
}

var analyzeTool = codingTool{
	name: "analyze_repo", kind: machine.KindAnalyzeRepo, what: "the analysis", timeout: analyzeTimeout,
	queue: func(r activity.CodingRouting) string { return r.AnalyzeQueue },
	parse: func(_ workflow.Context, raw json.RawMessage) (codingCall, error) {
		var input AnalyzeRepoInput
		if err := json.Unmarshal(raw, &input); err != nil {
			return codingCall{}, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("invalid analyze_repo input: %v", err), "InvalidInput", nil)
		}
		if strings.TrimSpace(input.Repo) == "" || strings.TrimSpace(input.Task) == "" {
			return codingCall{}, temporal.NewNonRetryableApplicationError(
				"analyze_repo requires repo and task", "InvalidInput", nil)
		}
		return codingCall{
			out:       ClaudeCodeOutput{Repo: input.Repo, Ref: input.Ref},
			directive: machine.AnalyzeInput{Repo: input.Repo, Ref: input.Ref, Task: input.Task},
			call:      input.CallContext,
		}, nil
	},
	fallback: func(ctx workflow.Context, raw json.RawMessage, probe activity.ProbeRunWorkerOutput) workflow.ChildWorkflowFuture {
		return workflow.ExecuteChildWorkflow(ctx, AnalyzeFallbackWorkflow, AnalyzeFallbackInput{Input: raw, Probe: probe})
	},
	noMachine: "no machine of yours is connected with Claude Code (logged in) and a run to spare",
}

var implementTool = codingTool{
	name: "implement_feature", kind: machine.KindImplementFeature, what: "the run", timeout: implementMachineTimeout,
	queue: func(r activity.CodingRouting) string { return r.ImplementQueue },
	parse: func(ctx workflow.Context, raw json.RawMessage) (codingCall, error) {
		var input ImplementFeatureInput
		if err := json.Unmarshal(raw, &input); err != nil {
			return codingCall{}, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("invalid implement_feature input: %v", err), "InvalidInput", nil)
		}
		if strings.TrimSpace(input.Repo) == "" || strings.TrimSpace(input.Task) == "" {
			return codingCall{}, temporal.NewNonRetryableApplicationError(
				"implement_feature requires repo and task", "InvalidInput", nil)
		}
		// The machine's branch: named here, as ImplementFeatureWorkflow
		// names its own on a fallback.
		branch := branchName(input.Title, input.Task, workflow.GetInfo(ctx).WorkflowExecution.RunID)
		return codingCall{
			out: ClaudeCodeOutput{Repo: input.Repo, Ref: input.Base, Branch: branch},
			directive: machine.ImplementInput{Repo: input.Repo, Base: input.Base, Task: input.Task, Branch: branch,
				MaxBudgetUSD: max(input.MaxBudgetUSD, 0)},
			call: input.CallContext,
		}, nil
	},
	fallback: func(ctx workflow.Context, raw json.RawMessage, probe activity.ProbeRunWorkerOutput) workflow.ChildWorkflowFuture {
		return workflow.ExecuteChildWorkflow(ctx, ImplementFallbackWorkflow, ImplementFallbackInput{Input: raw, Probe: probe})
	},
	noMachine: "no machine of yours is connected with Claude Code (logged in), pushes allowed (agent connect --allow-push) and a run to spare",
}

func codingRun(ctx workflow.Context, t codingTool, rawInput json.RawMessage) (ClaudeCodeOutput, error) {
	c, err := t.parse(ctx, rawInput)
	if err != nil {
		return ClaudeCodeOutput{}, err
	}
	out := c.out

	var mAct *activity.MachineActivities
	var route activity.CodingRouting
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, codingRouteOptions), mAct.CodingRoute).Get(ctx, &route); err != nil {
		out.Error = fmt.Sprintf("could not find where to run %s: %v; nothing was done", t.what, err)
		return out, nil
	}

	var why string
	switch {
	case !route.Machines:
	case c.call.UserID == "":
		why = "the call has no user whose machine could run it"
	default:
		ran, refused, err := onMachine(ctx, t, c, &out)
		if ran || err != nil {
			return out, err
		}
		why = refused
		if why == "" {
			why = t.noMachine
		}
		out = c.out
	}
	if queue := t.queue(route); queue != "" {
		return onFallback(ctx, t, rawInput, queue, why, out)
	}
	switch {
	case why != "":
		out.Error = why + ", and this installation has no fallback for " + t.name + ": start agent connect on your machine (« Mes machines »); nothing was done"
	default:
		out.Error = t.name + " has nowhere to run on this installation (no machines, no fallback queue); nothing was done"
	}
	return out, nil
}

// onMachine runs the call on a machine of the turn's author, if one takes
// it (ran). One that turns it down before anything ran (a repository it does
// not allow, its login refused, pushes it does not allow, a handoff that
// failed) is refused, with why: the run goes elsewhere. A cancelled workflow
// is its error: the rest goes into out.
func onMachine(ctx workflow.Context, t codingTool, c codingCall, out *ClaudeCodeOutput) (ran bool, refused string, err error) {
	raw, err := json.Marshal(c.directive)
	if err != nil {
		return false, "", err
	}
	wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
	sessionID, _ := SessionOf(wfID)
	participant, _ := ParticipantOf(wfID)
	pin := activity.PickMachineInput{UserID: c.call.UserID, Capabilities: machine.CapabilitiesOf(t.kind), Kind: t.kind,
		Input: raw, CallKey: t.kind, Timeout: t.timeout,
		SessionID: sessionID, Participant: participant, Agent: c.call.Agent, CallID: c.call.CallID}
	// Where the files it publishes go: the turn the call works for, and the
	// agent that made it (the last of its chain).
	if turn := c.call.Turn; turn != nil && turn.SessionID == sessionID {
		pin.TurnKey = turn.TurnKey
	}
	if n := len(c.call.AgentChain); n > 0 {
		pin.AgentID = c.call.AgentChain[n-1]
	}
	var mAct *activity.MachineActivities
	var pick activity.PickMachineOutput
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, pickMachineOptions), mAct.PickMachine, pin).Get(ctx, &pick); err != nil {
		// The machines' database away: the fallback may still answer.
		workflow.GetLogger(ctx).Warn("No machine could be picked for a coding run", "tool", t.name, "error", err)
		return false, "", nil
	}
	if pick.NoMachine != "" {
		return false, "", nil
	}
	out.Machine = pick.MachineName
	started := workflow.Now(ctx)
	var res machine.Result
	err = workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, runOnMachineOptions(t.timeout, 0)),
		mAct.RunOnMachine, activity.RunOnMachineInput{DirectiveID: pick.DirectiveID}).Get(ctx, &res)
	switch {
	case err == nil:
		out.fromMachine(t, res)
		return true, "", nil
	case temporal.IsCanceledError(err):
		return true, "", err
	case hasErrorType(err, machine.ErrTypeRefused):
		var appErr *temporal.ApplicationError
		errors.As(err, &appErr)
		return false, fmt.Sprintf("your machine %q turned it down (%s)", pick.MachineName, appErr.Message()), nil
	}
	// What the machine said along with its failure: a partial report, how
	// far it got, what it published.
	var appErr *temporal.ApplicationError
	var partial machine.Result
	if errors.As(err, &appErr) && appErr.HasDetails() && appErr.Details(&partial) == nil {
		out.fromMachine(t, partial)
	}
	var why *temporal.ApplicationError
	if errors.As(machineError(err, pick.MachineName, t.timeout, 0), &why) {
		out.Error = joinErrors(out.Error, t.what+" did not complete: "+why.Message())
	}
	// A machine lost without a word after its run: it may have pushed.
	if t.kind == machine.KindImplementFeature && len(partial.Output) == 0 && cliRan(err, partial) {
		out.Error = joinErrors(out.Error, fmt.Sprintf("if the machine got as far as the push, the branch is on the remote: check %s there", out.Branch))
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
// text and numbers, nothing acted upon), and the files it published, as
// the gateway listed them.
func (o *ClaudeCodeOutput) fromMachine(t codingTool, res machine.Result) {
	for _, f := range res.Files {
		o.Files = append(o.Files, tool.FileRef{ID: f.ID, Name: f.Name, ContentType: f.ContentType, Size: f.Size, SHA256: f.SHA256})
	}
	var c machine.CodingOutput
	if len(res.Output) == 0 || json.Unmarshal(res.Output, &c) != nil {
		return
	}
	o.Report, o.Commit = c.Report, c.Commit
	o.CostUSD, o.PaidBy, o.DurationMS, o.NumTurns, o.ToolUses = c.CostUSD, c.PaidBy, c.DurationMS, c.NumTurns, c.ToolUses
	o.Unpublished = c.Unpublished
	if t.kind == machine.KindImplementFeature {
		// The branch is the workflow's own (Branch, set before): the
		// machine says what is on it.
		for _, cm := range c.Commits {
			o.Commits = append(o.Commits, activity.CommitInfo{SHA: cm.SHA, Subject: cm.Subject})
		}
		o.Pushed, o.Dirty = c.Pushed, c.Dirty
	}
	switch {
	case c.Interrupted:
		o.Interrupted = true
		o.Progress = &runProgress{Events: c.Events, ToolCalls: c.ToolCalls, LastTool: c.LastTool}
		if c.Error != "" {
			o.Error = t.what + " did not complete: " + c.Error
		}
	case c.Error != "":
		o.Error = c.Error
	case c.IsError:
		o.Error = fmt.Sprintf("the run reported a failure (%s)", c.Subtype)
	}
}

// onFallback runs the call on the installation's queue for its tool:
// AnalyzeRepoWorkflow or ImplementFeatureWorkflow as they always were, a
// child whose ID keeps the session's prefix (SessionOf, query_workflow),
// with the same input, call context included (AnalyzeFallbackWorkflow,
// ImplementFallbackWorkflow). A probe first: with no worker on the queue,
// the child would never start, and its own probe never run; its answer goes
// to the child, which does not ask again. noMachine says why no machine of
// the user's ran it, for the agent.
func onFallback(ctx workflow.Context, t codingTool, raw json.RawMessage, queue, noMachine string, out ClaudeCodeOutput) (ClaudeCodeOutput, error) {
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
	child := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID + ":fallback",
		TaskQueue:  queue,
	})
	var res ClaudeCodeOutput
	if err := t.fallback(child, raw, probe).Get(ctx, &res); err != nil {
		return res, err
	}
	if noMachine != "" {
		res.Note = noMachine + "; it ran on the installation's coding workers instead"
	}
	return res, nil
}
