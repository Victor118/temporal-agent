package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
)

// The model on the machine of a turn's author (docs/design/machine-llm.md):
// a turn of an agent set to prefer or require chooses a machine of its
// author at its start, and every step calls the model there
// (CallLLMOnMachine), one attempt per activity: the retries, the fallback to
// the server's key and the search for another machine are here, where an
// attempt can go elsewhere than the one before.

// Attempts at one step of a turn on a machine, as CallLLM's retry policy has
// on the server's key: 6, 5 s apart and three times longer each time, 2 min
// at most.
const (
	machineLLMAttempts   = 6
	machineLLMFirstWait  = 5 * time.Second
	machineLLMMaxWait    = 2 * time.Minute
	machineLLMAbsurdWait = time.Hour
)

// machineLLMOptions are CallLLMOnMachine's: one attempt (the workflow
// counts them), a heartbeat the gateway records every 30 s while the machine
// answers its pings, and a stop that waits for the machine to drop the call.
var machineLLMOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 180 * time.Second,
	HeartbeatTimeout:    2 * time.Minute,
	WaitForCancellation: true,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
}

// machineChoiceOptions are ChooseMachine's and SetMachineAside's: reads and
// writes of the database, quick.
var machineChoiceOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 10 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
}

// ErrTypeMachineRequired stops the turn of an agent that runs its model on
// its author's machine only, when none offers one.
const ErrTypeMachineRequired = "MachineRequired"

// MachineRequiredMessage is what the session's members read then.
const MachineRequiredMessage = "Cet agent fait tourner son modèle sur la machine de l'auteur du message seulement, et aucune de tes machines n'en offre un en ce moment : " +
	"lance agent connect avec ton modèle (--llm-provider, --llm-model, AGENT_CONNECT_LLM_API_KEY), puis renvoie ton message."

// MachinesOffMessage is what they read when the installation has its
// machines off: no machine of theirs could ever run it.
const MachinesOffMessage = "Cet agent fait tourner son modèle sur la machine de l'auteur du message seulement, et les machines sont désactivées sur cette installation (MACHINES_ENABLED=false) : " +
	"un admin doit le régler sur « jamais » ou « de préférence » dans /admin."

// subAgentMachinesOff is what a parent reads of a sub-agent stopped because
// the machines are off.
const subAgentMachinesOff = "The agent stopped without an answer: it runs its model on the user's machine only, and this installation has its machines off."

// subAgentNoMachine is what a parent reads of a sub-agent stopped for it.
const subAgentNoMachine = "The agent stopped without an answer: it runs its model on the user's machine only, and none of the user's machines offers one now."

// llmRoute is where a turn's model runs: the machine of its author, or the
// server's key (machine nil). mode is the agent's llm_on_machine.
type llmRoute struct {
	mode    string
	machine *activity.LLMMachine
	// excluded are the machines the turn set aside: lost, unreachable or
	// refusing; never chosen again in this turn, nor by its sub-agents.
	excluded []string
	// off: the installation has its machines off (ChooseMachine said so).
	off bool
	// note says the turn's line shows where its model runs; session turns
	// only (sessionID, participant, agent).
	noting      bool
	noted       bool
	sessionID   string
	participant string
	agent       string
}

// startLLMRoute settles where a turn's model runs, at its start: on the
// server's key for an agent set to never, a run outside any session turn or
// sub-agent (a scheduled task: phase 3.1), or a turn without author; on the
// machine inherited from the parent (a sub-agent); else on the machine
// ChooseMachine finds. None found: the server's key (prefer), or the turn
// stops (require: a MachineRequired error).
func startLLMRoute(ctx workflow.Context, input AgentWorkflowInput, mode, signer string) (*llmRoute, error) {
	r := &llmRoute{mode: mode, excluded: slices.Clone(input.LLMExcluded)}
	if mode == "" {
		r.mode = store.LLMOnMachineNever
	}
	if r.mode == store.LLMOnMachineNever || (input.TurnKey == "" && len(input.AgentChain) == 0) {
		return r, nil
	}
	if input.TurnKey != "" {
		wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
		r.sessionID, r.noting = input.SessionID, true
		r.participant, _ = ParticipantOf(wfID)
		r.agent = signer
	}
	if input.LLMMachine != nil {
		r.machine = input.LLMMachine
	} else if !r.choose(ctx, input.UserID) {
		return r, r.noMachine(ctx)
	}
	if r.machine != nil {
		r.note(ctx, onMachineNote(r.machine))
	}
	return r, nil
}

// onMachineNote is the turn's line while its model runs on m.
func onMachineNote(m *activity.LLMMachine) string {
	text := fmt.Sprintf("modèle sur la machine « %s »", m.Name)
	if m.Model != "" {
		text += " (" + m.Model + ")"
	}
	return text
}

// choose looks for a machine of userID's for the model, none of those
// excluded: r.machine, nil when there is none (false on require, where
// none stops the turn).
func (r *llmRoute) choose(ctx workflow.Context, userID string) bool {
	var mAct *activity.MachineActivities
	var out activity.ChooseMachineOutput
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, machineChoiceOptions), mAct.ChooseMachine,
		activity.ChooseMachineInput{UserID: userID, Excluded: r.excluded}).Get(ctx, &out)
	if err != nil {
		workflow.GetLogger(ctx).Warn("No machine could be chosen for the turn's model", "error", err)
	} else if out.NoMachine != "" {
		workflow.GetLogger(ctx).Info("No machine for the turn's model", "why", out.NoMachine)
	}
	r.machine, r.off = out.Machine, out.MachinesOff
	return r.machine != nil || r.mode != store.LLMOnMachineRequire
}

// noMachine is the error that stops a turn that requires a machine and has
// none: nil when the turn goes on, on the server's key.
func (r *llmRoute) noMachine(ctx workflow.Context) error {
	if r.mode != store.LLMOnMachineRequire || ctx.Err() != nil {
		return nil
	}
	if r.off {
		return temporal.NewNonRetryableApplicationError(MachinesOffMessage, ErrTypeMachineRequired, nil)
	}
	return temporal.NewNonRetryableApplicationError(MachineRequiredMessage, ErrTypeMachineRequired, nil)
}

// call makes one step's call to the model, where the turn's model runs, and
// returns its answer and the machine that wrote it (nil: the server's key).
// On a machine, each attempt is a directive of its own: a passing failure is
// tried again on the same machine after a wait (the provider's own when it
// said one, capped), a lost or refusing machine is set aside, and the step
// with the rest of the turn goes to the server's key (prefer) or to another
// of the author's machines (require; none: MachineRequired). A refusal of
// the provider for good, or a conversation too long, stops the turn as on
// the server's key. A cancellation, during a call or a wait, is returned:
// the turn stops.
func (r *llmRoute) call(ctx workflow.Context, llmCtx workflow.Context, queue string, step int, userID string,
	request func() activity.LLMTurnRequest) (activity.LLMTurnResponse, *activity.LLMMachine, error) {
	var llmAct *activity.LLMActivities
	opts := machineLLMOptions
	if queue != "" {
		opts.TaskQueue = queue
	}
	machineCtx := workflow.WithActivityOptions(ctx, opts)
	var last error
	onServer := func() (activity.LLMTurnResponse, *activity.LLMMachine, error) {
		var resp activity.LLMTurnResponse
		err := workflow.ExecuteActivity(llmCtx, llmAct.CallLLM, request()).Get(ctx, &resp)
		return resp, nil, err
	}
	if r.machine == nil {
		return onServer()
	}
	for attempt := 1; attempt <= machineLLMAttempts; attempt++ {
		m := r.machine
		var resp activity.LLMTurnResponse
		err := workflow.ExecuteActivity(machineCtx, llmAct.CallLLMOnMachine, activity.CallLLMOnMachineInput{
			Request: request(), MachineID: m.ID, Step: step, Attempt: attempt}).Get(ctx, &resp)
		if err == nil {
			return resp, m, nil
		}
		if ctx.Err() != nil || temporal.IsCanceledError(err) {
			return resp, m, err
		}
		last = err
		kind, aside := machineFailure(err)
		switch kind {
		case failureFinal:
			return resp, m, err
		case failureWorker:
			// The worker of CallLLM's queue hands no call to a machine:
			// another machine would fare no better, and this one is not to
			// blame.
			workflow.GetLogger(ctx).Error("The worker of the turn's model cannot reach the machines", "error", err)
			if r.mode == store.LLMOnMachineRequire {
				return resp, m, err
			}
			r.machine = nil
			r.note(ctx, "modèle de l'installation : ce worker n'atteint pas les machines")
			return onServer()
		case failureMachine, failureRefused:
			workflow.GetLogger(ctx).Warn("The machine of the turn's model failed it", "machine", m.ID, "error", err)
			if aside {
				r.setAside(ctx, m.ID)
			}
			r.excluded = append(r.excluded, m.ID)
			if r.mode == store.LLMOnMachineRequire {
				if !r.choose(ctx, userID) {
					return resp, nil, r.noMachine(ctx)
				}
				r.note(ctx, onMachineNote(r.machine))
				continue
			}
			// prefer: the step, and the rest of the turn, on the server's
			// key. The prompt cache's prefix is lost: a cost, not an error.
			r.machine = nil
			r.note(ctx, fmt.Sprintf("modèle de l'installation : la machine « %s » ne répond plus", m.Name))
			return onServer()
		}
		if attempt == machineLLMAttempts {
			break
		}
		if err := workflow.Sleep(ctx, retryWait(err, attempt)); err != nil {
			return resp, m, err
		}
	}
	return activity.LLMTurnResponse{}, r.machine, last
}

// Failure kinds of a call on a machine.
const (
	failurePassing = iota
	// failureFinal: the provider refused the request for good, or the
	// conversation outgrew the model: as on the server's key, never retried.
	failureFinal
	// failureMachine: the machine is lost or out of reach (no heartbeat,
	// unreachable at the handoff, reconnected, stopped, revoked): set aside.
	failureMachine
	// failureRefused: the machine turned the call down before anything
	// (offline, paused, its model withdrawn, set aside). One at its cap is
	// busy, which passes: failurePassing, the same machine later.
	failureRefused
	// failureWorker: the worker that took the call hands none to a machine
	// (no gateway on it): neither the machine's fault nor another's cure.
	failureWorker
)

// machineFailure tells what a failed attempt on a machine calls for, and
// whether to set the machine aside for the turns to come: lost (no
// heartbeat), unreachable at the handoff, stopped, revoked. Not one that
// lost the call by connecting again (DirectiveLost: it is back), nor one
// whose gateway this worker could not reach (HandoffFailed: the server's
// trouble): excluded from the turn only.
func machineFailure(err error) (kind int, aside bool) {
	var timeoutErr *temporal.TimeoutError
	if errors.As(err, &timeoutErr) && timeoutErr.TimeoutType() == enumspb.TIMEOUT_TYPE_HEARTBEAT {
		return failureMachine, true
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		return failurePassing, false
	}
	switch appErr.Type() {
	case machine.ErrTypePermanentAPI, machine.ErrTypeContextTooLong:
		return failureFinal, false
	case machine.ErrTypeUnreachable, machine.ErrTypeStopping, machine.ErrTypeRevoked:
		return failureMachine, true
	case machine.ErrTypeLost, machine.ErrTypeHandoffFailed:
		return failureMachine, false
	case machine.ErrTypeRefused:
		return failureRefused, false
	case machine.ErrTypeNoGateway:
		return failureWorker, false
	}
	return failurePassing, false
}

// retryWait is how long a step waits before its next attempt on the same
// machine: what the provider asked for, capped at 2 min (a wait no API means
// is ignored: a machine is not trusted), else the policy's interval.
func retryWait(err error, attempt int) time.Duration {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.Type() == machine.ErrTypeRetryAfter && appErr.HasDetails() {
		var f machine.LLMFailure
		if appErr.Details(&f) == nil {
			if d := f.RetryAfter(); d > 0 && d <= machineLLMAbsurdWait {
				return min(d, machineLLMMaxWait)
			}
		}
	}
	wait := machineLLMFirstWait
	for i := 1; i < attempt && wait < machineLLMMaxWait; i++ {
		wait *= 3
	}
	return min(wait, machineLLMMaxWait)
}

// setAside sets a machine aside for the model until it connects again, for
// the turns to come. Best effort.
func (r *llmRoute) setAside(ctx workflow.Context, id string) {
	var mAct *activity.MachineActivities
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, machineChoiceOptions), mAct.SetMachineAside,
		activity.SetMachineAsideInput{MachineID: id}).Get(ctx, nil); err != nil && ctx.Err() == nil {
		workflow.GetLogger(ctx).Warn("A machine that lost a call was not set aside", "machine", id, "error", err)
	}
}

// note shows text on the turn's line (activity.EventNotice, web only); ""
// takes it off. Best effort. A session turn only: a sub-agent's machine is
// its parent's, said already.
func (r *llmRoute) note(ctx workflow.Context, text string) {
	if !r.noting || (text == "" && !r.noted) {
		return
	}
	data, _ := json.Marshal(map[string]string{
		"type":        activity.EventNotice,
		"text":        text,
		"agent":       r.agent,
		"participant": r.participant,
	})
	var notifAct *activity.NotificationActivities
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, turnNotifyOptions), notifAct.NotifyStep, activity.NotifyInput{
		SessionID: r.sessionID,
		Event:     activity.SSEEvent{Type: activity.EventNotice, Data: data},
	}).Get(ctx, nil)
	if err != nil && ctx.Err() == nil {
		workflow.GetLogger(ctx).Warn("The note of the turn's model was not delivered", "error", err)
	}
	r.noted = text != ""
}

// clearNote takes the turn's note off its line once it ends, cancelled
// included.
func (r *llmRoute) clearNote(ctx workflow.Context) {
	if !r.noted {
		return
	}
	if ctx.Err() != nil {
		dctx, cancel := workflow.NewDisconnectedContext(ctx)
		defer cancel()
		r.note(dctx, "")
		return
	}
	r.note(ctx, "")
}

// stampAnswer writes on an assistant message what wrote it: the model, what
// its call took, and the machine, when the turn's model ran on one.
func stampAnswer(msg *store.Message, resp provider.ChatResponse, on *activity.LLMMachine) {
	msg.Model = resp.Model
	if u := resp.Usage; u != nil {
		msg.Usage = &store.Usage{InputTokens: u.InputTokens, OutputTokens: u.OutputTokens,
			CacheCreationInputTokens: u.CacheCreationInputTokens, CacheReadInputTokens: u.CacheReadInputTokens}
	}
	if on != nil {
		msg.MachineID, msg.Machine = on.ID, on.Name
		if msg.Model == "" {
			msg.Model = on.Model
		}
	}
}
