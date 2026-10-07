package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/store"
)

// The model on the machine of a turn's author (docs/design/machine-llm.md):
// the turn chooses a machine (ChooseMachine), then each of its calls to the
// model is CallLLMOnMachine instead of CallLLM. The request is built here, as
// CallLLM builds it, and goes to the gateway in the handoff's body, then to
// the machine on its WebSocket: never through Temporal, never to the
// database. The machine's answer completes the activity.

// ChooseMachineInput asks for a machine of UserID's for a turn's model,
// none of Excluded (the machines the turn set aside).
type ChooseMachineInput struct {
	UserID   string   `json:"user_id"`
	Excluded []string `json:"excluded,omitempty"`
}

// LLMMachine is the machine a turn's model runs on: all its calls go there,
// with the same model and the same prompt cache. A sub-agent inherits it.
type LLMMachine struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Model is the model the machine said it calls (its hello): the
	// answers say which one wrote them.
	Model string `json:"model,omitempty"`
}

// ChooseMachineOutput is the machine chosen, or NoMachine: why none.
type ChooseMachineOutput struct {
	Machine   *LLMMachine `json:"machine,omitempty"`
	NoMachine string      `json:"no_machine,omitempty"`
}

// ChooseMachine chooses a machine of the turn's author for its model, and
// creates nothing: online, not paused, with its model, not set aside, under
// its cap of calls, none of those the turn excluded; the highest priority,
// then the least busy (store.ChooseLLMMachine). No machine is a result:
// trying again would not make one appear. The turn keeps the machine: a
// result in its history, the same on a replay.
func (a *MachineActivities) ChooseMachine(ctx context.Context, in ChooseMachineInput) (ChooseMachineOutput, error) {
	switch {
	case !a.Routing.Machines:
		return ChooseMachineOutput{NoMachine: "this installation has its machines off (MACHINES_ENABLED=false)"}, nil
	case in.UserID == "":
		return ChooseMachineOutput{NoMachine: "the turn has no author whose machine could run its model"}, nil
	}
	m, err := a.Store.ChooseLLMMachine(ctx, in.UserID, in.Excluded, time.Now().Add(-a.onlineWindow()))
	if errors.Is(err, store.ErrNoMachine) {
		return ChooseMachineOutput{NoMachine: "no machine of the author's is connected with a model to spare"}, nil
	}
	if err != nil {
		return ChooseMachineOutput{}, err
	}
	return ChooseMachineOutput{Machine: &LLMMachine{ID: m.ID, Name: m.Name, Model: m.LLMModel}}, nil
}

// SetMachineAsideInput names a machine that lost a call to the model.
type SetMachineAsideInput struct {
	MachineID string `json:"machine_id"`
}

// SetMachineAside sets a machine aside for the model until it connects
// again: lost (no heartbeat), or unreachable when its call was handed over,
// it would otherwise still look online for a while, and the next turns
// would choose it and wait for nothing.
func (a *MachineActivities) SetMachineAside(ctx context.Context, in SetMachineAsideInput) error {
	return a.Store.SetMachineAside(ctx, in.MachineID)
}

func (a *MachineActivities) onlineWindow() time.Duration {
	if a.OnlineWindow > 0 {
		return a.OnlineWindow
	}
	return DefaultMachineOnlineWindow
}

// CallLLMOnMachineInput is one attempt at one call of a turn, on the
// turn's machine: the same request as CallLLM's, by reference, with the
// step and the attempt that make its directive's key.
type CallLLMOnMachineInput struct {
	Request   LLMTurnRequest `json:"request"`
	MachineID string         `json:"machine_id"`
	Step      int            `json:"step"`
	Attempt   int            `json:"attempt"`
}

// LLMCallKey is an attempt's directive key: one directive per attempt, the
// workflow counting them.
func LLMCallKey(step, attempt int) string {
	return fmt.Sprintf("llm:%d:%d", step, attempt)
}

// LLMMachineStore is what CallLLMOnMachine reads and writes of the
// machines.
type LLMMachineStore interface {
	CreateLLMDirective(ctx context.Context, req store.LLMDirectiveRequest) (store.Directive, store.Machine, error)
	CloseDirective(ctx context.Context, id, state, errText string) (bool, error)
}

// CallLLMOnMachine builds the request as CallLLM does, under the same guard
// (and under what a machine reads of one message), creates its directive on
// the turn's machine (under the machine's cap of calls, with what the
// prompt holds of the memory, which comes back with the answer), hands it to
// the gateway with the request, and returns pending: the gateway completes
// it with the machine's answer. One attempt: the workflow retries, falls
// back or stops (AgentWorkflow). A machine that cannot take the call is
// DirectiveRefused; one the gateway could not reach, MachineUnreachable:
// nothing ran.
func (a *LLMActivities) CallLLMOnMachine(ctx context.Context, in CallLLMOnMachineInput) (LLMTurnResponse, error) {
	if a.Machines == nil || a.Handoff == nil {
		return LLMTurnResponse{}, temporal.NewNonRetryableApplicationError(
			"this worker hands no call to a machine (no gateway, MACHINES_ENABLED, NOTIFY_URL)", machine.ErrTypeUnreachable, nil)
	}
	req := in.Request
	request, memory, err := a.buildRequest(ctx, req)
	if err != nil {
		return LLMTurnResponse{}, err
	}
	body, err := json.Marshal(request)
	if err != nil {
		return LLMTurnResponse{}, temporal.NewNonRetryableApplicationError(ContextTooLongMessage, ErrContextTooLong, err)
	}
	limit := min(a.maxContextBytes(), machine.MaxLLMRequestBytes)
	if len(body) > limit {
		log.Printf("LLM call on a machine refused: agent %s, %d bytes over the %d limit", req.AgentID, len(body), limit)
		return LLMTurnResponse{}, temporal.NewNonRetryableApplicationError(ContextTooLongMessage, ErrContextTooLong, nil)
	}

	info := activity.GetInfo(ctx)
	input, err := json.Marshal(machine.LLMInput{PromptMemory: memory})
	if err != nil {
		return LLMTurnResponse{}, err
	}
	dr := store.LLMDirectiveRequest{DirectiveID: uuid.NewString(), MachineID: in.MachineID, UserID: req.UserID, Input: input,
		WorkflowID: info.WorkflowExecution.ID, RunID: info.WorkflowExecution.RunID, ActivityID: info.ActivityID,
		CallKey: LLMCallKey(in.Step, in.Attempt), TaskToken: info.TaskToken, Deadline: info.Deadline,
		SeenAfter: time.Now().Add(-a.onlineWindow()), AgentID: req.AgentID}
	if h := req.History; h != nil {
		dr.SessionID, dr.TurnKey = h.SessionID, h.TurnKey
	}
	d, m, err := a.Machines.CreateLLMDirective(ctx, dr)
	switch {
	case errors.Is(err, store.ErrMachineUnavailable), errors.Is(err, store.ErrDirectiveClosed):
		return LLMTurnResponse{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("the machine cannot take the call: %v", err), machine.ErrTypeRefused, nil)
	case err != nil:
		return LLMTurnResponse{}, err
	}

	hctx, cancel := context.WithTimeout(ctx, machine.LLMHandoffTimeout)
	defer cancel()
	if err := a.Handoff.DeliverLLM(hctx, d.ID, body); err != nil {
		// Never handed: nobody will end it. The gateway closed it already
		// when it said the machine was unreachable.
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelClose()
		a.Machines.CloseDirective(closeCtx, d.ID, store.DirectiveFailed, "not handed to the machine")
		return LLMTurnResponse{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("the call could not be handed to machine %q: %v", m.Name, err), machine.ErrTypeUnreachable, nil)
	}
	return LLMTurnResponse{}, activity.ErrResultPending
}

func (a *LLMActivities) onlineWindow() time.Duration {
	if a.OnlineWindow > 0 {
		return a.OnlineWindow
	}
	return DefaultMachineOnlineWindow
}
