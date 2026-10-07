package activity

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
)

// llmMachines records the calls to the model a worker creates and closes.
type llmMachines struct {
	created []store.LLMDirectiveRequest
	err     error
	closed  []string
}

func (f *llmMachines) CreateLLMDirective(_ context.Context, req store.LLMDirectiveRequest) (store.Directive, store.Machine, error) {
	f.created = append(f.created, req)
	return store.Directive{ID: req.DirectiveID}, store.Machine{ID: req.MachineID, Name: "maison"}, f.err
}

func (f *llmMachines) CloseDirective(_ context.Context, id, state, _ string) (bool, error) {
	f.closed = append(f.closed, state)
	return true, nil
}

// llmHandoff records what a worker hands the gateway.
type llmHandoff struct {
	requests []provider.ChatRequest
	err      error
}

func (h *llmHandoff) Deliver(context.Context, string) error {
	return errors.New("not a call to the model")
}

func (h *llmHandoff) DeliverLLM(_ context.Context, _ string, request json.RawMessage) error {
	var r provider.ChatRequest
	if err := json.Unmarshal(request, &r); err != nil {
		return err
	}
	h.requests = append(h.requests, r)
	return h.err
}

// The request is built as CallLLM builds it, handed over with its
// directive, never kept: the directive's input holds what the prompt held
// of the memory, its key the step and the attempt.
func TestCallLLMOnMachine(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	st := &memConversation{memory: map[string]store.Memory{"u-alice": {Content: "likes tea", Version: 3}}}
	st.add("msg:1", store.Message{Role: store.RoleUser, Content: text("hello"), UserID: "u-alice"})
	machines, handoff := &llmMachines{}, &llmHandoff{}
	llm := newLLM(failingProvider{errors.New("never the server's key")}, st)
	llm.Machines, llm.Handoff = machines, handoff
	env.RegisterActivity(llm)

	in := CallLLMOnMachineInput{MachineID: "m-1", Step: 2, Attempt: 3, Request: LLMTurnRequest{AgentID: "smith", UserID: "u-alice",
		Prompt:  PromptRef{MemoryOf: "u-alice"},
		History: &TurnHistory{SessionID: "s-1", TurnKey: "m1.smith"}}}
	_, err := env.ExecuteActivity(llm.CallLLMOnMachine, in)
	if !errors.Is(err, activity.ErrResultPending) {
		t.Fatalf("pending: %v", err)
	}
	if len(machines.created) != 1 || len(handoff.requests) != 1 {
		t.Fatalf("created %d, handed %d", len(machines.created), len(handoff.requests))
	}
	d := machines.created[0]
	var kept machine.LLMInput
	if d.CallKey != "llm:2:3" || d.MachineID != "m-1" || d.UserID != "u-alice" || len(d.TaskToken) == 0 || d.SessionID != "s-1" ||
		d.TurnKey != "m1.smith" || json.Unmarshal(d.Input, &kept) != nil || kept.MemoryVersion == nil || *kept.MemoryVersion != 3 {
		t.Errorf("directive %+v (input %s)", d, d.Input)
	}
	if strings.Contains(string(d.Input), "likes tea") || strings.Contains(string(d.Input), "hello") {
		t.Errorf("the request went into the database: %s", d.Input)
	}
	if r := handoff.requests[0]; !strings.Contains(r.System, "likes tea") || !strings.Contains(what(r), "hello") {
		t.Errorf("request %+v", r)
	}

	// The machine cannot take it: refused, nothing handed over.
	machines.err = store.ErrMachineUnavailable
	_, err = env.ExecuteActivity(llm.CallLLMOnMachine, in)
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != machine.ErrTypeRefused || len(handoff.requests) != 1 {
		t.Errorf("refused: %v", err)
	}

	// The gateway could not reach it: unreachable, the directive closed.
	machines.err, handoff.err = nil, errors.New("424")
	_, err = env.ExecuteActivity(llm.CallLLMOnMachine, in)
	if !errors.As(err, &appErr) || appErr.Type() != machine.ErrTypeUnreachable || len(machines.closed) != 1 || machines.closed[0] != store.DirectiveFailed {
		t.Errorf("unreachable: %v %v", err, machines.closed)
	}

	// Past the guard, or past what a machine reads: too long, nothing
	// created.
	handoff.err = nil
	llm.MaxContextBytes = 10
	before := len(machines.created)
	_, err = env.ExecuteActivity(llm.CallLLMOnMachine, in)
	if !errors.As(err, &appErr) || appErr.Type() != ErrContextTooLong || len(machines.created) != before {
		t.Errorf("too long: %v", err)
	}
}

// What the gateway completes the activity with reads as the workflow reads
// CallLLM's answer.
func TestLLMResult_IsATurnResponse(t *testing.T) {
	v := int64(4)
	raw, _ := json.Marshal(machine.LLMResult{ChatResponse: provider.ChatResponse{Content: "hi", StopReason: "end_turn", Model: "m",
		Usage: &provider.Usage{InputTokens: 2}}, PromptMemory: machine.PromptMemory{MemoryVersion: &v}})
	var resp LLMTurnResponse
	if err := json.Unmarshal(raw, &resp); err != nil || resp.Content != "hi" || resp.Model != "m" || resp.Usage.InputTokens != 2 ||
		resp.MemoryVersion == nil || *resp.MemoryVersion != 4 {
		t.Errorf("%+v %v", resp, err)
	}
	if ErrContextTooLong != machine.ErrTypeContextTooLong {
		t.Errorf("the machine's ContextTooLong is %q, CallLLM's %q", machine.ErrTypeContextTooLong, ErrContextTooLong)
	}
}

// No machine: a result, not an error; machines off or no author, none
// asked for.
func TestChooseMachine(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	acts := &MachineActivities{Store: &fakeMachineStore{}, Routing: CodingRouting{Machines: true}}
	env.RegisterActivity(acts)
	for name, c := range map[string]struct {
		machines bool
		user     string
	}{"off": {false, "u"}, "no author": {true, ""}, "none": {true, "u"}} {
		acts.Routing.Machines = c.machines
		v, err := env.ExecuteActivity(acts.ChooseMachine, ChooseMachineInput{UserID: c.user})
		var out ChooseMachineOutput
		if err != nil || v.Get(&out) != nil || out.Machine != nil || out.NoMachine == "" {
			t.Errorf("%s: %+v %v", name, out, err)
		}
	}
}
