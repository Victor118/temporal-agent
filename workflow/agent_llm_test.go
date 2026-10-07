package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
)

// machineWorld is the machines a turn's model may run on: which machine
// ChooseMachine gives (the first of free not excluded), and what a call on
// each answers.
type machineWorld struct {
	mu      sync.Mutex
	modes   map[string]string // agent → llm_on_machine
	free    []string          // machines ChooseMachine may give, in order
	chooses [][]string        // the exclusions of each ChooseMachine
	aside   []string
	calls   []activity.CallLLMOnMachineInput
	notes   []string
	answer  func(n int, in activity.CallLLMOnMachineInput) (activity.LLMTurnResponse, error)
}

func (w *machineWorld) register(env *testsuite.TestWorkflowEnvironment, tools activity.ListToolsOutput) {
	env.RegisterActivityWithOptions(func(context.Context, activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return tools, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{Name: in.AgentID, LLMOnMachine: w.modes[in.AgentID]}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.NotifyInput) error {
		if in.Event.Type == activity.EventNotice {
			var n struct{ Text string }
			json.Unmarshal(in.Event.Data, &n)
			w.mu.Lock()
			w.notes = append(w.notes, n.Text)
			w.mu.Unlock()
		}
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.ChooseMachineInput) (activity.ChooseMachineOutput, error) {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.chooses = append(w.chooses, slices.Clone(in.Excluded))
		for _, id := range w.free {
			if !slices.Contains(in.Excluded, id) {
				return activity.ChooseMachineOutput{Machine: &activity.LLMMachine{ID: id, Name: "machine-" + id, Model: "model-" + id}}, nil
			}
		}
		return activity.ChooseMachineOutput{NoMachine: "none"}, nil
	}, sdkactivity.RegisterOptions{Name: "ChooseMachine"})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.SetMachineAsideInput) error {
		w.mu.Lock()
		w.aside = append(w.aside, in.MachineID)
		w.mu.Unlock()
		return nil
	}, sdkactivity.RegisterOptions{Name: "SetMachineAside"})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.CallLLMOnMachineInput) (activity.LLMTurnResponse, error) {
		w.mu.Lock()
		w.calls = append(w.calls, in)
		n := len(w.calls)
		w.mu.Unlock()
		return w.answer(n, in)
	}, sdkactivity.RegisterOptions{Name: "CallLLMOnMachine"})
	env.RegisterActivityWithOptions(func(context.Context, activity.ExecuteToolInput) (activity.ExecuteToolOutput, error) {
		return activity.ExecuteToolOutput{Content: "page"}, nil
	}, sdkactivity.RegisterOptions{Name: "ExecuteTool"})
}

// keys are the directive keys of the calls made on machines, with their
// machine.
func (w *machineWorld) keys() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []string
	for _, c := range w.calls {
		out = append(out, c.MachineID+"/"+activity.LLMCallKey(c.Step, c.Attempt))
	}
	return out
}

var fetchTools = activity.ListToolsOutput{
	Tools:       []provider.ToolDefinition{{Name: "web_fetch", InputSchema: json.RawMessage(`{"type":"object"}`)}},
	Resolutions: map[string]activity.ToolResolution{"web_fetch": {Kind: "activity", TaskQueue: "tools-web"}},
}

var fetchCall = provider.ChatResponse{StopReason: "tool_use", ToolCalls: []provider.ToolCallInfo{{ID: "c1", Name: "web_fetch", Input: json.RawMessage(`{}`)}}}

// machineAnswers answers the n-th call on a machine with the n-th of
// responses (an error for a nil response: errs), the last one past the end.
func machineAnswers(responses ...any) func(int, activity.CallLLMOnMachineInput) (activity.LLMTurnResponse, error) {
	return func(n int, in activity.CallLLMOnMachineInput) (activity.LLMTurnResponse, error) {
		switch r := responses[min(n, len(responses))-1].(type) {
		case error:
			return activity.LLMTurnResponse{}, r
		case provider.ChatResponse:
			r.Model = "model-" + in.MachineID
			return activity.LLMTurnResponse{ChatResponse: r}, nil
		}
		panic("bad response")
	}
}

// runTurn runs a session turn of jarvis for victor, his question stored.
func runTurn(t *testing.T, env *testsuite.TestWorkflowEnvironment, f *llmFakes) AgentWorkflowOutput {
	t.Helper()
	upTo := f.session.add(store.HumanMessageKey("q"), store.Message{Role: store.RoleUser, Content: `"question"`, UserID: "victor"})
	env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", UserID: "victor", AgentID: "jarvis", TurnKey: store.TurnKey(upTo, "jarvis")})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	var out AgentWorkflowOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// A turn of an agent set to prefer runs every step on the machine chosen at
// its start, each its own directive key; its answers say which machine and
// model wrote them; the turn's line says so, and is cleared at its end.
func TestAgentWorkflow_ModelOnTheMachine(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	w := &machineWorld{modes: map[string]string{"jarvis": store.LLMOnMachinePrefer}, free: []string{"m1"},
		answer: machineAnswers(fetchCall, done)}
	w.register(env, fetchTools)
	f := registerLLM(env, answers(provider.ChatResponse{Content: "from the server"}))

	out := runTurn(t, env, f)
	if out.Response != "done" || len(f.model.sent()) != 0 {
		t.Fatalf("answer %q, server calls %d", out.Response, len(f.model.sent()))
	}
	if got := w.keys(); !slices.Equal(got, []string{"m1/llm:0:1", "m1/llm:1:1"}) {
		t.Errorf("calls %v", got)
	}
	for _, m := range out.NewMessages {
		if m.Role == store.RoleAssistant && (m.MachineID != "m1" || m.Machine != "machine-m1" || m.Model != "model-m1") {
			t.Errorf("answer not stamped: %+v", m)
		}
	}
	if !slices.Equal(w.notes, []string{"modèle sur la machine « machine-m1 » (model-m1)", ""}) {
		t.Errorf("notes %q", w.notes)
	}
}

// An agent set to never calls the server's key, as before: no machine
// chosen.
func TestAgentWorkflow_NeverOnAMachine(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	w := &machineWorld{modes: map[string]string{}, free: []string{"m1"}, answer: machineAnswers(done)}
	w.register(env, fetchTools)
	f := registerLLM(env, answers(done))
	runTurn(t, env, f)
	if len(w.chooses) != 0 || len(w.calls) != 0 || len(f.model.sent()) != 1 || len(w.notes) != 0 {
		t.Errorf("chooses %d, machine calls %d, server calls %d, notes %q", len(w.chooses), len(w.calls), len(f.model.sent()), w.notes)
	}
}

// No machine at the start: prefer takes the server's key, require stops the
// turn with a clear word, calling nothing.
func TestAgentWorkflow_NoMachine(t *testing.T) {
	for _, mode := range []string{store.LLMOnMachinePrefer, store.LLMOnMachineRequire} {
		t.Run(mode, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			w := &machineWorld{modes: map[string]string{"jarvis": mode}, answer: machineAnswers(done)}
			w.register(env, fetchTools)
			f := registerLLM(env, answers(done))
			out := runTurn(t, env, f)
			if mode == store.LLMOnMachinePrefer {
				if out.Response != "done" || len(f.model.sent()) != 1 || out.Error != "" {
					t.Errorf("prefer: %+v", out)
				}
				return
			}
			if out.ErrorType != ErrTypeMachineRequired || out.Error != MachineRequiredMessage || len(f.model.sent()) != 0 || len(w.calls) != 0 {
				t.Errorf("require: %+v", out)
			}
		})
	}
}

// A machine lost in the middle of a turn is set aside: prefer runs the step
// and the rest of the turn on the server's key; require looks for another
// machine of the author's, excluding it, and stops when there is none.
func TestAgentWorkflow_MachineLostMidTurn(t *testing.T) {
	lost := temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_HEARTBEAT, nil)
	t.Run("prefer", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		w := &machineWorld{modes: map[string]string{"jarvis": store.LLMOnMachinePrefer}, free: []string{"m1"},
			answer: machineAnswers(fetchCall, lost)}
		w.register(env, fetchTools)
		f := registerLLM(env, answers(fetchCall, done))
		out := runTurn(t, env, f)
		if out.Response != "done" || !slices.Equal(w.keys(), []string{"m1/llm:0:1", "m1/llm:1:1"}) || len(f.model.sent()) != 2 ||
			!slices.Equal(w.aside, []string{"m1"}) {
			t.Errorf("answer %q, machine calls %v, server calls %d, aside %v", out.Response, w.keys(), len(f.model.sent()), w.aside)
		}
		if len(w.notes) != 3 || !strings.Contains(w.notes[1], "modèle de l'installation") || w.notes[2] != "" {
			t.Errorf("notes %q", w.notes)
		}
		// The steps on the server's key are not stamped with the machine.
		last := out.NewMessages[len(out.NewMessages)-1]
		if last.MachineID != "" || out.NewMessages[0].MachineID != "m1" {
			t.Errorf("stamps: first %q, last %q", out.NewMessages[0].MachineID, last.MachineID)
		}
	})
	t.Run("require", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		w := &machineWorld{modes: map[string]string{"jarvis": store.LLMOnMachineRequire}, free: []string{"m1", "m2"}}
		w.answer = func(n int, in activity.CallLLMOnMachineInput) (activity.LLMTurnResponse, error) {
			if in.MachineID == "m1" {
				return activity.LLMTurnResponse{}, temporal.NewNonRetryableApplicationError("not connected", machine.ErrTypeUnreachable, nil)
			}
			return activity.LLMTurnResponse{ChatResponse: done}, nil
		}
		w.register(env, fetchTools)
		f := registerLLM(env, answers(done))
		out := runTurn(t, env, f)
		if out.Response != "done" || !slices.Equal(w.keys(), []string{"m1/llm:0:1", "m2/llm:0:2"}) || len(f.model.sent()) != 0 ||
			fmt.Sprint(w.chooses) != "[[] [m1]]" {
			t.Errorf("answer %q, calls %v, chooses %v", out.Response, w.keys(), w.chooses)
		}
	})
	t.Run("require, no other", func(t *testing.T) {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		w := &machineWorld{modes: map[string]string{"jarvis": store.LLMOnMachineRequire}, free: []string{"m1"},
			answer: machineAnswers(temporal.NewNonRetryableApplicationError("full", machine.ErrTypeRefused, nil))}
		w.register(env, fetchTools)
		f := registerLLM(env, answers(done))
		out := runTurn(t, env, f)
		// Refused is no loss: not set aside.
		if out.ErrorType != ErrTypeMachineRequired || len(w.aside) != 0 || len(f.model.sent()) != 0 {
			t.Errorf("%+v, aside %v", out, w.aside)
		}
	})
}

// The provider's failures on a machine: a wait it asked for is waited, then
// the same machine again; a refusal for good or a conversation too long
// stops the turn as on the server's key; other failures are tried again,
// six attempts in all.
func TestAgentWorkflow_MachineProviderFailures(t *testing.T) {
	retry := temporal.NewApplicationErrorWithOptions("busy", machine.ErrTypeRetryAfter, temporal.ApplicationErrorOptions{
		NonRetryable: true, Details: []any{machine.LLMFailure{Type: machine.LLMFailRetryAfter, RetryAfterMS: 40_000}}})
	for name, c := range map[string]struct {
		answers  []any
		keys     []string
		errType  string
		response string
		minWait  time.Duration
	}{
		"retry after": {[]any{retry, done}, []string{"m1/llm:0:1", "m1/llm:0:2"}, "", "done", 40 * time.Second},
		"too long": {[]any{temporal.NewNonRetryableApplicationError("too long", machine.ErrTypeContextTooLong, nil)},
			[]string{"m1/llm:0:1"}, activity.ErrContextTooLong, "", 0},
		"permanent": {[]any{temporal.NewNonRetryableApplicationError("key refused", machine.ErrTypePermanentAPI, nil)},
			[]string{"m1/llm:0:1"}, machine.ErrTypePermanentAPI, "", 0},
		"passing, six times": {[]any{temporal.NewNonRetryableApplicationError("boom", machine.ErrTypeFailed, nil)},
			[]string{"m1/llm:0:1", "m1/llm:0:2", "m1/llm:0:3", "m1/llm:0:4", "m1/llm:0:5", "m1/llm:0:6"}, machine.ErrTypeFailed, "",
			(5 + 15 + 45 + 120 + 120) * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			w := &machineWorld{modes: map[string]string{"jarvis": store.LLMOnMachineRequire}, free: []string{"m1"}, answer: machineAnswers(c.answers...)}
			w.register(env, fetchTools)
			f := registerLLM(env, answers(done))
			start := env.Now()
			out := runTurn(t, env, f)
			if !slices.Equal(w.keys(), c.keys) || out.ErrorType != c.errType || out.Response != c.response || len(f.model.sent()) != 0 {
				t.Errorf("calls %v, out %+v", w.keys(), out)
			}
			if c.errType == activity.ErrContextTooLong && out.Error != activity.ContextTooLongMessage {
				t.Errorf("too long: %q", out.Error)
			}
			if waited := env.Now().Sub(start); waited < c.minWait {
				t.Errorf("waited %s, want %s at least", waited, c.minWait)
			}
		})
	}
}

// A stop during a wait between two attempts stops the turn: no further
// attempt, the transcript kept.
func TestAgentWorkflow_StopDuringAMachineWait(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	w := &machineWorld{modes: map[string]string{"jarvis": store.LLMOnMachinePrefer}, free: []string{"m1"},
		answer: machineAnswers(temporal.NewNonRetryableApplicationError("boom", machine.ErrTypeFailed, nil))}
	w.register(env, fetchTools)
	f := registerLLM(env, answers(done))
	env.RegisterDelayedCallback(env.CancelWorkflow, 2*time.Second)
	out := runTurn(t, env, f)
	if out.Response != "Agent cancelled." || len(w.calls) != 1 || len(f.model.sent()) != 0 {
		t.Errorf("out %+v, calls %v", out, w.keys())
	}
	if len(w.notes) == 0 || w.notes[len(w.notes)-1] != "" {
		t.Errorf("the note stays: %q", w.notes)
	}
}

// A sub-agent calls its model on its parent's machine, unless its own agent
// says never; with no machine from its parent, its own setting decides.
func TestAgentWorkflow_SubAgentFollowsTheMachine(t *testing.T) {
	tools := activity.ListToolsOutput{
		Tools:       []provider.ToolDefinition{{Name: "agent_analyst", InputSchema: json.RawMessage(activity.AgentToolSchema)}},
		Resolutions: map[string]activity.ToolResolution{"agent_analyst": {Kind: "workflow", AgentID: "analyst"}},
	}
	delegate := provider.ChatResponse{StopReason: "tool_use", ToolCalls: []provider.ToolCallInfo{
		{ID: "1", Name: "agent_analyst", Input: json.RawMessage(`{"task":"look"}`)}}}
	for name, c := range map[string]struct {
		parent, child string
		want          string // the child's LLMMachine
	}{
		"follows":                {store.LLMOnMachinePrefer, store.LLMOnMachinePrefer, "m1"},
		"never stays on the key": {store.LLMOnMachinePrefer, store.LLMOnMachineNever, "m1"},
		"parent on the key":      {store.LLMOnMachineNever, store.LLMOnMachinePrefer, ""},
	} {
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			w := &machineWorld{modes: map[string]string{"jarvis": c.parent, "analyst": c.child}, free: []string{"m1"},
				answer: machineAnswers(delegate, done)}
			w.register(env, tools)
			f := registerLLM(env, answers(delegate, done))
			var child AgentWorkflowInput
			var childCalls []string
			env.OnWorkflow(AgentWorkflow, mock.Anything, mock.Anything).Return(
				func(ctx workflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
					if in.AgentID == "jarvis" {
						return AgentWorkflow(ctx, in)
					}
					child = in
					before := len(w.keys())
					out, err := AgentWorkflow(ctx, in)
					childCalls = w.keys()[before:]
					return out, err
				})
			runTurn(t, env, f)
			got := ""
			if child.LLMMachine != nil {
				got = child.LLMMachine.ID
			}
			if got != c.want {
				t.Errorf("child's machine %q, want %q", got, c.want)
			}
			switch name {
			case "follows":
				if len(childCalls) != 1 || !strings.HasPrefix(childCalls[0], "m1/") {
					t.Errorf("child's calls %v", childCalls)
				}
			case "never stays on the key":
				if len(childCalls) != 0 {
					t.Errorf("a never sub-agent called the machine: %v", childCalls)
				}
			case "parent on the key":
				// Its own prefer: it chooses a machine itself.
				if len(childCalls) == 0 || !strings.HasPrefix(childCalls[0], "m1/") {
					t.Errorf("child's calls %v", childCalls)
				}
			}
		})
	}
}

func TestRetryWait(t *testing.T) {
	retry := func(ms int64) error {
		return temporal.NewApplicationErrorWithOptions("busy", machine.ErrTypeRetryAfter, temporal.ApplicationErrorOptions{
			Details: []any{machine.LLMFailure{Type: machine.LLMFailRetryAfter, RetryAfterMS: ms}}})
	}
	other := temporal.NewApplicationError("x", machine.ErrTypeFailed)
	for _, c := range []struct {
		err     error
		attempt int
		want    time.Duration
	}{
		{retry(30_000), 1, 30 * time.Second},
		{retry(600_000), 1, 2 * time.Minute},                                  // capped
		{retry(int64(2 * time.Hour / time.Millisecond)), 2, 15 * time.Second}, // absurd: the policy
		{other, 1, 5 * time.Second},
		{other, 3, 45 * time.Second},
		{other, 5, 2 * time.Minute},
	} {
		if got := retryWait(c.err, c.attempt); got != c.want {
			t.Errorf("%v, attempt %d: %s, want %s", c.err, c.attempt, got, c.want)
		}
	}
}
