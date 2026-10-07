package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/machine/connect"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// The model on the machines (docs/design/machine-llm.md, phase 3.0) against
// the real Temporal server and the throwaway database: real turns
// (AgentWorkflow, CallLLM, ChooseMachine, CallLLMOnMachine) on a worker of
// their own, real machines (agent connect's client and Modeler) whose
// provider is a stand-in, the gateway of newSmokeEnv. No paid call.

// llmModel is a stand-in provider: answer gets the n-th request (from 1).
type llmModel struct {
	mu     sync.Mutex
	n      int
	reqs   []provider.ChatRequest
	answer func(ctx context.Context, n int, req provider.ChatRequest) (provider.ChatResponse, error)
}

func (m *llmModel) Chat(ctx context.Context, req provider.ChatRequest) (provider.ChatResponse, error) {
	m.mu.Lock()
	m.n++
	n := m.n
	m.reqs = append(m.reqs, req)
	f := m.answer
	m.mu.Unlock()
	return f(ctx, n, req)
}

// set gives the model its answers, counting again from 1.
func (m *llmModel) set(f func(ctx context.Context, n int, req provider.ChatRequest) (provider.ChatResponse, error)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n, m.reqs, m.answer = 0, nil, f
}

func (m *llmModel) requests() []provider.ChatRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]provider.ChatRequest(nil), m.reqs...)
}

// say is a model answering text, always.
func say(text string) func(context.Context, int, provider.ChatRequest) (provider.ChatResponse, error) {
	return func(context.Context, int, provider.ChatRequest) (provider.ChatResponse, error) {
		return provider.ChatResponse{Content: text, StopReason: "end_turn", Usage: &provider.Usage{InputTokens: 10, OutputTokens: 2}}, nil
	}
}

// fetchThen answers the first request with a web_fetch call, then text.
func fetchThen(text string) func(context.Context, int, provider.ChatRequest) (provider.ChatResponse, error) {
	return func(ctx context.Context, n int, req provider.ChatRequest) (provider.ChatResponse, error) {
		if n == 1 {
			return provider.ChatResponse{StopReason: "tool_use", ToolCalls: []provider.ToolCallInfo{{ID: "fetch-1", Name: "web_fetch", Input: json.RawMessage(`{"url":"x"}`)}}}, nil
		}
		return say(text)(ctx, n, req)
	}
}

// llmPrompts names the agent in its prompt: a model tells a sub-agent's
// requests from its parent's.
type llmPrompts struct{}

func (llmPrompts) AgentPrompt(agentID string, _ []string) string { return "I am " + agentID + "." }

type llmNote struct{ session, text string }

// llmWorld is the worker of the turns: its queue, each agent's
// llm_on_machine, the server's model, and what the turns did.
type llmWorld struct {
	e      *smokeEnv
	queue  string
	server *llmModel
	notes  chan llmNote
	tools  chan activity.ExecuteToolInput

	mu    sync.Mutex
	modes map[string]string
}

func (w *llmWorld) setModes(modes map[string]string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.modes = modes
}

func (e *smokeEnv) startLLMWorker(t *testing.T) *llmWorld {
	w := &llmWorld{e: e, queue: "smoke-llm-" + smokeRandom(t), server: &llmModel{answer: say("from the server")},
		notes: make(chan llmNote, 256), tools: make(chan activity.ExecuteToolInput, 64), modes: map[string]string{}}
	schema := json.RawMessage(`{"type":"object"}`)
	catalog := activity.NewCatalog()
	catalog.SetTools([]store.ToolRecord{{Name: "web_fetch", Description: "fetch", InputSchema: schema}})
	wk := worker.New(e.tc, w.queue, worker.Options{})
	wk.RegisterWorkflow(workflow.AgentWorkflow)
	wk.RegisterActivity(&activity.LLMActivities{Provider: w.server, Store: e.st, Catalog: catalog, Prompts: llmPrompts{},
		Machines: e.st, Handoff: activity.NewHTTPDirectiveHandoff(e.base, smokeInternalKey)})
	wk.RegisterActivity(&activity.MachineActivities{Store: e.st, Routing: activity.CodingRouting{Machines: true}})
	wk.RegisterActivity(&activity.MemoryActivities{Store: e.st})
	wk.RegisterActivityWithOptions(func(context.Context, activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{
			Tools: []provider.ToolDefinition{{Name: "web_fetch", InputSchema: schema}, {Name: "agent_helper", InputSchema: json.RawMessage(activity.AgentToolSchema)}},
			Resolutions: map[string]activity.ToolResolution{
				"web_fetch":    {Kind: "activity", TaskQueue: w.queue, NeedsCallContext: true},
				"agent_helper": {Kind: "workflow", AgentID: "helper"},
			},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	wk.RegisterActivityWithOptions(func(_ context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		w.mu.Lock()
		defer w.mu.Unlock()
		return activity.LoadSkillsForAgentOutput{Name: in.AgentID, LLMOnMachine: w.modes[in.AgentID]}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	wk.RegisterActivityWithOptions(func(_ context.Context, in activity.NotifyInput) error {
		if in.Event.Type == activity.EventNotice {
			var n struct{ Text string }
			json.Unmarshal(in.Event.Data, &n)
			select {
			case w.notes <- llmNote{in.SessionID, n.Text}:
			default:
			}
		}
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	wk.RegisterActivityWithOptions(func(_ context.Context, in activity.ExecuteToolInput) (activity.ExecuteToolOutput, error) {
		select {
		case w.tools <- in:
		default:
		}
		return activity.ExecuteToolOutput{Content: "the page"}, nil
	}, sdkactivity.RegisterOptions{Name: "ExecuteTool"})
	if err := wk.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wk.Stop)
	return w
}

// turn starts the turn of agent on a message of userID's, text, in a
// session of their own: the participant's workflow ID and turn key.
func (w *llmWorld) turn(t *testing.T, userID, agent, text string) (string, client.WorkflowRun) {
	t.Helper()
	ctx := context.Background()
	session := w.e.session(t, userID)
	content, _ := json.Marshal(text)
	id, err := w.e.st.AppendMessage(ctx, session, store.HumanMessageKey(uuid.NewString()),
		store.Message{Role: store.RoleUser, Content: string(content), UserID: userID, Author: "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	wfID := fmt.Sprintf("%s:p:%s:m%d", session, agent, id)
	run, err := w.e.tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: wfID, TaskQueue: w.queue}, workflow.AgentWorkflow,
		workflow.AgentWorkflowInput{SessionID: session, UserID: userID, UserName: "Alice", AgentID: agent, TurnKey: store.TurnKey(id, agent)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.e.tc.TerminateWorkflow(context.Background(), run.GetID(), "", "smoke test over") })
	return session, run
}

func turnResult(t *testing.T, run client.WorkflowRun, within time.Duration) workflow.AgentWorkflowOutput {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	var out workflow.AgentWorkflowOutput
	if err := run.Get(ctx, &out); err != nil {
		t.Fatalf("turn: %v", err)
	}
	return out
}

// llmMachine is a machine offering its model (a stand-in), and echoes.
type llmMachine struct {
	id      string
	model   *llmModel
	modeler *connect.Modeler
	client  *connect.Client
	stop    context.CancelFunc
	runErr  chan error
}

// startLLMMachine enrolls and starts a machine of userID's with a model,
// reaching the gateway at server ("" = e.base); wrap, when given, wraps its
// llm executor.
func (e *smokeEnv) startLLMMachine(t *testing.T, userID, name, server string, priority int, wrap func(connect.Executor) connect.Executor) *llmMachine {
	t.Helper()
	return e.startLLMMachineWith(t, userID, name, server, priority, 4, wrap)
}

// startLLMMachineWith is startLLMMachine, with maxLLM calls at once.
func (e *smokeEnv) startLLMMachineWith(t *testing.T, userID, name, server string, priority, maxLLM int, wrap func(connect.Executor) connect.Executor) *llmMachine {
	t.Helper()
	id, token := e.enrollToken(userID, name, []string{machine.CapLLM, machine.KindEcho}, 1)
	if server == "" {
		server = e.base
	}
	dir := t.TempDir()
	if err := (connect.State{Dir: dir}).Save(connect.Config{Server: server, MachineID: id, Name: name, Token: token}); err != nil {
		t.Fatal(err)
	}
	if priority != 0 {
		if err := e.st.SetMachinePriority(context.Background(), userID, id, priority); err != nil {
			t.Fatal(err)
		}
	}
	lm := &llmMachine{id: id, model: &llmModel{answer: say("from " + name)}, runErr: make(chan error, 1)}
	lm.modeler = &connect.Modeler{Provider: lm.model, ProviderName: "fake", Model: "machine-model"}
	exec := connect.Executor(lm.modeler.Call)
	if wrap != nil {
		exec = wrap(exec)
	}
	lm.client = &connect.Client{State: connect.State{Dir: dir},
		Executors: map[string]connect.Executor{machine.KindEcho: connect.Echo, machine.KindLLM: exec},
		Status: func() connect.Status {
			s := connect.Status{Capabilities: []string{machine.KindEcho}, LLM: lm.modeler.State()}
			if lm.modeler.Offered() {
				s.Capabilities = append(s.Capabilities, machine.CapLLM)
			}
			return s
		},
		StatusEvery: 200 * time.Millisecond, MaxDirectives: 1, MaxLLM: maxLLM, LLMProvider: "fake", LLMModel: "machine-model",
		OS: "linux", Version: "smoke", MinBackoff: 100 * time.Millisecond, MaxBackoff: 500 * time.Millisecond, StopWait: time.Second}
	lm.modeler.OnRefused = lm.client.Refresh
	ctx, cancel := context.WithCancel(context.Background())
	lm.stop = cancel
	go func() { lm.runErr <- lm.client.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-lm.runErr:
		case <-time.After(10 * time.Second):
		}
	})
	waitFor(t, "machine "+name+" online", 10*time.Second, func() bool { return e.gateway().Online(id) })
	return lm
}

// llmDirective is a call to the model as the database keeps it.
type llmDirective struct{ machine, key, state, errText string }

// llmDirectives are the calls to the model of the workflows whose ID starts
// with prefix, in order.
func (e *smokeEnv) llmDirectives(t *testing.T, prefix string) []llmDirective {
	t.Helper()
	rows, err := e.db.Query(`SELECT machine_id, call_key, state, error FROM machine_directives
		WHERE kind = 'llm' AND workflow_id LIKE $1 || '%' ORDER BY created_at, call_key`, prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []llmDirective
	for rows.Next() {
		var d llmDirective
		rows.Scan(&d.machine, &d.key, &d.state, &d.errText)
		out = append(out, d)
	}
	return out
}

func (e *smokeEnv) asideForLLM(id string) bool {
	m, _ := e.st.GetMachine(context.Background(), id)
	return m != nil && m.AsideForLLM(time.Now().Add(-activity.DefaultMachineOnlineWindow))
}

// blockUntilStopped is a model that answers nothing until its call is
// cancelled, saying on blocked that it waits.
func blockUntilStopped(blocked chan<- struct{}, ended chan<- error) func(context.Context, int, provider.ChatRequest) (provider.ChatResponse, error) {
	return func(ctx context.Context, _ int, _ provider.ChatRequest) (provider.ChatResponse, error) {
		blocked <- struct{}{}
		<-ctx.Done()
		ended <- context.Cause(ctx)
		return provider.ChatResponse{}, ctx.Err()
	}
}

func TestMachinesLLM_RealServer(t *testing.T) {
	e := newSmokeEnv(t)
	w := e.startLLMWorker(t)
	ctx := context.Background()
	prefer := map[string]string{"jarvis": store.LLMOnMachinePrefer}
	require := map[string]string{"jarvis": store.LLMOnMachineRequire}

	// --- A turn on the machine: every step there, the prompt's memory back
	// with each answer, answers stamped, the turn's line said and cleared.
	t.Run("routed", func(t *testing.T) {
		w.setModes(prefer)
		alice := e.user("llm-alice")
		if _, err := e.st.SaveMemory(ctx, store.MemoryScopeUser, alice, "likes tea", 0); err != nil {
			t.Fatal(err)
		}
		m := e.startLLMMachine(t, alice, "portable", "", 0, nil)
		m.model.set(fetchThen("done on the machine"))
		session, run := w.turn(t, alice, "jarvis", "hello")
		out := turnResult(t, run, time.Minute)
		if out.Response != "done on the machine" || out.Error != "" || len(w.server.requests()) != 0 {
			t.Fatalf("out %+v, server calls %d", out, len(w.server.requests()))
		}
		reqs := m.model.requests()
		if len(reqs) != 2 || reqs[0].Model != "machine-model" || !strings.Contains(reqs[0].System, "likes tea") ||
			!strings.Contains(reqs[0].System, "I am jarvis") {
			t.Errorf("the machine's requests: %d, first %+v", len(reqs), reqs[0])
		}
		var tool activity.ExecuteToolInput
		select {
		case tool = <-w.tools:
		case <-time.After(5 * time.Second):
			t.Fatal("the tool never ran")
		}
		if tool.Call == nil || tool.Call.MemoryVersion == nil || *tool.Call.MemoryVersion != 1 {
			t.Errorf("the prompt's memory did not come back with the answer: %+v", tool.Call)
		}
		ds := e.llmDirectives(t, run.GetID())
		if len(ds) != 2 || ds[0].key != "llm:0:1" || ds[1].key != "llm:1:1" || ds[0].machine != m.id || ds[1].machine != m.id ||
			ds[0].state != store.DirectiveCompleted || ds[1].state != store.DirectiveCompleted {
			t.Errorf("directives %+v", ds)
		}
		for _, msg := range out.NewMessages {
			if msg.Role == store.RoleAssistant && (msg.MachineID != m.id || msg.Machine != "portable" || msg.Model != "machine-model" ||
				msg.Usage == nil && msg.Content != "") {
				t.Errorf("answer not stamped: %+v", msg)
			}
		}
		// Written so in the session.
		var on, off int
		rows, _ := e.db.Query(`SELECT data->>'machine_id' FROM messages WHERE session_id = $1 AND data->>'role' = 'assistant'`, session)
		for rows.Next() {
			var id *string
			rows.Scan(&id)
			if id != nil && *id == m.id {
				on++
			} else {
				off++
			}
		}
		rows.Close()
		if on != 2 || off != 0 {
			t.Errorf("stored answers on the machine %d, else %d", on, off)
		}
		var notes []string
		for len(notes) < 2 {
			select {
			case n := <-w.notes:
				if n.session == session {
					notes = append(notes, n.text)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("notes %q", notes)
			}
		}
		if notes[0] != "modèle sur la machine « portable » (machine-model)" || notes[1] != "" {
			t.Errorf("notes %q", notes)
		}
	})

	// --- The prompt's memory comes back when the sweep completes the call
	// (the gateway's completions failing: Temporal out of reach).
	t.Run("memory through the sweep", func(t *testing.T) {
		w.setModes(prefer)
		bob := e.user("llm-bob")
		if _, err := e.st.SaveMemory(ctx, store.MemoryScopeUser, bob, "likes coffee", 0); err != nil {
			t.Fatal(err)
		}
		m := e.startLLMMachine(t, bob, "maison", "", 0, nil)
		m.model.set(fetchThen("done"))
		for len(w.tools) > 0 {
			<-w.tools
		}
		e.flaky.failures.Store(completeTries)
		_, run := w.turn(t, bob, "jarvis", "hello")
		out := turnResult(t, run, time.Minute)
		if out.Response != "done" {
			t.Fatalf("out %+v", out)
		}
		tool := <-w.tools
		if tool.Call == nil || tool.Call.MemoryVersion == nil || *tool.Call.MemoryVersion != 1 {
			t.Errorf("memory after the sweep: %+v", tool.Call)
		}
		if e.flaky.failures.Load() > 0 {
			t.Errorf("the completions did not fail")
		}
	})

	// --- No machine: prefer takes the server's key, require stops.
	t.Run("no machine", func(t *testing.T) {
		carol := e.user("llm-carol")
		w.setModes(prefer)
		w.server.set(say("from the server"))
		_, run := w.turn(t, carol, "jarvis", "hello")
		if out := turnResult(t, run, time.Minute); out.Response != "from the server" || len(w.server.requests()) != 1 {
			t.Errorf("prefer: %+v", out)
		}
		w.setModes(require)
		_, run = w.turn(t, carol, "jarvis", "hello")
		if out := turnResult(t, run, time.Minute); out.ErrorType != workflow.ErrTypeMachineRequired || out.Error != workflow.MachineRequiredMessage ||
			len(w.server.requests()) != 1 {
			t.Errorf("require: %+v", out)
		}
	})

	// --- A machine lost in the middle of a turn (agent connect stops during
	// a call): set aside; prefer finishes on the server's key, require on
	// another machine of the author's.
	t.Run("machine lost", func(t *testing.T) {
		dave := e.user("llm-dave")
		w.setModes(prefer)
		m := e.startLLMMachine(t, dave, "fragile", "", 0, nil)
		blocked, ended := make(chan struct{}, 4), make(chan error, 4)
		m.model.set(func(ctx context.Context, n int, req provider.ChatRequest) (provider.ChatResponse, error) {
			if n == 1 {
				return fetchThen("")(ctx, n, req)
			}
			return blockUntilStopped(blocked, ended)(ctx, n, req)
		})
		w.server.set(say("finished by the server"))
		_, run := w.turn(t, dave, "jarvis", "hello")
		<-blocked
		m.stop()
		out := turnResult(t, run, time.Minute)
		if out.Response != "finished by the server" || !e.asideForLLM(m.id) {
			t.Errorf("prefer: %+v, aside %v", out, e.asideForLLM(m.id))
		}
		if ds := e.llmDirectives(t, run.GetID()); len(ds) != 2 || ds[1].state != store.DirectiveStopping {
			t.Errorf("directives %+v", ds)
		}

		erin := e.user("llm-erin")
		w.setModes(require)
		first := e.startLLMMachine(t, erin, "first", "", 5, nil)
		second := e.startLLMMachine(t, erin, "second", "", 0, nil)
		first.model.set(blockUntilStopped(blocked, ended))
		second.model.set(say("done on the second"))
		_, run = w.turn(t, erin, "jarvis", "hello")
		<-blocked
		first.stop()
		out = turnResult(t, run, time.Minute)
		ds := e.llmDirectives(t, run.GetID())
		if out.Response != "done on the second" || len(ds) != 2 || ds[0].machine != first.id || ds[1].machine != second.id ||
			ds[1].key != "llm:0:2" {
			t.Errorf("require: %+v, directives %+v", out, ds)
		}
	})

	// --- The provider's failures on the machine.
	t.Run("provider failures", func(t *testing.T) {
		frank := e.user("llm-frank")
		w.setModes(require)
		m := e.startLLMMachine(t, frank, "busy", "", 0, nil)

		// Retry-After: waited, then the same machine again.
		m.model.set(func(ctx context.Context, n int, req provider.ChatRequest) (provider.ChatResponse, error) {
			if n == 1 {
				return provider.ChatResponse{}, &provider.RetryAfterError{Err: errors.New("overloaded (529)"), Delay: 2 * time.Second}
			}
			return say("after the wait")(ctx, n, req)
		})
		start := time.Now()
		_, run := w.turn(t, frank, "jarvis", "hello")
		out := turnResult(t, run, time.Minute)
		ds := e.llmDirectives(t, run.GetID())
		if out.Response != "after the wait" || len(ds) != 2 || ds[0].state != store.DirectiveFailed || ds[1].key != "llm:0:2" ||
			ds[1].machine != m.id || time.Since(start) < 2*time.Second {
			t.Errorf("retry after: %+v, %+v, %s", out, ds, time.Since(start))
		}

		// Too long for the machine's model: the fork advice, never retried.
		m.model.set(func(context.Context, int, provider.ChatRequest) (provider.ChatResponse, error) {
			return provider.ChatResponse{}, &provider.PermanentAPIError{Err: fmt.Errorf("%w: prompt is too long", provider.ErrContextTooLong)}
		})
		_, run = w.turn(t, frank, "jarvis", "hello")
		out = turnResult(t, run, time.Minute)
		if out.ErrorType != activity.ErrContextTooLong || out.Error != activity.ContextTooLongMessage || len(e.llmDirectives(t, run.GetID())) != 1 {
			t.Errorf("too long: %+v", out)
		}

		// The key refused: the turn stops, the machine withdraws its model.
		m.model.set(func(context.Context, int, provider.ChatRequest) (provider.ChatResponse, error) {
			return provider.ChatResponse{}, &provider.PermanentAPIError{Err: errors.New("invalid x-api-key (401)"), Credentials: true}
		})
		_, run = w.turn(t, frank, "jarvis", "hello")
		out = turnResult(t, run, time.Minute)
		if out.ErrorType != machine.ErrTypePermanentAPI || len(e.llmDirectives(t, run.GetID())) != 1 {
			t.Errorf("refused key: %+v", out)
		}
		waitFor(t, "the model withdrawn", 10*time.Second, func() bool {
			got, _ := e.st.GetMachine(ctx, m.id)
			return got.LLMState == machine.LLMStateRefused && !got.Can(machine.CapLLM)
		})
		if _, err := e.st.ChooseLLMMachine(ctx, frank, nil, time.Now().Add(-time.Minute)); !errors.Is(err, store.ErrNoMachine) {
			t.Errorf("a machine whose model is withdrawn chosen: %v", err)
		}
	})

	// --- A machine the database says online, that no gateway holds: the
	// call is unreachable, the machine set aside, prefer goes on with the
	// server's key.
	t.Run("unreachable", func(t *testing.T) {
		gina := e.user("llm-gina")
		w.setModes(prefer)
		id, _ := e.enrollToken(gina, "ghost", []string{machine.CapLLM}, 1)
		if _, err := e.db.Exec(`UPDATE machines SET connected_to = 'gw-elsewhere', seen_at = NOW(), connected_at = NOW() - interval '1 minute',
			capabilities = '["llm"]', max_llm = 4, llm_state = 'ok' WHERE id = $1`, id); err != nil {
			t.Fatal(err)
		}
		w.server.set(say("from the server"))
		_, run := w.turn(t, gina, "jarvis", "hello")
		out := turnResult(t, run, time.Minute)
		ds := e.llmDirectives(t, run.GetID())
		if out.Response != "from the server" || len(ds) != 1 || ds[0].state != store.DirectiveFailed ||
			!strings.Contains(ds[0].errText, "unreachable") || !e.asideForLLM(id) {
			t.Errorf("%+v, %+v, aside %v", out, ds, e.asideForLLM(id))
		}
	})

	// --- What a machine answers is checked: too large, or malformed, it is
	// refused and written as a failure; the step is tried again.
	t.Run("answer refused", func(t *testing.T) {
		hugo := e.user("llm-hugo")
		w.setModes(require)
		var calls sync.Map
		m := e.startLLMMachine(t, hugo, "liar", "", 0, func(next connect.Executor) connect.Executor {
			return func(ctx context.Context, in json.RawMessage, p func(string)) (json.RawMessage, error) {
				n, _ := calls.LoadOrStore("n", new(int))
				count := n.(*int)
				*count++
				switch *count {
				case 1: // over 1.5 MiB
					return json.Marshal(provider.ChatResponse{Content: strings.Repeat("x", machine.MaxLLMOutputBytes), StopReason: "end_turn"})
				case 2: // a stop reason no API gives, and a tool call without ID
					return json.RawMessage(`{"stop_reason":"whatever","tool_calls":[{"name":"web_fetch","input":{}}]}`), nil
				}
				return next(ctx, in, p)
			}
		})
		m.model.set(say("an honest answer"))
		_, run := w.turn(t, hugo, "jarvis", "hello")
		out := turnResult(t, run, 2*time.Minute)
		ds := e.llmDirectives(t, run.GetID())
		if out.Response != "an honest answer" || len(ds) != 3 || ds[0].state != store.DirectiveFailed || ds[1].state != store.DirectiveFailed ||
			!strings.Contains(ds[0].errText, "refused") || !strings.Contains(ds[1].errText, "refused") || ds[2].state != store.DirectiveCompleted {
			t.Errorf("%+v, %+v", out, ds)
		}
	})

	// --- Stop during a call: the machine drops it; during a wait between two
	// attempts: no other attempt.
	t.Run("stop", func(t *testing.T) {
		ines := e.user("llm-ines")
		w.setModes(require)
		m := e.startLLMMachine(t, ines, "stoppable", "", 0, nil)
		blocked, ended := make(chan struct{}, 1), make(chan error, 1)
		m.model.set(blockUntilStopped(blocked, ended))
		_, run := w.turn(t, ines, "jarvis", "hello")
		<-blocked
		if err := e.tc.CancelWorkflow(ctx, run.GetID(), ""); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ended:
		case <-time.After(20 * time.Second):
			t.Fatal("the machine's call was never stopped")
		}
		out := turnResult(t, run, time.Minute)
		ds := e.llmDirectives(t, run.GetID())
		if out.Response != "Agent cancelled." || len(ds) != 1 || ds[0].state != store.DirectiveCanceled {
			t.Errorf("during a call: %+v, %+v", out, ds)
		}

		m.model.set(func(context.Context, int, provider.ChatRequest) (provider.ChatResponse, error) {
			return provider.ChatResponse{}, &provider.RetryAfterError{Err: errors.New("busy"), Delay: 90 * time.Second}
		})
		_, run = w.turn(t, ines, "jarvis", "hello")
		waitFor(t, "the first attempt over", 20*time.Second, func() bool {
			ds := e.llmDirectives(t, run.GetID())
			return len(ds) == 1 && ds[0].state != store.DirectiveRunning
		})
		start := time.Now()
		if err := e.tc.CancelWorkflow(ctx, run.GetID(), ""); err != nil {
			t.Fatal(err)
		}
		out = turnResult(t, run, time.Minute)
		if out.Response != "Agent cancelled." || len(e.llmDirectives(t, run.GetID())) != 1 || time.Since(start) > 30*time.Second {
			t.Errorf("during a wait: %+v after %s", out, time.Since(start))
		}
	})

	// --- A sub-agent follows its parent's machine, unless its agent says
	// never: then the server's key.
	t.Run("sub-agents", func(t *testing.T) {
		jules := e.user("llm-jules")
		m := e.startLLMMachine(t, jules, "partagee", "", 0, nil)
		answer := func(ctx context.Context, n int, req provider.ChatRequest) (provider.ChatResponse, error) {
			if strings.Contains(req.System, "I am helper") {
				return say("the helper's part")(ctx, n, req)
			}
			for _, msg := range req.Messages {
				if msg.ToolResult != nil {
					return say("done with help")(ctx, n, req)
				}
			}
			return provider.ChatResponse{StopReason: "tool_use", ToolCalls: []provider.ToolCallInfo{
				{ID: "help-1", Name: "agent_helper", Input: json.RawMessage(`{"task":"look"}`)}}}, nil
		}
		m.model.set(answer)
		w.server.set(answer)
		for _, helper := range []string{store.LLMOnMachinePrefer, store.LLMOnMachineNever} {
			w.setModes(map[string]string{"jarvis": store.LLMOnMachinePrefer, "helper": helper})
			before := len(w.server.requests())
			_, run := w.turn(t, jules, "jarvis", "hello")
			out := turnResult(t, run, time.Minute)
			child := e.llmDirectives(t, run.GetID()+":tool:agent_helper:")
			onServer := len(w.server.requests()) - before
			switch helper {
			case store.LLMOnMachinePrefer:
				if out.Response != "done with help" || len(child) != 1 || child[0].machine != m.id || onServer != 0 {
					t.Errorf("follows: %+v, child %+v, server %d", out, child, onServer)
				}
			default:
				if out.Response != "done with help" || len(child) != 0 || onServer != 1 {
					t.Errorf("never: %+v, child %+v, server %d", out, child, onServer)
				}
			}
		}
	})

	// --- A call to the model is served while the machine runs its one
	// directive (an echo): each family has its own cap.
	t.Run("caps per family", func(t *testing.T) {
		kim := e.user("llm-kim")
		w.setModes(prefer)
		m := e.startLLMMachine(t, kim, "occupee", "", 0, nil)
		m.model.set(say("served meanwhile"))
		echo := e.echo(kim, "long", 8*time.Second, 0, 0)
		waitFor(t, "the echo running", 10*time.Second, func() bool {
			var n int
			e.db.QueryRow(`SELECT COUNT(*) FROM machine_directives WHERE machine_id = $1 AND kind = 'echo' AND state = 'running' AND sent_conn <> ''`, m.id).Scan(&n)
			return n == 1
		})
		_, run := w.turn(t, kim, "jarvis", "hello")
		out := turnResult(t, run, time.Minute)
		var running int
		e.db.QueryRow(`SELECT COUNT(*) FROM machine_directives WHERE machine_id = $1 AND kind = 'echo' AND state = 'running'`, m.id).Scan(&running)
		if out.Response != "served meanwhile" || running != 1 {
			t.Errorf("%+v, echo running %d", out, running)
		}
		if _, err := result(t, echo, 30*time.Second); err != nil {
			t.Errorf("echo: %v", err)
		}
	})

	// --- A request of 1.5 MB, compressed, to a machine behind a slow link:
	// written in chunks, the pings in between, no disconnection.
	// --- Busy is passing: a machine at its cap of calls makes the next one
	// wait, then take it; the machine is not excluded.
	t.Run("busy", func(t *testing.T) {
		mia := e.user("llm-mia")
		w.setModes(require)
		m := e.startLLMMachineWith(t, mia, "etroite", "", 0, 1, nil)
		blocked, release := make(chan struct{}, 1), make(chan struct{})
		m.model.set(func(ctx context.Context, n int, req provider.ChatRequest) (provider.ChatResponse, error) {
			if n == 1 {
				blocked <- struct{}{}
				<-release
				return say("first")(ctx, n, req)
			}
			return say("second")(ctx, n, req)
		})
		_, first := w.turn(t, mia, "jarvis", "one")
		<-blocked
		_, second := w.turn(t, mia, "jarvis", "two")
		// Its first attempt finds the machine full (no directive), and waits.
		time.Sleep(3 * time.Second)
		if ds := e.llmDirectives(t, second.GetID()); len(ds) != 0 {
			t.Errorf("a call created on a full machine: %+v", ds)
		}
		close(release)
		if out := turnResult(t, first, time.Minute); out.Response != "first" {
			t.Errorf("first: %+v", out)
		}
		out := turnResult(t, second, time.Minute)
		ds := e.llmDirectives(t, second.GetID())
		if out.Response != "second" || len(ds) != 1 || ds[0].machine != m.id || ds[0].key == "llm:0:1" || e.asideForLLM(m.id) {
			t.Errorf("second: %+v, %+v", out, ds)
		}
	})

	// --- A call carried across the gateway's restart: running, or finished
	// while no gateway listened, its answer is given at the reconnection,
	// never asked again (paid once).
	t.Run("across a gateway restart", func(t *testing.T) {
		nina := e.user("llm-nina")
		w.setModes(require)
		m := e.startLLMMachine(t, nina, "fidele", "", 0, nil)
		for _, finishedFirst := range []bool{false, true} {
			blocked, release := make(chan struct{}, 1), make(chan struct{})
			m.model.set(func(ctx context.Context, n int, req provider.ChatRequest) (provider.ChatResponse, error) {
				blocked <- struct{}{}
				<-release
				return say("kept")(ctx, n, req)
			})
			_, run := w.turn(t, nina, "jarvis", "hello")
			<-blocked
			e.stopGateway()
			if finishedFirst {
				close(release)
				time.Sleep(500 * time.Millisecond) // the machine keeps its result, no gateway to take it
			}
			e.restartGateway()
			waitFor(t, "the machine back", 20*time.Second, func() bool { return e.gateway().Online(m.id) })
			if !finishedFirst {
				close(release)
			}
			out := turnResult(t, run, time.Minute)
			ds := e.llmDirectives(t, run.GetID())
			if out.Response != "kept" || len(m.model.requests()) != 1 || len(ds) != 1 || ds[0].state != store.DirectiveCompleted {
				t.Errorf("finished first %v: %+v, calls %d, %+v", finishedFirst, out, len(m.model.requests()), ds)
			}
		}
	})

	t.Run("large request, slow link", func(t *testing.T) {
		lena := e.user("llm-lena")
		w.setModes(prefer)
		// A ping timeout shorter than the request takes, and small send
		// buffers: the pings go out between its frames, or the connection
		// is cut.
		e.pingTimeout, e.writeBuffer = 5*time.Second, 64<<10
		e.stopGateway()
		e.restartGateway()
		defer func() {
			e.pingTimeout, e.writeBuffer = 0, 0
			e.stopGateway()
			e.restartGateway()
		}()
		proxy := slowProxy(t, e.addr, 100<<10)
		m := e.startLLMMachine(t, lena, "lente", "http://"+proxy, 0, nil)
		m.model.set(say("read it all"))
		before := e.gateway().conn(m.id)
		big := make([]byte, 750<<10)
		rand.Read(big)
		start := time.Now()
		_, run := w.turn(t, lena, "jarvis", hex.EncodeToString(big))
		out := turnResult(t, run, 2*time.Minute)
		reqs := m.model.requests()
		if out.Response != "read it all" || len(reqs) != 1 || len(reqs[0].Messages) != 1 || len(reqs[0].Messages[0].Content) < 1500<<10 {
			t.Fatalf("%+v, requests %d", out, len(reqs))
		}
		if after := e.gateway().conn(m.id); after != before || before == nil {
			t.Errorf("the machine reconnected during the request")
		}
		took := time.Since(start)
		if took < 6*time.Second {
			t.Errorf("the request took %s: not longer than the 5 s ping timeout, the test proves nothing", took)
		}
		t.Logf("1.5 MB request through a 100 KiB/s link, pings every second within 5 s: %s", took.Round(time.Millisecond))
	})
}

// slowProxy forwards to addr, the way down (to the machine) at rate bytes a
// second, in small reads; the way up at full speed. Its address.
func slowProxy(t *testing.T, addr string, rate int) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			down, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", addr)
			if err != nil {
				down.Close()
				continue
			}
			// Small buffers: what waits on the way down is what this link
			// holds, not megabytes the kernel takes at once on loopback.
			up.(*net.TCPConn).SetReadBuffer(64 << 10)
			down.(*net.TCPConn).SetWriteBuffer(64 << 10)
			go func() { io.Copy(up, down); up.Close() }()
			go func() {
				defer down.Close()
				const chunk = 4 << 10
				buf := make([]byte, chunk)
				for {
					n, err := up.Read(buf)
					if n > 0 {
						if _, werr := down.Write(buf[:n]); werr != nil {
							return
						}
						time.Sleep(time.Duration(n) * time.Second / time.Duration(rate))
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String()
}
