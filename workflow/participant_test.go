package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
)

// logs records what the workflows log at warn and error.
type logs struct {
	mu      sync.Mutex
	entries []string
}

func (l *logs) record(level, msg string, kv []interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, fmt.Sprint(level, " ", msg, " ", kv))
}
func (l *logs) Debug(string, ...interface{})        {}
func (l *logs) Info(string, ...interface{})         {}
func (l *logs) Warn(msg string, kv ...interface{})  { l.record("WARN", msg, kv) }
func (l *logs) Error(msg string, kv ...interface{}) { l.record("ERROR", msg, kv) }
func (l *logs) contain(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.ContainsFunc(l.entries, func(e string) bool { return strings.Contains(e, s) })
}

// harness runs participants over a store in memory: the real turn and
// relay bookkeeping, a model, or a stubbed turn, and records what the
// channels and the relay were told.
type harness struct {
	t   *testing.T
	env *testsuite.TestWorkflowEnvironment
	f   *llmFakes
	log *logs

	mu       sync.Mutex
	notified []activity.NotifyInput
	relays   []activity.RelayInput
	turns    []childRun // the turns run, when stubbed
	// relayErr fails every relay.
	relayErr error
}

// newHarness builds a harness. With agent nil, turns run the real
// AgentWorkflow, the model answering with answer (each agent's prompt is
// "I am <agent>"); otherwise agent is the turn.
func newHarness(t *testing.T, answer func(int, provider.ChatRequest) (provider.ChatResponse, error), agent func(sdkworkflow.Context, AgentWorkflowInput) (AgentWorkflowOutput, error)) *harness {
	t.Helper()
	h := &harness{t: t, log: &logs{}}
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(h.log)
	h.env = suite.NewTestWorkflowEnvironment()
	if answer == nil {
		answer = answers(done)
	}
	h.f = registerLLM(h.env, answer)
	h.f.session.agents = map[string]string{"jarvis": "Jarvis", "smith": "Agent Smith"}
	h.f.llm.Prompts = promptFunc(func(agentID string, _ []string) string { return "I am " + agentID })
	h.f.catalog.SetAgents([]activity.AgentCatalogEntry{{ID: "jarvis", Name: "Jarvis", Mention: "jarvis"}, {ID: "smith", Name: "Agent Smith", Mention: "smith"}})
	if agent == nil {
		h.env.RegisterWorkflow(AgentWorkflow)
	} else {
		h.env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
			h.mu.Lock()
			h.turns = append(h.turns, childRun{workflowID: sdkworkflow.GetInfo(ctx).WorkflowExecution.ID, in: in})
			h.mu.Unlock()
			return agent(ctx, in)
		}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})
	}
	h.env.RegisterWorkflow(ParticipantWorkflow)
	turnAct := &activity.TurnActivities{Store: h.f.session}
	h.env.RegisterActivityWithOptions(turnAct.CheckTurn, sdkactivity.RegisterOptions{Name: "CheckTurn"})
	h.env.RegisterActivityWithOptions(turnAct.EndTurn, sdkactivity.RegisterOptions{Name: "EndTurn"})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in activity.RelayInput) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.relays = append(h.relays, in)
		return h.relayErr
	}, sdkactivity.RegisterOptions{Name: "Relay"})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in activity.NotifyInput) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.notified = append(h.notified, in)
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{Name: h.f.session.agents[in.AgentID]}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{
			Tools:       []provider.ToolDefinition{{Name: "web_fetch", InputSchema: json.RawMessage(`{"type":"object"}`)}},
			Resolutions: map[string]activity.ToolResolution{"web_fetch": {Kind: "activity", TaskQueue: "tools-web"}},
		}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	executePage(h.env)
	return h
}

// human stores a person's message, as the server does before delivering it,
// and returns it as delivered.
func (h *harness) human(text, user string) ParticipantMessage {
	id := h.f.session.add(store.HumanMessageKey(fmt.Sprint(len(h.f.session.history())+1)), store.Message{Role: store.RoleUser, Content: `"` + text + `"`, UserID: "u-" + user, Author: user})
	return ParticipantMessage{MessageID: id, UserID: "u-" + user, UserName: user}
}

// run runs agentID's participant on the messages of its inbox, and the
// signals sent meanwhile; it reports the workflow's error.
func (h *harness) run(agentID string, inbox ...ParticipantMessage) error {
	h.t.Helper()
	h.env.ExecuteWorkflow(ParticipantWorkflow, ParticipantInput{SessionID: "s1", AgentID: agentID, Channel: "telegram", ChannelID: "42", Inbox: inbox})
	if !h.env.IsWorkflowCompleted() {
		h.t.Fatal("participant did not complete")
	}
	if h.log.contain("unhandled signals") {
		h.t.Errorf("a participant ended with unhandled signals: %v", h.log.entries)
	}
	return h.env.GetWorkflowError()
}

// events lists the turn events and channel messages, in order.
func (h *harness) events() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, n := range h.notified {
		var e map[string]string
		json.Unmarshal(n.Event.Data, &e)
		switch n.Event.Type {
		case EventTurnStarted, EventTurnDone:
			out = append(out, n.Event.Type+" "+e["turn"])
		case activity.EventMessage:
			out = append(out, "message "+e["content"])
		}
	}
	return out
}

// ends lists each turn's end in the store: its key, and its error.
func (h *harness) ends() map[string]string {
	ends := map[string]string{}
	for _, m := range h.f.session.history() {
		if store.IsTurnEnd(m.Key) {
			turn, _ := store.TurnOf(m.Key)
			ends[turn] = store.TurnEndError(m.Message)
		}
	}
	return ends
}

// read is what a request's model read, one line per message.
func read(req provider.ChatRequest) []string {
	var out []string
	for _, m := range req.Messages {
		text := textOf(m)
		switch {
		case m.ToolResult != nil:
			text = "result " + m.ToolResult.Content
		case len(m.ToolCalls) > 0:
			text = "call " + m.ToolCalls[0].Name
		}
		out = append(out, m.Role+" "+text)
	}
	return out
}

// bySystem answers each agent's calls, counted per agent, with its own
// script.
func bySystem(scripts map[string]func(n int) provider.ChatResponse) func(int, provider.ChatRequest) (provider.ChatResponse, error) {
	var mu sync.Mutex
	calls := map[string]int{}
	return func(_ int, req provider.ChatRequest) (provider.ChatResponse, error) {
		agent := strings.TrimPrefix(strings.SplitN(req.System, "\n", 2)[0], "I am ")
		mu.Lock()
		calls[agent]++
		n := calls[agent]
		mu.Unlock()
		return scripts[agent](n), nil
	}
}

func say(text string) provider.ChatResponse {
	return provider.ChatResponse{Content: text, StopReason: "end_turn"}
}

// A participant answers its messages in order, one at a time; the second
// turn reads the answer to the first. Each turn has its events and its end,
// and its own workflow ID.
func TestParticipant_AnswersInOrder(t *testing.T) {
	h := newHarness(t, func(n int, _ provider.ChatRequest) (provider.ChatResponse, error) {
		return say(fmt.Sprint("R", n)), nil
	}, nil)
	m1, m2 := h.human("M1", "Alice"), h.human("M2", "Bob")
	if err := h.run("jarvis", m1, m2); err != nil {
		t.Fatal(err)
	}
	sent := h.f.model.sent()
	if len(sent) != 2 {
		t.Fatalf("%d calls, want one per message", len(sent))
	}
	if got, want := read(sent[1]), []string{"user [Alice] M1", "assistant R1", "user [Bob] M2"}; !slices.Equal(got, want) {
		t.Errorf("the second turn read %q, want %q", got, want)
	}
	t1, t2 := store.TurnKey(m1.MessageID, "jarvis"), store.TurnKey(m2.MessageID, "jarvis")
	want := []string{"turn_started " + t1, "message R1", "turn_done " + t1, "turn_started " + t2, "message R2", "turn_done " + t2}
	if got := h.events(); !slices.Equal(got, want) {
		t.Errorf("events %q\nwant %q", got, want)
	}
	if ends := h.ends(); len(ends) != 2 || ends[t1] != "" || ends[t2] != "" {
		t.Errorf("ends %v, want one per turn, without error", ends)
	}
}

// The turn runs under its own ID, for its author, on the message's channel,
// told its part when the message addresses several agents, and signed.
func TestParticipant_TurnInput(t *testing.T) {
	h := newHarness(t, nil, answer)
	msg := h.human("@jarvis résume, @smith juge", "Alice")
	msg.SignReply, msg.Channel, msg.ChannelID = true, "web", ""
	msg.Next = []AddressedAgent{{ID: "smith", Name: "Agent Smith", Mention: "smith"}}
	msg.Part = &Part{Agents: []AddressedAgent{{ID: "jarvis", Name: "Jarvis", Mention: "jarvis"}, msg.Next[0]}, Quote: Quote("@jarvis résume,\n @smith juge")}
	if err := h.run("jarvis", msg); err != nil {
		t.Fatal(err)
	}
	if len(h.turns) != 1 {
		t.Fatalf("%d turns", len(h.turns))
	}
	got := h.turns[0]
	if got.workflowID != "s1:p:jarvis:m1" || got.in.TurnKey != "m1.jarvis" || got.in.UserID != "u-Alice" || got.in.UserName != "Alice" ||
		got.in.Channel != "web" || got.in.ChannelID != "" || !got.in.SignReply || got.in.UserMessage != "" {
		t.Errorf("turn %+v", got)
	}
	for _, want := range []string{"from Alice that reads “@jarvis résume, @smith juge”", "You are Jarvis (@jarvis)", "after you"} {
		if !strings.Contains(got.in.PartNote, want) {
			t.Errorf("part note %q lacks %q", got.in.PartNote, want)
		}
	}
}

// A message without an ID cannot be read: it is skipped, the next one
// answered.
func TestParticipant_SkipsAMessageWithoutID(t *testing.T) {
	h := newHarness(t, nil, answer)
	if err := h.run("jarvis", ParticipantMessage{UserID: "u-alice"}, h.human("hello", "Alice")); err != nil {
		t.Fatal(err)
	}
	if len(h.turns) != 1 || h.turns[0].in.TurnKey != "m1.jarvis" {
		t.Errorf("turns %+v, want the stored message's alone", h.turns)
	}
}

// Two participants answer different messages at once: Smith, while Jarvis
// is between two calls of his turn, reads nothing of it; Jarvis's next turn
// reads Smith's, ended, whole.
func TestParticipant_TwoInParallel(t *testing.T) {
	jarvisMidway, smithDone := make(chan struct{}), make(chan struct{})
	h := newHarness(t, bySystem(map[string]func(int) provider.ChatResponse{
		"jarvis": func(n int) provider.ChatResponse {
			switch n {
			case 1:
				return provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{{ID: "j1", Name: "web_fetch", Input: json.RawMessage(`{}`)}}}
			case 2:
				// Its call and result are written: Smith answers now.
				close(jarvisMidway)
				<-smithDone
				return say("J1")
			}
			return say("J2")
		},
		"smith": func(int) provider.ChatResponse {
			defer close(smithDone)
			return say("S1")
		},
	}), nil)
	// Smith's turn waits for Jarvis to be midway before it loads anything.
	h.env.RegisterActivityWithOptions(func(_ context.Context, in activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		if in.AgentID == "smith" {
			<-jarvisMidway
		}
		return activity.LoadSkillsForAgentOutput{Name: h.f.session.agents[in.AgentID]}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	m1, m2 := h.human("@jarvis read", "Alice"), h.human("@smith hi", "Bob")
	h.env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context) error {
		start := func(agent string, inbox ...ParticipantMessage) sdkworkflow.ChildWorkflowFuture {
			return sdkworkflow.ExecuteChildWorkflow(sdkworkflow.WithChildOptions(ctx, sdkworkflow.ChildWorkflowOptions{WorkflowID: ParticipantWorkflowID("s1", agent)}),
				ParticipantWorkflow, ParticipantInput{SessionID: "s1", AgentID: agent, Inbox: inbox})
		}
		jarvis, smith := start("jarvis", m1), start("smith", m2)
		return errors.Join(jarvis.Get(ctx, nil), smith.Get(ctx, nil))
	}, sdkworkflow.RegisterOptions{Name: "both"})
	h.env.ExecuteWorkflow("both")
	if err := h.env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}

	var smith provider.ChatRequest
	for _, req := range h.f.model.sent() {
		if strings.HasPrefix(req.System, "I am smith") {
			smith = req
		}
	}
	if got, want := read(smith), []string{"user [Alice] @jarvis read\n\n[Bob] @smith hi"}; !slices.Equal(got, want) {
		t.Errorf("smith read %q, want the two messages alone", got)
	}

	// Jarvis answers a third message: Smith's turn, ended, is read whole.
	m3 := h.human("@jarvis and now?", "Alice")
	h2 := newHarness(t, bySystem(map[string]func(int) provider.ChatResponse{"jarvis": func(int) provider.ChatResponse { return say("J2") }}), nil)
	h2.f.session.messages = h.f.session.history()
	if err := h2.run("jarvis", m3); err != nil {
		t.Fatal(err)
	}
	got := read(h2.f.model.sent()[0])
	want := []string{"user [Alice] @jarvis read", "assistant call web_fetch", "tool result page content", "assistant J1",
		"user [Bob] @smith hi\n\n[agent Agent Smith (@smith)] S1\n\n[Alice] @jarvis and now?"}
	if !slices.Equal(got, want) {
		t.Errorf("jarvis read %q\nwant %q", got, want)
	}
}

// relayed decodes what a relay delivered.
func relayed(t *testing.T, in activity.RelayInput) (ParticipantMessage, ParticipantInput) {
	t.Helper()
	var msg ParticipantMessage
	var start ParticipantInput
	if err := json.Unmarshal(in.Message, &msg); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(in.Start, &start); err != nil {
		t.Fatal(err)
	}
	return msg, start
}

// A message to Jarvis then Smith: Jarvis's participant relays it once his
// turn succeeded, Smith's turn reads Jarvis's answer to it. A relay
// delivered twice is answered once.
func TestParticipant_Relay(t *testing.T) {
	h := newHarness(t, bySystem(map[string]func(int) provider.ChatResponse{
		"jarvis": func(int) provider.ChatResponse { return say("résumé") },
		"smith":  func(int) provider.ChatResponse { return say("jugé") },
	}), nil)
	msg := h.human("@jarvis résume, @smith juge", "Alice")
	smith := AddressedAgent{ID: "smith", Name: "Agent Smith", Mention: "smith"}
	msg.Next, msg.SignReply = []AddressedAgent{smith}, true
	msg.Part = &Part{Agents: []AddressedAgent{{ID: "jarvis", Name: "Jarvis", Mention: "jarvis"}, smith}, Quote: "@jarvis résume, @smith juge"}
	if err := h.run("jarvis", msg); err != nil {
		t.Fatal(err)
	}
	if len(h.relays) != 1 {
		t.Fatalf("%d relays, want one", len(h.relays))
	}
	in := h.relays[0]
	next, start := relayed(t, in)
	if in.WorkflowID != "s1:p:smith" || in.WorkflowType != "ParticipantWorkflow" || in.Signal != SignalMessage || in.TaskQueue == "" {
		t.Errorf("relay %+v", in)
	}
	if start.SessionID != "s1" || start.AgentID != "smith" || start.Channel != "telegram" || start.ChannelID != "42" || len(start.Inbox) != 0 {
		t.Errorf("start %+v", start)
	}
	if next.MessageID != msg.MessageID || len(next.Next) != 0 || !slices.Equal(next.EarlierTurns, []string{"m1.jarvis"}) || !next.SignReply || next.Part == nil || next.UserID != "u-Alice" {
		t.Errorf("relayed %+v", next)
	}

	h2 := newHarness(t, bySystem(map[string]func(int) provider.ChatResponse{"smith": func(int) provider.ChatResponse { return say("jugé") }}), nil)
	h2.f.session.messages = h.f.session.history()
	h2.env.ExecuteWorkflow(ParticipantWorkflow, ParticipantInput{SessionID: "s1", AgentID: "smith", Inbox: []ParticipantMessage{next, next}})
	if err := h2.env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	sent := h2.f.model.sent()
	if len(sent) != 1 {
		t.Fatalf("smith answered %d times, want once", len(sent))
	}
	if got, want := read(sent[0]), []string{"user [Alice] @jarvis résume, @smith juge\n\n[agent Jarvis (@jarvis)] résumé"}; !slices.Equal(got, want) {
		t.Errorf("smith read %q, want %q", got, want)
	}
	if !strings.Contains(sent[0].System, "You are Agent Smith (@smith)") || !strings.Contains(sent[0].System, "before you") {
		t.Errorf("smith's prompt lacks its part: %q", sent[0].System)
	}
}

// The relay stops when the turn fails or is stopped.
func TestParticipant_RelayStopsOnFailureOrStop(t *testing.T) {
	for name, c := range map[string]struct {
		agent  func(sdkworkflow.Context, AgentWorkflowInput) (AgentWorkflowOutput, error)
		stopAt time.Duration
	}{
		"failure": {func(sdkworkflow.Context, AgentWorkflowInput) (AgentWorkflowOutput, error) {
			return AgentWorkflowOutput{Error: "call LLM: boom"}, nil
		}, 0},
		"stop": {slowAgent, 5 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, nil, c.agent)
			msg := h.human("@jarvis then @smith", "Alice")
			msg.Next = []AddressedAgent{{ID: "smith"}}
			if c.stopAt > 0 {
				h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(SignalStopTurn, nil) }, c.stopAt)
			}
			if err := h.run("jarvis", msg); err != nil {
				t.Fatal(err)
			}
			if len(h.relays) != 0 {
				t.Errorf("relayed %+v", h.relays)
			}
		})
	}
}

// slowAgent is a turn of 10 s that, stopped, returns what it wrote.
func slowAgent(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
	if err := sdkworkflow.Sleep(ctx, 10*time.Second); err != nil {
		content, _ := json.Marshal("half")
		return AgentWorkflowOutput{Response: "Agent cancelled.", NewMessages: []store.Message{{Role: store.RoleAssistant, Content: string(content), AgentID: in.AgentID}}}, nil
	}
	return AgentWorkflowOutput{Response: "done"}, nil
}

// A relay that fails for good: Smith's turn on the message gets an end
// saying so, the channel is told, and Jarvis goes on with his next message.
func TestParticipant_RelayFailsForGood(t *testing.T) {
	h := newHarness(t, nil, answer)
	h.relayErr = temporal.NewNonRetryableApplicationError("server away", "Unavailable", nil)
	msg := h.human("@jarvis then @smith", "Alice")
	msg.Next = []AddressedAgent{{ID: "smith", Mention: "smith"}}
	next := h.human("@jarvis again", "Alice")
	if err := h.run("jarvis", msg, next); err != nil {
		t.Fatal(err)
	}
	ends := h.ends()
	if reason := ends["m1.smith"]; !strings.Contains(reason, "le relais vers @smith a échoué") {
		t.Errorf("smith's end %q, want the relay's failure", reason)
	}
	if len(h.turns) != 2 {
		t.Errorf("%d turns, want jarvis's two", len(h.turns))
	}
	if !slices.ContainsFunc(h.events(), func(e string) bool { return strings.Contains(e, "le relais vers @smith a échoué") }) {
		t.Errorf("the channel was not told: %q", h.events())
	}

	// Delivered after all, late: Smith does not answer it.
	h2 := newHarness(t, nil, answer)
	h2.f.session.messages = h.f.session.history()
	if err := h2.run("smith", ParticipantMessage{MessageID: msg.MessageID, UserID: "u-Alice", EarlierTurns: []string{"m1.jarvis"}}); err != nil {
		t.Fatal(err)
	}
	if len(h2.turns) != 0 {
		t.Errorf("smith answered a message whose relay failed: %+v", h2.turns)
	}
}

// A relay received after the participant answered a later message: its
// turn on the earlier one does not read its own later turn.
func TestParticipant_RelayAfterALaterMessage(t *testing.T) {
	h := newHarness(t, bySystem(map[string]func(int) provider.ChatResponse{"smith": func(n int) provider.ChatResponse { return say(fmt.Sprint("S", n)) }}), nil)
	m1 := h.human("@jarvis résume, @smith juge", "Alice")
	jarvis := store.TurnKey(m1.MessageID, "jarvis")
	h.f.session.add(store.TurnMessageKey(jarvis, 0), store.Message{Role: store.RoleAssistant, AgentID: "jarvis", Content: `"résumé"`})
	m3 := h.human("@smith hello", "Bob")
	h.f.session.add(store.TurnEndKey(jarvis), store.TurnEnd("jarvis", ""))
	relay := m1
	relay.EarlierTurns = []string{jarvis}
	// Smith answers the later message first, then gets the relay.
	if err := h.run("smith", m3, relay); err != nil {
		t.Fatal(err)
	}
	sent := h.f.model.sent()
	if len(sent) != 2 {
		t.Fatalf("%d calls", len(sent))
	}
	if got, want := read(sent[1]), []string{"user [Alice] @jarvis résume, @smith juge\n\n[agent Jarvis (@jarvis)] résumé"}; !slices.Equal(got, want) {
		t.Errorf("smith on the relay read %q, want %q", got, want)
	}
}

// stop-turn stops the turn running, not the messages waiting; clear stops
// it and drops them, each with an end saying so. A late delivery of a
// dropped message is not answered.
func TestParticipant_StopAndClear(t *testing.T) {
	for _, c := range []struct {
		signal    string
		wantTurns int
	}{
		{SignalStopTurn, 2},
		{SignalClear, 1},
	} {
		t.Run(c.signal, func(t *testing.T) {
			h := newHarness(t, nil, slowAgent)
			m1, m2 := h.human("M1", "Alice"), h.human("M2", "Alice")
			h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(c.signal, nil) }, 5*time.Second)
			if err := h.run("jarvis", m1, m2); err != nil {
				t.Fatal(err)
			}
			if len(h.turns) != c.wantTurns {
				t.Errorf("%d turns, want %d", len(h.turns), c.wantTurns)
			}
			ends := h.ends()
			if reason, ok := ends["m1.jarvis"]; !ok || reason != "" {
				t.Errorf("the stopped turn's end: %q %v, want one without error", reason, ok)
			}
			if !slices.Contains(h.events(), "message Agent interrupted by user.") {
				t.Errorf("events %q, want the interruption", h.events())
			}
			if c.signal == SignalClear {
				if reason := ends["m2.jarvis"]; reason != clearedReason {
					t.Errorf("the dropped message's end %q", reason)
				}
				h2 := newHarness(t, nil, answer)
				h2.f.session.messages = h.f.session.history()
				if err := h2.run("jarvis", m2); err != nil {
					t.Fatal(err)
				}
				if len(h2.turns) != 0 {
					t.Errorf("a dropped message was answered: %+v", h2.turns)
				}
			}
		})
	}
}

// A stop sent while no turn runs is not for the next message.
func TestParticipant_AStaleStopIsDropped(t *testing.T) {
	h := newHarness(t, nil, slowAgent)
	m1 := h.human("M1", "Alice")
	// Sent before the participant's first task: it finds it with the message.
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(SignalStopTurn, nil) }, 0)
	if err := h.run("jarvis", m1); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(h.events(), "message Agent interrupted by user.") {
		t.Errorf("events %q: a stop sent before the turn interrupted it", h.events())
	}
}

// An agent deleted with messages waiting: no turn, an end saying why, and
// the next message answered. An agent the store holds is answered at once,
// whatever the worker's catalog says.
func TestParticipant_AgentGone(t *testing.T) {
	h := newHarness(t, nil, answer)
	h.f.session.agents = map[string]string{"smith": "Agent Smith"} // jarvis deleted
	m1 := h.human("@jarvis hi", "Alice")
	if err := h.run("jarvis", m1); err != nil {
		t.Fatal(err)
	}
	if len(h.turns) != 0 {
		t.Errorf("turns %+v, want none", h.turns)
	}
	if reason := h.ends()["m1.jarvis"]; !strings.Contains(reason, `agent "jarvis" no longer exists`) {
		t.Errorf("end %q", reason)
	}
	if got := h.events(); len(got) != 2 || got[1] != "turn_done m1.jarvis" || !strings.HasPrefix(got[0], "message Error processing message") {
		t.Errorf("events %q, want the error then the message done", got)
	}

	h = newHarness(t, nil, answer)
	h.f.session.agents["fresh"] = "Fresh" // created a moment ago, in the store only
	if err := h.run("fresh", h.human("@fresh hi", "Alice")); err != nil {
		t.Fatal(err)
	}
	if len(h.turns) != 1 {
		t.Errorf("the new agent did not answer")
	}
}

// A session deleted: the participant ends without a turn.
func TestParticipant_SessionGone(t *testing.T) {
	h := newHarness(t, nil, answer)
	m1, m2 := h.human("M1", "Alice"), h.human("M2", "Alice")
	h.f.session.deleted = true
	if err := h.run("jarvis", m1, m2); err != nil {
		t.Fatal(err)
	}
	if len(h.turns) != 0 || len(h.ends()) != 0 {
		t.Errorf("turns %+v, ends %v: want none", h.turns, h.ends())
	}
}

// The turn's end is written in every case, after what the turn wrote:
// done, failed after writing (a partial flush), failed before writing
// anything, failed hard (its workflow failed), stopped. A failure is told on
// the channel.
func TestParticipant_TurnEndInEveryCase(t *testing.T) {
	call := store.Message{Role: store.RoleAssistant, AgentID: "jarvis", ToolCalls: []store.ToolCall{{ID: "t1", Name: "web_fetch"}}}
	result := store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "t1", Content: "page"}}
	for name, c := range map[string]struct {
		agent  func(sdkworkflow.Context, AgentWorkflowInput) (AgentWorkflowOutput, error)
		stopAt time.Duration
		reason string
		wrote  int
	}{
		"done": {answer, 0, "", 0},
		"failed after a partial write": {func(sdkworkflow.Context, AgentWorkflowInput) (AgentWorkflowOutput, error) {
			return AgentWorkflowOutput{NewMessages: []store.Message{call, result}, Error: "call LLM: boom"}, nil
		}, 0, "call LLM: boom", 2},
		"failed before writing": {func(sdkworkflow.Context, AgentWorkflowInput) (AgentWorkflowOutput, error) {
			return AgentWorkflowOutput{Error: "call LLM: credit balance is too low"}, nil
		}, 0, "call LLM: credit balance is too low", 0},
		"failed hard": {func(sdkworkflow.Context, AgentWorkflowInput) (AgentWorkflowOutput, error) {
			return AgentWorkflowOutput{}, temporal.NewNonRetryableApplicationError("load skills: boom", "X", nil)
		}, 0, "load skills: boom", 0},
		"stopped": {slowAgent, 5 * time.Second, "", 1},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, nil, c.agent)
			if c.stopAt > 0 {
				h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(SignalStopTurn, nil) }, c.stopAt)
			}
			if err := h.run("jarvis", h.human("go", "Alice")); err != nil {
				t.Fatal(err)
			}
			history := h.f.session.history()
			last := history[len(history)-1]
			if last.Key != "m1.jarvis:end" || last.Kind != store.KindTurnEnd || !strings.Contains(store.TurnEndError(last.Message), c.reason) || (c.reason == "") != (store.TurnEndError(last.Message) == "") {
				t.Errorf("last message %s %+v, want the end with %q", last.Key, last.Message, c.reason)
			}
			if len(history) != 2+c.wrote {
				t.Errorf("%d messages, want the question, the %d written, the end", len(history), c.wrote)
			}
			told := slices.ContainsFunc(h.events(), func(e string) bool { return strings.HasPrefix(e, "message Error processing message") })
			if told != (c.reason != "") {
				t.Errorf("events %q: told of an error %v, want %v", h.events(), told, c.reason != "")
			}
		})
	}
}

// The state query tells the message answered and how many wait, those
// delivered during the turn included.
func TestParticipant_State(t *testing.T) {
	h := newHarness(t, nil, slowAgent)
	m1, m2, m3 := h.human("M1", "Alice"), h.human("M2", "Bob"), h.human("M3", "Bob")
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(SignalMessage, m3) }, 2*time.Second)
	var state ParticipantState
	h.env.RegisterDelayedCallback(func() {
		v, err := h.env.QueryWorkflow(QueryState)
		if err != nil {
			t.Error(err)
			return
		}
		v.Get(&state)
	}, 5*time.Second)
	if err := h.run("jarvis", m1, m2); err != nil {
		t.Fatal(err)
	}
	if state.Current == nil || state.Current.MessageID != m1.MessageID || state.Current.UserName != "Alice" || state.Queued != 2 || state.Background == nil {
		t.Errorf("state %+v (current %+v), want M1 answered, 2 waiting", state, state.Current)
	}
	if len(h.turns) != 3 {
		t.Errorf("%d turns, want 3", len(h.turns))
	}
}

// A participant whose history grows continues as new with its inbox: no
// message lost, no signal left unhandled, an input that does not carry the
// messages' text.
func TestParticipant_ContinueAsNewCarriesTheInbox(t *testing.T) {
	second := func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
		return AgentWorkflowOutput{Response: "done"}, sdkworkflow.Sleep(ctx, time.Second)
	}
	h := newHarness(t, nil, second)
	var msgs []ParticipantMessage
	for i := range 20 {
		msgs = append(msgs, h.human(fmt.Sprint("M", i, " ", strings.Repeat("long text ", 1000)), "Alice"))
	}
	// The other 19 arrive during the first turn, and the history grows past
	// the threshold.
	for i, m := range msgs[1:] {
		h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(SignalMessage, m) }, time.Duration(i+1)*time.Millisecond)
	}
	h.env.RegisterDelayedCallback(func() { h.env.SetCurrentHistoryLength(maxParticipantHistoryEvents) }, 500*time.Millisecond)
	err := h.run("jarvis", msgs[0])
	var next *sdkworkflow.ContinueAsNewError
	if !errors.As(err, &next) {
		t.Fatalf("error %v, want a continue-as-new", err)
	}
	var in ParticipantInput
	if err := converter.GetDefaultDataConverter().FromPayloads(next.Input, &in); err != nil {
		t.Fatal(err)
	}
	answered := map[int64]bool{}
	for _, run := range h.turns {
		anchor, _ := store.TurnAnchor(run.in.TurnKey)
		answered[anchor] = true
	}
	for _, m := range in.Inbox {
		if answered[m.MessageID] {
			t.Errorf("message %d both answered and carried", m.MessageID)
		}
		answered[m.MessageID] = true
	}
	for _, m := range msgs {
		if !answered[m.MessageID] {
			t.Errorf("message %d lost", m.MessageID)
		}
	}
	if len(h.turns) != 1 || len(in.Inbox) != 19 || in.SessionID != "s1" || in.AgentID != "jarvis" || in.Channel != "telegram" {
		t.Errorf("%d answered, carried %+v", len(h.turns), in)
	}
	if size := len(next.Input.String()); size > 8*1024 {
		t.Errorf("carried input of %d bytes", size)
	}
	if !slices.IsSortedFunc(in.Inbox, func(a, b ParticipantMessage) int { return int(a.MessageID - b.MessageID) }) {
		t.Errorf("inbox out of order: %+v", in.Inbox)
	}

	// The new run answers the rest.
	h2 := newHarness(t, nil, second)
	h2.f.session.messages = h.f.session.history()
	h2.env.ExecuteWorkflow(ParticipantWorkflow, in)
	if err := h2.env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(h2.turns) != len(in.Inbox) {
		t.Errorf("the new run answered %d of %d", len(h2.turns), len(in.Inbox))
	}
}

// The part note quotes the message it is about, on one line and clipped, and
// names each agent as the history does.
func TestPartNote_QuotesTheMessage(t *testing.T) {
	agents := []AddressedAgent{{ID: "cr", Name: "Reviewer de code", Mention: "cr"}, {ID: "smith"}}
	long := "@cr relis\n\n" + strings.Repeat("é", 300) + " FIN"
	part := &Part{Agents: agents, Quote: Quote(long)}
	note := part.noteFor("smith", "")
	for _, want := range []string{"the message that reads “@cr relis éé", "…”", "Reviewer de code (@cr), smith (@smith)", "You are smith (@smith)"} {
		if !strings.Contains(note, want) {
			t.Errorf("note %q lacks %q", note, want)
		}
	}
	if strings.Contains(note, "FIN") || strings.Contains(note, "relis\n") || !utf8.ValidString(note) {
		t.Errorf("note %q: the quote is not clipped to one line", note)
	}
	if partNote(agents[:1], 0, part.Quote, "") != "" || part.noteFor("jarvis", "") != "" || (*Part)(nil).noteFor("cr", "") != "" {
		t.Error("a note for an agent addressed alone, or not at all")
	}
}

// childRun is what a stubbed AgentWorkflow saw of its run.
type childRun struct {
	workflowID string
	in         AgentWorkflowInput
}

// answer is a turn that answers at once.
func answer(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
	return AgentWorkflowOutput{Response: "done"}, nil
}

// A member writes while a turn runs, before it wrote anything: the turn does
// not read the message, and the next turn, which answers it, reads it last,
// after the first turn's answer, not before it.
func TestParticipant_AMessageWrittenMidTurnIsTheNextOne(t *testing.T) {
	var h *harness
	h = newHarness(t, func(n int, _ provider.ChatRequest) (provider.ChatResponse, error) {
		switch n {
		case 1: // Bob writes while the model thinks: the server stores it
			h.human("and the tests?", "Bob")
			return provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{{ID: "t1", Name: "web_fetch", Input: json.RawMessage(`{}`)}}}, nil
		case 2:
			return say("read"), nil
		}
		return say("tests too"), nil
	}, nil)
	alice := h.human("read the page", "Alice")
	// Bob's message, stored as 2 during the first call, is delivered behind
	// Alice's.
	bob := ParticipantMessage{MessageID: 2, UserID: "u-Bob", UserName: "Bob"}
	if err := h.run("jarvis", alice, bob); err != nil {
		t.Fatal(err)
	}
	requests := h.f.model.sent()
	if len(requests) != 3 {
		t.Fatalf("%d LLM calls, want 3: two for Alice's message, one for Bob's", len(requests))
	}
	for _, line := range read(requests[1]) {
		if strings.Contains(line, "and the tests?") {
			t.Errorf("the first turn read Bob's message, written after it started: %q", read(requests[1]))
		}
	}
	want := []string{"user [Alice] read the page", "assistant call web_fetch", "tool result page content", "assistant read", "user [Bob] and the tests?"}
	if got := read(requests[2]); !slices.Equal(got, want) {
		t.Errorf("the next turn read %q\nwant %q", got, want)
	}
}

// A conversation too long for the model fails its turn with what to do,
// written as the turn's end.
func TestParticipant_RecordsAConversationTooLong(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.f.llm.MaxContextBytes = 2000
	if err := h.run("jarvis", h.human(strings.Repeat("x", 3000), "Alice")); err != nil {
		t.Fatal(err)
	}
	if got := h.ends()["m1.jarvis"]; got != activity.ContextTooLongMessage {
		t.Errorf("end %q, want %q", got, activity.ContextTooLongMessage)
	}
}

// A turn event the server does not take is given up at once: the turns run,
// persist and end as if it had gone, without a retry to wait for.
func TestParticipant_TurnEventsFailing(t *testing.T) {
	var starts []time.Time
	h := newHarness(t, nil, func(ctx sdkworkflow.Context, in AgentWorkflowInput) (AgentWorkflowOutput, error) {
		starts = append(starts, sdkworkflow.Now(ctx))
		return answer(ctx, in)
	})
	sent := 0
	h.env.RegisterActivityWithOptions(func(_ context.Context, in activity.NotifyInput) error {
		if in.Event.Type == EventTurnStarted || in.Event.Type == EventTurnDone {
			sent++
			return errors.New("server away")
		}
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	if err := h.run("jarvis", h.human("M1", "Alice"), h.human("M2", "Alice")); err != nil {
		t.Fatal(err)
	}
	if sent != 4 || len(starts) != 2 {
		t.Fatalf("%d events sent, %d turns: want each event tried once, both turns", sent, len(starts))
	}
	if gap := starts[1].Sub(starts[0]); gap >= time.Second {
		t.Errorf("the second turn started %v after the first", gap)
	}
	if len(h.ends()) != 2 {
		t.Errorf("ends %v", h.ends())
	}
}

// A message delivered again after its participant ended (a delivery
// retried) starts a new run that does not answer it again.
func TestParticipant_RedeliveredAfterItEnded(t *testing.T) {
	h := newHarness(t, nil, answer)
	m1 := h.human("M1", "Alice")
	if err := h.run("jarvis", m1); err != nil {
		t.Fatal(err)
	}
	h2 := newHarness(t, nil, answer)
	h2.f.session.messages = h.f.session.history()
	if err := h2.run("jarvis", m1); err != nil {
		t.Fatal(err)
	}
	if len(h.turns) != 1 || len(h2.turns) != 0 {
		t.Errorf("turns %d then %d, want one then none", len(h.turns), len(h2.turns))
	}
	if got := h2.events(); !slices.Equal(got, []string{"turn_done m1.jarvis"}) {
		t.Errorf("the second run's events %q, want the message done alone", got)
	}
}
