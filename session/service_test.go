package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

const sid = "6f1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b"

func newTest(st *memStore, tc *fakeTemporal) *Service {
	return New(st, tc, &nopHub{}, Config{WorkflowQueue: "agent", DefaultAgentID: "default"})
}

// A title is cut in characters: cut in bytes, an accented letter can be split
// and Postgres refuses the string.
func TestSetTitleFrom_CutsOnCharacters(t *testing.T) {
	st := &memStore{}
	newTest(st, &fakeTemporal{}).setTitleFrom(sid, strings.Repeat("é", 100))
	if !utf8.ValidString(st.title) || st.title != strings.Repeat("é", maxTitleRunes)+"..." {
		t.Errorf("title %q", st.title)
	}
}

// A session ID goes into a visibility query between quotes: anything but a
// canonical UUID is refused, whatever the route checked before.
func TestVisibilityQueriesTakeOnlyUUIDs(t *testing.T) {
	for _, id := range []string{"", "s1", "x' OR WorkflowId STARTS_WITH '", "{" + sid + "}", "urn:uuid:" + sid, strings.ToUpper(sid)} {
		if q, err := pendingQuestionsQuery(id); err == nil {
			t.Errorf("pendingQuestionsQuery(%q) = %q", id, q)
		}
	}
	if _, err := pendingQuestionsQuery(sid); err != nil {
		t.Error(err)
	}

	tc := &fakeTemporal{running: []string{sid + ":p:default:m1:tool:ask_user:c1"}}
	if newTest(&memStore{}, tc).AnswerPending(context.Background(), "x' OR '1'='1", "yes") || len(tc.lists) != 0 {
		t.Errorf("a forged session ID reached Temporal: %v", tc.lists)
	}
}

// An answer from a channel without buttons reaches a question a sub-agent
// asked: they are found by type, under the session's ID. With several
// waiting, the oldest one.
func TestAnswerPending_FindsSubAgentQuestions(t *testing.T) {
	question := sid + ":p:jarvis:m3:tool:agent_analyst:c1:tool:ask_user:c2"
	newer := sid + ":p:smith:m4:tool:ask_user:c1"
	now := time.Now()
	tc := &fakeTemporal{running: []string{newer, question}, startedAt: map[string]time.Time{question: now.Add(-time.Minute), newer: now}}
	if !newTest(&memStore{}, tc).AnswerPending(context.Background(), sid, "yes") {
		t.Fatal("the answer was not delivered")
	}
	if len(tc.lists) != 1 || !strings.Contains(tc.lists[0], "WorkflowType = 'AskUserWorkflow'") || !strings.Contains(tc.lists[0], "STARTS_WITH '"+sid+":'") {
		t.Errorf("query %v", tc.lists)
	}
	if len(tc.signals) != 1 || tc.signals[0] != question {
		t.Errorf("signalled %v, want the oldest question", tc.signals)
	}
	if qs := newTest(&memStore{}, tc).PendingQuestions(context.Background(), sid); len(qs) != 0 {
		t.Errorf("questions %+v from a fake that answers no query", qs)
	}
}

// Answering through a session reaches that session's questions only.
func TestAnswer_OwnQuestionsOnly(t *testing.T) {
	tc := &fakeTemporal{}
	s := newTest(&memStore{}, tc)
	for _, other := range []string{"other:p:x:m1:tool:ask_user:c1", sid + "x:p:x:m1:tool:ask_user:c1", sid + "-tool-ask_user-1"} {
		if err := s.Answer(context.Background(), sid, other, "yes"); !errors.Is(err, ErrForeignQuestion) {
			t.Errorf("another session's question %q: %v", other, err)
		}
	}
	own := sid + ":p:default:m1:tool:ask_user:c1"
	if err := s.Answer(context.Background(), sid, own, "  "); !errors.Is(err, ErrEmptyAnswer) {
		t.Errorf("an empty answer: %v", err)
	}
	if err := s.Answer(context.Background(), sid, own, "yes"); err != nil || len(tc.signals) != 1 {
		t.Errorf("own question: %v, %v", err, tc.signals)
	}
}

// Every tab refreshes the tree: the states are read from Temporal once for
// all of them, and again after an action changes them.
func TestStatuses_SharedForAFewSeconds(t *testing.T) {
	tc := &fakeTemporal{running: []string{sid + ":p:default"}}
	s := newTest(&memStore{}, tc)
	for i := 0; i < 10; i++ {
		s.Statuses(context.Background())
	}
	if len(tc.lists) != 3 {
		t.Errorf("%d visibility queries for 10 reads, want 3", len(tc.lists))
	}
	// The fake answers every query with the same workflow: the strongest
	// state, a question waiting, wins.
	if got := s.Statuses(context.Background())[sid]; got != StatusWaiting {
		t.Errorf("status %q, want waiting", got)
	}
	s.statuses.invalidate()
	s.Statuses(context.Background())
	if len(tc.lists) != 6 {
		t.Errorf("%d visibility queries after an invalidation, want 6", len(tc.lists))
	}
}

// states is what the visibility queries tell: the session in status.
func states(status Status) *visible {
	return &visible{statuses: map[string]Status{sid: status}}
}

// The request that starts a load and goes away does not cancel it: the
// others share that load, and get what it read.
func TestStatusCache_LoadOutlivesTheRequest(t *testing.T) {
	var c statusCache
	release := make(chan struct{})
	var loadErr error
	loads := 0
	load := func(ctx context.Context) *visible {
		loads++
		<-release
		loadErr = ctx.Err()
		return states(StatusWorking)
	}

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan *visible)
	go func() { got <- c.get(ctx, load) }()
	cancel()
	if v := <-got; v != nil {
		t.Errorf("a cancelled request got %v, want nil", v)
	}

	close(release)
	if v := c.get(context.Background(), load); v.statuses[sid] != StatusWorking {
		t.Errorf("after the load: %v", v)
	}
	if loadErr != nil {
		t.Errorf("the load ran under the cancelled request's context: %v", loadErr)
	}
	if loads != 1 {
		t.Errorf("%d loads, want 1 shared", loads)
	}
}

// While states expire and reload, requests get the previous ones instead of
// waiting behind a slow Temporal.
func TestStatusCache_ServesStaleStatesWhileReloading(t *testing.T) {
	c := statusCache{value: states(StatusWorking), at: time.Now().Add(-time.Minute)}
	release := make(chan struct{})
	load := func(context.Context) *visible {
		<-release
		return states(StatusWaiting)
	}

	if v := c.get(context.Background(), load); v.statuses[sid] != StatusWorking {
		t.Errorf("during the reload: %v, want the previous states", v)
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for c.get(context.Background(), load).statuses[sid] != StatusWaiting {
		if time.Now().After(deadline) {
			t.Fatal("the reloaded states never replaced the previous ones")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A load that started before an action must not bring back the states from
// before it.
func TestStatusCache_InvalidationDiscardsARunningLoad(t *testing.T) {
	var c statusCache
	started, release := make(chan struct{}), make(chan struct{})
	var loads atomic.Int32
	load := func(context.Context) *visible {
		if loads.Add(1) == 1 {
			close(started)
			<-release
			return states(StatusWorking)
		}
		return states(StatusIdle)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { c.get(ctx, load) }()
	// Wait for the load itself, not for get to mark one as running: the
	// refresh goroutine may not have called load yet, and a load started
	// after the invalidation could then be the one that counts as first.
	<-started
	c.invalidate()
	close(release)
	cancel()

	if v := c.get(context.Background(), load); v.statuses[sid] != StatusIdle {
		t.Errorf("got %v, want the states read after the invalidation", v)
	}
}

// An empty message would poison the session; a fork takes none before its
// summary.
func TestDeliver_Refusals(t *testing.T) {
	st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}}
	s := newTest(st, &fakeTemporal{})
	alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
	sess := &store.Session{SessionID: sid, AgentID: "default"}

	if _, err := s.Deliver(context.Background(), sess, alice, " \n"); !errors.Is(err, ErrEmptyMessage) {
		t.Errorf("empty message: %v", err)
	}
	fork := &store.Session{SessionID: sid, AgentID: "default", ForkedAtMessageID: 3}
	// No summary yet and its workflow is not running: failed, not pending,
	// so the message goes through.
	if _, err := s.Deliver(context.Background(), fork, alice, "hello"); err != nil {
		t.Errorf("fork with a failed summary: %v", err)
	}
	if len(st.appended) != 1 {
		t.Errorf("%d messages stored, want 1", len(st.appended))
	}
	// Its workflow still writing it: refused. (Each store takes one message:
	// a stored message titles the session in the background.)
	pendingStore := &memStore{members: st.members}
	pending := newTest(pendingStore, &fakeTemporal{running: []string{workflow.ForkWorkflowID(sid)}})
	if _, err := pending.Deliver(context.Background(), fork, alice, "hello"); !errors.Is(err, ErrSummaryPending) || len(pendingStore.appended) != 0 {
		t.Errorf("fork with its summary pending: %v, %d messages stored", err, len(pendingStore.appended))
	}
	// Recorded on the fork: in, whatever the workflow's state says.
	fork.SummaryMessageID = 1
	if _, err := pending.Deliver(context.Background(), fork, alice, "hello"); err != nil || len(pendingStore.appended) != 1 {
		t.Errorf("fork with its summary: %v, %d messages stored", err, len(pendingStore.appended))
	}
	// Whether the summary is in is read from the fork's row: no conversation
	// is loaded for it.
	if st.loads+pendingStore.loads != 0 {
		t.Errorf("%d conversations loaded", st.loads+pendingStore.loads)
	}
}

// A message is delivered to its agent's participant, started if it does not
// run, in one call on the participant's fixed ID, with the session's agent
// and channel. It carries the ID it was stored under, not its text: its
// turn reads the session up to it.
func TestDeliver_SignalsWithStartTheParticipant(t *testing.T) {
	st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}}
	tc := &fakeTemporal{}
	s := newTest(st, tc)
	alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
	sess := &store.Session{SessionID: sid, AgentID: "analyst", Channel: "telegram", ChannelID: "42"}

	if called, err := s.Deliver(context.Background(), sess, alice, "bonjour"); err != nil || !called {
		t.Fatalf("Deliver = %v, %v", called, err)
	}
	if len(tc.signals) != 0 || len(tc.started) != 0 {
		t.Errorf("signalled %v, started %v: want one signal-with-start only", tc.signals, tc.started)
	}
	if len(tc.signalStarts) != 1 {
		t.Fatalf("%d signal-with-start calls, want 1", len(tc.signalStarts))
	}
	got := tc.signalStarts[0]
	if got.id != sid+":p:analyst" || got.options.ID != got.id || got.options.TaskQueue != "agent" || got.signal != workflow.SignalMessage {
		t.Errorf("signal-with-start %q, options %+v, signal %q", got.id, got.options, got.signal)
	}
	want := workflow.ParticipantMessage{MessageID: 1, UserID: "u-alice", UserName: "alice@example.com", Channel: "telegram", ChannelID: "42"}
	if msg, ok := got.arg.(workflow.ParticipantMessage); !ok || fmt.Sprint(msg) != fmt.Sprint(want) {
		t.Errorf("message %+v, want %+v", got.arg, want)
	}
	start := workflow.ParticipantInput{SessionID: sid, AgentID: "analyst", Channel: "telegram", ChannelID: "42"}
	if len(got.input) != 1 || fmt.Sprint(got.input[0]) != fmt.Sprint(start) {
		t.Errorf("input %+v, want %+v", got.input, start)
	}
	s.background.Wait()
}

// A message to several agents goes to the first: the others follow by
// relay, in order, each told its part; the answers are signed.
func TestDeliver_SeveralAgents(t *testing.T) {
	st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}, agents: team}
	tc := &fakeTemporal{}
	s := newTest(st, tc)
	alice := &store.User{ID: "u-alice", Email: "alice@example.com", DisplayName: "Alice"}
	sess := &store.Session{SessionID: sid, AgentID: "default"}
	if _, err := s.Deliver(context.Background(), sess, alice, "@agentSmith résume,\n @jarvis juge"); err != nil {
		t.Fatal(err)
	}
	got := tc.signalStarts[0]
	msg := got.arg.(workflow.ParticipantMessage)
	smith, jarvis := workflow.AddressedAgent{ID: "smith", Name: "Agent Smith", Mention: "agentSmith"}, workflow.AddressedAgent{ID: "default", Name: "Jarvis", Mention: "jarvis"}
	if got.id != sid+":p:smith" || !msg.SignReply || fmt.Sprint(msg.Next) != fmt.Sprint([]workflow.AddressedAgent{jarvis}) ||
		msg.Part == nil || fmt.Sprint(msg.Part.Agents) != fmt.Sprint([]workflow.AddressedAgent{smith, jarvis}) || msg.Part.Quote != "@agentSmith résume, @jarvis juge" {
		t.Errorf("delivered to %s: %+v (part %+v)", got.id, msg, msg.Part)
	}
	// Another agent than the session's, alone: signed, no part.
	if _, err := s.Deliver(context.Background(), sess, alice, "@analyst ?"); err != nil {
		t.Fatal(err)
	}
	if msg := tc.signalStarts[1].arg.(workflow.ParticipantMessage); !msg.SignReply || msg.Part != nil || len(msg.Next) != 0 {
		t.Errorf("one other agent: %+v", msg)
	}
	// The session's agent: unsigned.
	if _, err := s.Deliver(context.Background(), sess, alice, "merci"); err != nil {
		t.Fatal(err)
	}
	if got := tc.signalStarts[2]; got.id != sid+":p:default" || got.arg.(workflow.ParticipantMessage).SignReply {
		t.Errorf("the session's agent: %s %+v", got.id, got.arg)
	}
	s.background.Wait()
}

// started and done are turn events of a participant on a message.
func started(agentID string, message int64) activity.SSEEvent {
	return turnEvent(workflow.EventTurnStarted, agentID, "", message)
}
func done(agentID string, message int64) activity.SSEEvent {
	return turnEvent(workflow.EventTurnDone, agentID, "", message)
}

// A participant takes MaxQueued messages waiting: one more is refused, and
// not stored. Once it starts one, there is room again. Another agent's
// queue is its own; a relay, which the server does not deliver, counts in
// none. A delivery that failed is not counted either.
func TestDeliver_QueueCap(t *testing.T) {
	st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}, agents: team}
	tc := &fakeTemporal{}
	s := newTest(st, tc)
	alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
	sess := &store.Session{SessionID: sid, AgentID: "default"}
	ctx := context.Background()
	for i := range MaxQueued {
		if _, err := s.Deliver(ctx, sess, alice, fmt.Sprint("M", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Deliver(ctx, sess, alice, "one more"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("over the cap: %v", err)
	}
	if len(st.appended) != MaxQueued || len(tc.signalStarts) != MaxQueued {
		t.Errorf("%d stored, %d delivered: the refused message is in", len(st.appended), len(tc.signalStarts))
	}
	if _, err := s.Deliver(ctx, sess, alice, "@agentSmith ?"); err != nil {
		t.Errorf("another agent's queue: %v", err)
	}
	s.Observe(sid, started("smith", 99)) // a relay: delivered by a participant
	s.Observe(sid, started("default", 1))
	if _, err := s.Deliver(ctx, sess, alice, "now it fits"); err != nil {
		t.Errorf("after a message started: %v", err)
	}
	// A participant done with a message it never started (dropped by a
	// clear, delivered twice) has it out of its queue too.
	s.Observe(sid, done("default", 2))
	tc.startErr = errors.New("temporal away")
	if _, err := s.Deliver(ctx, sess, alice, "lost"); err == nil {
		t.Error("a failed delivery reported none")
	}
	tc.startErr = nil
	if _, err := s.Deliver(ctx, sess, alice, "fits again"); err != nil {
		t.Errorf("a failed delivery took a place: %v", err)
	}
	s.background.Wait()
}

// The live message tells, for each agent it calls that answers another
// message, which one: it waits behind it.
func TestDeliver_QueuedBehind(t *testing.T) {
	st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}, agents: team}
	s := newTest(st, &fakeTemporal{})
	alice := &store.User{ID: "u-alice", Email: "alice@example.com"}
	sess := &store.Session{SessionID: sid, AgentID: "default"}
	s.Observe(sid, started("default", 7))
	if _, err := s.Deliver(context.Background(), sess, alice, "@jarvis @agentSmith ?"); err != nil {
		t.Fatal(err)
	}
	s.background.Wait()
	var event struct {
		Agents       []string         `json:"agents"`
		QueuedBehind map[string]int64 `json:"queued_behind"`
	}
	for _, ev := range s.hub.(*nopHub).on(sid) {
		if ev.Type == EventUserMessage {
			json.Unmarshal(ev.Data, &event)
		}
	}
	if fmt.Sprint(event.Agents) != "[Jarvis Agent Smith]" || len(event.QueuedBehind) != 1 || event.QueuedBehind["default"] != 7 {
		t.Errorf("event %+v", event)
	}
}

// The state of a session is its participants', each as its state query
// answers.
func TestState_ThePartipantsStates(t *testing.T) {
	jarvis, smith := sid+":p:jarvis", sid+":p:smith"
	tc := &fakeTemporal{running: []string{smith, jarvis}, states: map[string]interface{}{
		jarvis: workflow.ParticipantState{Current: &workflow.CurrentMessage{MessageID: 4, UserName: "Alice"}, Queued: 2},
		smith:  workflow.ParticipantState{Queued: 0},
	}}
	got, err := newTest(&memStore{}, tc).State(context.Background(), sid)
	if err != nil || len(got) != 2 {
		t.Fatalf("State = %+v, %v", got, err)
	}
	if got[0].AgentID != "jarvis" || got[0].WorkflowID != jarvis || got[0].Current == nil || got[0].Current.MessageID != 4 || got[0].Queued != 2 || got[1].AgentID != "smith" {
		t.Errorf("states %+v", got)
	}
	if len(tc.lists) != 1 || !strings.Contains(tc.lists[0], "WorkflowType = 'ParticipantWorkflow'") || !strings.Contains(tc.lists[0], "STARTS_WITH '"+sid+":'") {
		t.Errorf("query %v", tc.lists)
	}
	if _, err := newTest(&memStore{}, tc).State(context.Background(), "x' OR '1'='1"); err == nil {
		t.Error("a forged session ID reached Temporal")
	}
}

// Arrêter stops every participant at work: those the queries list, and one
// a turn event says started, which they may not list yet. With none, there
// is nothing to stop.
func TestCancel_StopsEveryParticipant(t *testing.T) {
	tc := &fakeTemporal{
		byType: map[string][]string{"ParticipantWorkflow": {sid + ":p:jarvis"}},
		states: map[string]interface{}{sid + ":p:smith": answering("smith", 3, bob.ID)},
	}
	s := newTest(&memStore{}, tc)
	s.Observe(sid, started("smith", 3))
	if err := s.Cancel(context.Background(), creatorSession(), alice); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(tc.signals) != fmt.Sprint([]string{sid + ":p:jarvis", sid + ":p:smith"}) || fmt.Sprint(tc.signalNames) != "[stop-turn stop-turn]" {
		t.Errorf("signalled %v %v", tc.signals, tc.signalNames)
	}
	if err := newTest(&memStore{}, &fakeTemporal{}).Cancel(context.Background(), creatorSession(), alice); !errors.Is(err, ErrNothingToStop) {
		t.Errorf("nothing running: %v", err)
	}
	s.background.Wait()
}

// Deleting a session, or its last member leaving it, ends its participants,
// and their turns with them.
func TestDeleteAndLeave_EndTheParticipants(t *testing.T) {
	running := map[string][]string{"ParticipantWorkflow": {sid + ":p:jarvis", sid + ":p:smith"}}
	st := &memStore{session: &store.Session{SessionID: sid, CreatedBy: "u-alice"}, members: []store.SessionMember{{UserID: "u-alice"}}}
	tc := &fakeTemporal{byType: running}
	s := newTest(st, tc)
	if err := s.Delete(context.Background(), sid, "u-alice"); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(tc.terminated) != fmt.Sprint(running["ParticipantWorkflow"]) {
		t.Errorf("delete ended %v", tc.terminated)
	}

	st = &memStore{session: &store.Session{SessionID: sid, CreatedBy: "u-alice"}}
	tc = &fakeTemporal{byType: running}
	s = newTest(st, tc)
	if err := s.Leave(context.Background(), sid, "u-alice"); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(tc.terminated) != fmt.Sprint(running["ParticipantWorkflow"]) {
		t.Errorf("the last member's leave ended %v", tc.terminated)
	}

	// A member leaving others behind ends nothing.
	st = &memStore{session: &store.Session{SessionID: sid, CreatedBy: "u-alice"}, members: []store.SessionMember{{UserID: "u-bob"}}}
	tc = &fakeTemporal{byType: running}
	s = newTest(st, tc)
	if err := s.Leave(context.Background(), sid, "u-alice"); err != nil || len(tc.terminated) != 0 {
		t.Errorf("leave with members left: %v, ended %v", err, tc.terminated)
	}
	s.background.Wait()
}

// A new session runs nothing until a message calls an agent.
func TestOpen_StartsNothing(t *testing.T) {
	st := &memStore{}
	tc := &fakeTemporal{}
	id, err := newTest(st, tc).Open(context.Background(), &store.User{ID: "u-alice"}, OpenOptions{})
	if err != nil || st.session == nil || st.session.SessionID != id || st.session.AgentID != "default" || st.session.Channel != ChannelWeb {
		t.Fatalf("Open = %q, %v; stored %+v", id, err, st.session)
	}
	if len(tc.started)+len(tc.signalStarts) != 0 {
		t.Errorf("started %v %v", tc.started, tc.signalStarts)
	}
}
