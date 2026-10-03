package session

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

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

	tc := &fakeTemporal{running: []string{sid + "-tool-ask_user-1"}}
	if newTest(&memStore{}, tc).AnswerPending(context.Background(), "x' OR '1'='1", "yes") || len(tc.lists) != 0 {
		t.Errorf("a forged session ID reached Temporal: %v", tc.lists)
	}
}

// An answer from a channel without buttons reaches a question a sub-agent
// asked: they are found by type, not by the session agent's own ID prefix.
func TestAnswerPending_FindsSubAgentQuestions(t *testing.T) {
	question := sid + "-tool-agent_analyst-c1-tool-ask_user-c2"
	tc := &fakeTemporal{running: []string{question}}
	if !newTest(&memStore{}, tc).AnswerPending(context.Background(), sid, "yes") {
		t.Fatal("the answer was not delivered")
	}
	if len(tc.lists) != 1 || !strings.Contains(tc.lists[0], "WorkflowType = 'AskUserWorkflow'") || !strings.Contains(tc.lists[0], "STARTS_WITH '"+sid+"-'") {
		t.Errorf("query %v", tc.lists)
	}
	if len(tc.signals) != 1 || tc.signals[0] != question {
		t.Errorf("signalled %v", tc.signals)
	}
}

// Answering through a session reaches that session's questions only.
func TestAnswer_OwnQuestionsOnly(t *testing.T) {
	tc := &fakeTemporal{}
	s := newTest(&memStore{}, tc)
	if err := s.Answer(context.Background(), sid, "other-tool-ask_user-1", "yes"); !errors.Is(err, ErrForeignQuestion) {
		t.Errorf("another session's question: %v", err)
	}
	if err := s.Answer(context.Background(), sid, sid+"-tool-ask_user-1", "  "); !errors.Is(err, ErrEmptyAnswer) {
		t.Errorf("an empty answer: %v", err)
	}
	if err := s.Answer(context.Background(), sid, sid+"-tool-ask_user-1", "yes"); err != nil || len(tc.signals) != 1 {
		t.Errorf("own question: %v, %v", err, tc.signals)
	}
}

// Every tab refreshes the tree: the states are read from Temporal once for
// all of them, and again after an action changes them.
func TestStatuses_SharedForAFewSeconds(t *testing.T) {
	tc := &fakeTemporal{running: []string{sid + "-turn-1"}}
	s := newTest(&memStore{}, tc)
	for i := 0; i < 10; i++ {
		s.Statuses(context.Background())
	}
	if len(tc.lists) != 4 {
		t.Errorf("%d visibility queries for 10 reads, want 4", len(tc.lists))
	}
	// The fake answers every query with the same workflow: the strongest
	// state, a question waiting, wins.
	if got := s.Statuses(context.Background())[sid]; got != StatusWaiting {
		t.Errorf("status %q, want waiting", got)
	}
	s.statuses.invalidate()
	s.Statuses(context.Background())
	if len(tc.lists) != 8 {
		t.Errorf("%d visibility queries after an invalidation, want 8", len(tc.lists))
	}
}

// The request that starts a load and goes away does not cancel it: the
// others share that load, and get what it read.
func TestStatusCache_LoadOutlivesTheRequest(t *testing.T) {
	var c statusCache
	release := make(chan struct{})
	var loadErr error
	loads := 0
	load := func(ctx context.Context) map[string]Status {
		loads++
		<-release
		loadErr = ctx.Err()
		return map[string]Status{sid: StatusWorking}
	}

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan map[string]Status)
	go func() { got <- c.get(ctx, load) }()
	cancel()
	if v := <-got; v != nil {
		t.Errorf("a cancelled request got %v, want nil", v)
	}

	close(release)
	if v := c.get(context.Background(), load); v[sid] != StatusWorking {
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
	c := statusCache{value: map[string]Status{sid: StatusActive}, at: time.Now().Add(-time.Minute)}
	release := make(chan struct{})
	load := func(context.Context) map[string]Status {
		<-release
		return map[string]Status{sid: StatusWaiting}
	}

	if v := c.get(context.Background(), load); v[sid] != StatusActive {
		t.Errorf("during the reload: %v, want the previous states", v)
	}
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for c.get(context.Background(), load)[sid] != StatusWaiting {
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
	load := func(context.Context) map[string]Status {
		if loads.Add(1) == 1 {
			close(started)
			<-release
			return map[string]Status{sid: StatusActive}
		}
		return map[string]Status{sid: StatusIdle}
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

	if v := c.get(context.Background(), load); v[sid] != StatusIdle {
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
}

// A message to a session whose run timed out starts a new run on the
// session's fixed ID, in the same call that signals it, with the input the
// session had: its agent and its channel. The message carries the ID it was
// stored under: its turns read the session up to it.
func TestDeliver_SignalsWithStartOnTheFixedID(t *testing.T) {
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
	if got.id != "session-"+sid || got.options.ID != got.id || got.options.TaskQueue != "agent" || got.signal != workflow.SignalUserMessage {
		t.Errorf("signal-with-start %q, options %+v, signal %q", got.id, got.options, got.signal)
	}
	if msg, ok := got.arg.(workflow.UserMessage); !ok || msg.Text != "bonjour" || msg.UserID != "u-alice" || !msg.Stored || msg.MessageID != 1 {
		t.Errorf("message %+v", got.arg)
	}
	want := workflow.SessionWorkflowInput{SessionID: sid, AgentID: "analyst", Channel: "telegram", ChannelID: "42"}
	if len(got.input) != 1 || got.input[0] != want {
		t.Errorf("input %+v, want %+v", got.input, want)
	}
}

// The state is the running run's, a resumed session's included; with none
// running, the last run's: every run has the session's fixed ID.
func TestState_FindsTheSession(t *testing.T) {
	base := "session-" + sid
	for name, tc := range map[string]*fakeTemporal{
		"resumed":      {running: []string{base}},
		"none running": {},
	} {
		t.Run(name, func(t *testing.T) {
			want := base
			tc.states = map[string]workflow.SessionState{want: {SessionID: sid, Status: "idle", TurnCount: 3}}
			got, err := newTest(&memStore{}, tc).State(context.Background(), sid)
			if err != nil || got.TurnCount != 3 {
				t.Errorf("State = %+v, %v; queried %v", got, err, tc.queried)
			}
		})
	}
}
