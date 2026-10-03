package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

func turnEvent(typ, agentID, name string) activity.SSEEvent {
	data, _ := json.Marshal(workflow.TurnEvent{AgentID: agentID, AgentName: name, Turn: "k"})
	return activity.SSEEvent{Type: typ, Data: data}
}

// The turn events tell, at once, what the visibility queries tell late: a
// turn started is working, one over is not, whatever the queries still say.
// Past a while the queries are right again, an event lost included; with no
// event at all (after a restart), they are all there is.
func TestStatuses_TurnEventsOutweighTheQueriesForAWhile(t *testing.T) {
	other := "0e1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b"
	tc := &fakeTemporal{byType: map[string][]string{
		"SessionWorkflow": {"session-" + sid, "session-" + other},
		"AgentWorkflow":   {sid + "-turn-1"}, // the queries lag: the turn is over
	}}
	s := newTest(&memStore{}, tc)
	now := time.Now()
	s.turns.now = func() time.Time { return now }
	ctx := context.Background()

	if got := s.Statuses(ctx)[sid]; got != StatusWorking {
		t.Fatalf("no event: %q, want the queries' working", got)
	}
	if id, _ := s.WorkingAgent(sid); id != "" {
		t.Errorf("no event names %q", id)
	}

	s.Observe(other, turnEvent(workflow.EventTurnStarted, "smith", "Agent Smith"))
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "default", ""))
	s.Observe(sid, turnEvent(workflow.EventTurnDone, "default", ""))
	statuses := s.Statuses(ctx)
	if statuses[sid] != StatusActive || statuses[other] != StatusWorking {
		t.Errorf("after the events: %v", statuses)
	}
	if id, name := s.WorkingAgent(other); id != "smith" || name != "Agent Smith" {
		t.Errorf("working agent %q %q", id, name)
	}
	if id, _ := s.WorkingAgent(sid); id != "" {
		t.Errorf("a turn over still names %q", id)
	}
	if shared := s.statuses.get(ctx, s.loadStatuses); shared[sid] != StatusWorking || shared[other] != StatusActive {
		t.Errorf("the shared statuses were written to: %v", shared)
	}

	// Past the trust: the queries again (the started event's turn, never
	// seen ending, is no longer working).
	now = now.Add(turnTrust)
	if statuses := s.Statuses(ctx); statuses[sid] != StatusWorking || statuses[other] != StatusActive {
		t.Errorf("past the trust: %v", statuses)
	}

	// A question waits during a turn: waiting it stays.
	tc.byType["AskUserWorkflow"] = []string{other + "-tool-ask_user-1"}
	s.Observe(other, turnEvent(workflow.EventTurnStarted, "smith", ""))
	if got := s.Statuses(ctx)[other]; got != StatusWaiting {
		t.Errorf("a question during a turn: %q", got)
	}
}

// A turn event drops the cached statuses; an event of another topic than a
// session's is not one. The statuses' clock stands still: only an
// invalidation reloads them.
func TestObserve_TurnEventsInvalidate(t *testing.T) {
	tc := &fakeTemporal{}
	s := newTest(&memStore{}, tc)
	now := time.Now()
	s.statuses.now = func() time.Time { return now }
	ctx := context.Background()
	s.Statuses(ctx)
	for _, topic := range []string{"notifications:u1", TreeTopic("u1"), "admin", strings.ToUpper(sid)} {
		s.Observe(topic, turnEvent(workflow.EventTurnStarted, "x", ""))
		if _, ok := s.turns.get(topic); ok {
			t.Errorf("a turn recorded for %q, not a session", topic)
		}
	}
	s.Observe(sid, activity.SSEEvent{Type: activity.EventToolCalls, Data: []byte(`{}`)})
	s.Statuses(ctx)
	if len(tc.lists) != 4 {
		t.Fatalf("%d queries: an event that changes nothing reloaded the statuses", len(tc.lists))
	}
	s.Observe(sid, turnEvent(workflow.EventTurnDone, "default", ""))
	s.Statuses(ctx)
	if len(tc.lists) != 8 {
		t.Errorf("%d queries after a turn event, want 8", len(tc.lists))
	}
	s.background.Wait()
}

// A session's topic is its ID; nothing else is one.
func TestIsSessionTopic(t *testing.T) {
	if !IsSessionTopic(sid) {
		t.Errorf("%q is a session", sid)
	}
	for _, topic := range []string{"", "s1", "admin", "global", TreeTopic("u1"), "notifications:u1", strings.ToUpper(sid), "{" + sid + "}", "tree:" + sid} {
		if IsSessionTopic(topic) {
			t.Errorf("%q taken for a session", topic)
		}
	}
}

// An event that changes what a session is doing rings its members' trees;
// one that does not (a tool call), or one on another topic, does not. A
// member who leaves is rung too: their tree drops the session.
func TestTreesRing(t *testing.T) {
	st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}, {UserID: "u-bob"}}}
	s := newTest(st, &fakeTemporal{})
	hub := s.hub.(*nopHub)
	rung := func() string {
		s.background.Wait()
		return fmt.Sprint(len(hub.on(TreeTopic("u-alice"))), len(hub.on(TreeTopic("u-bob"))), len(hub.on(TreeTopic("u-carol"))))
	}

	s.Observe(sid, activity.SSEEvent{Type: activity.EventToolCalls, Data: []byte(`{}`)})
	s.Observe(TreeTopic("u-alice"), activity.SSEEvent{Type: EventTreeChanged})
	if got := rung(); got != "0 0 0" {
		t.Fatalf("rung %s for nothing", got)
	}
	for _, typ := range []string{workflow.EventTurnStarted, workflow.EventTurnDone, activity.EventAskUser, workflow.EventForkReady} {
		s.Observe(sid, turnEvent(typ, "default", ""))
	}
	if got := rung(); got != "4 4 0" {
		t.Errorf("rung %s, want each member's tree four times", got)
	}
	if err := s.Leave(context.Background(), sid, "u-carol"); err != nil || rung() != "5 5 1" {
		t.Errorf("leave: %v, rung %s", err, rung())
	}
	if ev := hub.on(TreeTopic("u-alice"))[0]; ev.Type != EventTreeChanged || string(ev.Data) != `{"session_id":"`+sid+`"}` {
		t.Errorf("event %s %s", ev.Type, ev.Data)
	}
}

// The trees ring off the publisher's way: a store slow to list the members
// holds neither the event nor what Observe must do first.
func TestObserve_RingsInTheBackground(t *testing.T) {
	gate := make(chan struct{})
	st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}, membersGate: gate}
	tc := &fakeTemporal{}
	s := newTest(st, tc)
	hub := s.hub.(*nopHub)
	done := make(chan struct{})
	go func() {
		s.Observe(sid, turnEvent(workflow.EventTurnStarted, "default", ""))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Observe waits for the members")
	}
	if id, _ := s.WorkingAgent(sid); id != "default" {
		t.Errorf("the turn is not recorded before the ring: %q", id)
	}
	if n := len(hub.on(TreeTopic("u-alice"))); n != 0 {
		t.Errorf("rung %d times before the members were read", n)
	}
	close(gate)
	s.background.Wait()
	if n := len(hub.on(TreeTopic("u-alice"))); n != 1 {
		t.Errorf("rung %d times, want 1", n)
	}
}

// Members that cannot be read ring no member's tree, but still the ones
// named: a member who just left.
func TestRingTrees_MembersUnread(t *testing.T) {
	st := &memStore{members: []store.SessionMember{{UserID: "u-alice"}}, membersErr: errors.New("db down")}
	s := newTest(st, &fakeTemporal{})
	hub := s.hub.(*nopHub)
	s.ringTrees(context.Background(), sid, "u-carol")
	if a, c := len(hub.on(TreeTopic("u-alice"))), len(hub.on(TreeTopic("u-carol"))); a != 0 || c != 1 {
		t.Errorf("rung alice %d, carol %d times", a, c)
	}
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "default", ""))
	s.background.Wait()
	if id, _ := s.WorkingAgent(sid); id != "default" {
		t.Errorf("a failed ring lost the turn: %q", id)
	}
}

// A turn longer than the trust still names its agent, as long as the queries
// say a turn runs. One never seen ending is forgotten after a while, and with
// its session.
func TestWorkingAgent_PastTheTrust(t *testing.T) {
	st := &memStore{session: &store.Session{SessionID: sid, CreatedBy: "u-alice"}, members: []store.SessionMember{{UserID: "u-alice"}}}
	tc := &fakeTemporal{byType: map[string][]string{
		"SessionWorkflow": {"session-" + sid},
		"AgentWorkflow":   {sid + "-turn-1"},
	}}
	s := newTest(st, tc)
	now := time.Now()
	s.turns.now = func() time.Time { return now }
	s.statuses.now = func() time.Time { return now }
	ctx := context.Background()

	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "smith", "Agent Smith"))
	now = now.Add(turnTrust + time.Minute)
	if got := s.Statuses(ctx)[sid]; got != StatusWorking {
		t.Fatalf("past the trust, the queries: %q", got)
	}
	if id, name := s.WorkingAgent(sid); id != "smith" || name != "Agent Smith" {
		t.Errorf("past the trust, a turn the queries see running: %q %q", id, name)
	}

	// Its end never came: forgotten, and dropped by the next event.
	now = now.Add(turnForget)
	if id, _ := s.WorkingAgent(sid); id != "" {
		t.Errorf("a turn never seen ending still names %q", id)
	}
	other := "0e1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b"
	s.Observe(other, turnEvent(workflow.EventTurnStarted, "default", ""))
	s.turns.mu.Lock()
	_, kept := s.turns.m[sid]
	s.turns.mu.Unlock()
	if kept {
		t.Error("a forgotten turn is still held")
	}

	// A session deleted during a turn takes its turn along.
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "smith", ""))
	if err := s.Delete(ctx, sid, "u-alice"); err != nil {
		t.Fatal(err)
	}
	if id, _ := s.WorkingAgent(sid); id != "" {
		t.Errorf("a deleted session's turn still names %q", id)
	}
	s.background.Wait()
}

// Members out of a session are told on its topic: its streams end theirs.
func TestMembersLeft_Published(t *testing.T) {
	st := &memStore{session: &store.Session{SessionID: sid, CreatedBy: "u-alice"},
		members: []store.SessionMember{{UserID: "u-alice"}, {UserID: "u-bob"}}}
	s := newTest(st, &fakeTemporal{})
	hub := s.hub.(*nopHub)
	ctx := context.Background()
	if err := s.Leave(ctx, sid, "u-bob"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, sid, "u-alice"); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, ev := range hub.on(sid) {
		if ev.Type == EventMemberLeft {
			got = append(got, string(ev.Data))
		}
	}
	if want := []string{`{"user_ids":["u-bob"]}`, `{"user_ids":["u-alice","u-bob"]}`}; !slices.Equal(got, want) {
		t.Errorf("member_left events %q, want %q", got, want)
	}
}
