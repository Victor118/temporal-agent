package session

import (
	"cmp"
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

func turnEvent(typ, agentID, name string, message int64) activity.SSEEvent {
	data, _ := json.Marshal(workflow.TurnEvent{AgentID: agentID, AgentName: name, Turn: store.TurnKey(message, agentID)})
	return activity.SSEEvent{Type: typ, Data: data}
}

// names lists the working participants' agents, with their names.
func names(ws []Working) string {
	var out []string
	for _, w := range ws {
		out = append(out, w.AgentID+"="+w.Name)
	}
	return strings.Join(out, " ")
}

// The turn events tell, at once, what the visibility queries tell late: a
// participant that started a turn works, one done does not, whatever the
// queries still say; and a session works while one of its participants
// does, another one's end hiding nothing. Past a while the queries are
// right again, an event lost included; with no event at all (after a
// restart), they are all there is.
func TestStatuses_TurnEventsOutweighTheQueriesForAWhile(t *testing.T) {
	other := "0e1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b"
	tc := &fakeTemporal{byType: map[string][]string{
		"ParticipantWorkflow": {sid + ":p:default"}, // the queries lag: it ended
	}}
	s := newTest(&memStore{}, tc)
	now := time.Now()
	s.turns.now = func() time.Time { return now }
	ctx := context.Background()

	if got := s.Statuses(ctx)[sid]; got != StatusWorking {
		t.Fatalf("no event: %q, want the queries' working", got)
	}
	if got := names(s.WorkingAgents(ctx, sid)); got != "default=" {
		t.Errorf("no event: working %q, want the participant the queries see", got)
	}

	s.Observe(other, turnEvent(workflow.EventTurnStarted, "smith", "Agent Smith", 4))
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "default", "Jarvis", 2))
	s.Observe(sid, turnEvent(workflow.EventTurnDone, "default", "Jarvis", 2))
	statuses := s.Statuses(ctx)
	if cmp.Or(statuses[sid], StatusIdle) != StatusIdle || statuses[other] != StatusWorking {
		t.Errorf("after the events: %v", statuses)
	}
	if got := names(s.WorkingAgents(ctx, other)); got != "smith=Agent Smith" {
		t.Errorf("working %q", got)
	}
	if got := s.WorkingAgents(ctx, sid); len(got) != 0 {
		t.Errorf("a participant done still works: %+v", got)
	}

	// Two participants of one session: one done, the other still works.
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "default", "Jarvis", 5))
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "smith", "Agent Smith", 6))
	s.Observe(sid, turnEvent(workflow.EventTurnDone, "smith", "Agent Smith", 6))
	if got := s.Statuses(ctx)[sid]; got != StatusWorking {
		t.Errorf("smith done, jarvis working: %q", got)
	}
	if got := names(s.WorkingAgents(ctx, sid)); got != "default=Jarvis" {
		t.Errorf("working %q", got)
	}
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "smith", "Agent Smith", 7))
	if got := names(s.WorkingAgents(ctx, sid)); got != "default=Jarvis smith=Agent Smith" {
		t.Errorf("both at work: %q", got)
	}
	// A participant done with a message it was not on (one it skipped) is
	// still on its turn.
	s.Observe(sid, turnEvent(workflow.EventTurnDone, "smith", "", 8))
	if got := names(s.WorkingAgents(ctx, sid)); got != "default=Jarvis smith=Agent Smith" {
		t.Errorf("after a skipped message: %q", got)
	}

	// Past the trust: the queries again; a participant they see running is
	// named by its last turn.
	now = now.Add(turnTrust)
	statuses = s.Statuses(ctx)
	if statuses[sid] != StatusWorking || cmp.Or(statuses[other], StatusIdle) != StatusIdle {
		t.Errorf("past the trust: %v", statuses)
	}
	if got := names(s.WorkingAgents(ctx, sid)); got != "default=Jarvis" {
		t.Errorf("past the trust: %q", got)
	}

	// A question waits during a turn: waiting it stays.
	tc.byType["AskUserWorkflow"] = []string{other + ":p:smith:m4:tool:ask_user:c1"}
	s.Observe(other, turnEvent(workflow.EventTurnStarted, "smith", "", 9))
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
		s.Observe(topic, turnEvent(workflow.EventTurnStarted, "x", "", 1))
		if got := s.turns.sessions(); len(got) != 0 {
			t.Errorf("a turn recorded for %q, not a session: %v", topic, got)
		}
	}
	s.Observe(sid, activity.SSEEvent{Type: activity.EventToolCalls, Data: []byte(`{}`)})
	s.Statuses(ctx)
	if len(tc.lists) != 3 {
		t.Fatalf("%d queries: an event that changes nothing reloaded the statuses", len(tc.lists))
	}
	s.Observe(sid, turnEvent(workflow.EventTurnDone, "default", "", 1))
	s.Statuses(ctx)
	if len(tc.lists) != 6 {
		t.Errorf("%d queries after a turn event, want 6", len(tc.lists))
	}
	s.background.Wait()
}

func notice(participant, text string) activity.SSEEvent {
	data, _ := json.Marshal(map[string]string{"type": activity.EventNotice, "text": text, "agent": "Jarvis", "participant": participant})
	return activity.SSEEvent{Type: activity.EventNotice, Data: data}
}

// notes lists the working participants' notes.
func notes(ws []Working) string {
	var out []string
	for _, w := range ws {
		out = append(out, w.AgentID+":"+w.Note)
	}
	return strings.Join(out, " ")
}

// A notice sets what its participant's working turn waits for, an empty one
// clears it, and the participant's next turn starts without it. A notice is
// no state event: the statuses stay cached.
func TestObserve_NoticeSetsTheWorkingNote(t *testing.T) {
	tc := &fakeTemporal{}
	s := newTest(&memStore{}, tc)
	now := time.Now()
	s.statuses.now = func() time.Time { return now }
	ctx := context.Background()

	s.Observe(sid, notice("default", "waits"))
	if got := s.WorkingAgents(ctx, sid); len(got) != 0 {
		t.Errorf("a note with no turn known: %+v", got)
	}

	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "default", "", 1))
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "smith", "", 2))
	s.Statuses(ctx)
	queries := len(tc.lists)
	s.Observe(sid, notice("default", "Ton run attend un worker libre"))
	if got := notes(s.WorkingAgents(ctx, sid)); got != "default:Ton run attend un worker libre smith:" {
		t.Errorf("notes %q", got)
	}
	s.Statuses(ctx)
	if len(tc.lists) != queries {
		t.Errorf("a notice reloaded the statuses: %d queries, want %d", len(tc.lists), queries)
	}

	s.Observe(sid, notice("default", ""))
	if got := notes(s.WorkingAgents(ctx, sid)); got != "default: smith:" {
		t.Errorf("cleared, the notes are %q", got)
	}

	s.Observe(sid, notice("default", "again"))
	s.Observe(sid, activity.SSEEvent{Type: activity.EventNotice, Data: []byte(`not json`)})
	if got := notes(s.WorkingAgents(ctx, sid)); got != "default:again smith:" {
		t.Errorf("an unreadable notice changed the note: %q", got)
	}
	// A notice naming no participant is for those at work.
	s.Observe(sid, notice("", "all"))
	if got := notes(s.WorkingAgents(ctx, sid)); got != "default:all smith:all" {
		t.Errorf("notes %q", got)
	}
	s.Observe(sid, turnEvent(workflow.EventTurnDone, "default", "", 1))
	s.Observe(sid, notice("default", "late"))
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "default", "", 3))
	if got := notes(s.WorkingAgents(ctx, sid)); got != "default: smith:all" {
		t.Errorf("the next turn inherits the note: %q", got)
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
		s.Observe(sid, turnEvent(typ, "default", "", 1))
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
		s.Observe(sid, turnEvent(workflow.EventTurnStarted, "default", "", 1))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Observe waits for the members")
	}
	if got := names(s.turns.working(sid, nil)); got != "default=" {
		t.Errorf("the turn is not recorded before the ring: %q", got)
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
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "default", "", 1))
	s.background.Wait()
	if got := names(s.turns.working(sid, nil)); got != "default=" {
		t.Errorf("a failed ring lost the turn: %q", got)
	}
}

// A turn longer than the trust still names its agent, as long as the queries
// say its participant runs. One never seen ending is forgotten after a
// while, and with its session.
func TestWorkingAgents_PastTheTrust(t *testing.T) {
	st := &memStore{session: &store.Session{SessionID: sid, CreatedBy: "u-alice"}, members: []store.SessionMember{{UserID: "u-alice"}}}
	tc := &fakeTemporal{byType: map[string][]string{"ParticipantWorkflow": {sid + ":p:smith"}}}
	s := newTest(st, tc)
	now := time.Now()
	s.turns.now = func() time.Time { return now }
	s.statuses.now = func() time.Time { return now }
	ctx := context.Background()

	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "smith", "Agent Smith", 1))
	now = now.Add(turnTrust + time.Minute)
	if got := s.Statuses(ctx)[sid]; got != StatusWorking {
		t.Fatalf("past the trust, the queries: %q", got)
	}
	if got := names(s.WorkingAgents(ctx, sid)); got != "smith=Agent Smith" {
		t.Errorf("past the trust, a turn the queries see running: %q", got)
	}

	// Its end never came: forgotten, and dropped by the next event.
	now = now.Add(turnForget)
	if got := names(s.WorkingAgents(ctx, sid)); got != "smith=" {
		t.Errorf("a turn never seen ending still names its agent: %q", got)
	}
	other := "0e1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b"
	s.Observe(other, turnEvent(workflow.EventTurnStarted, "default", "", 1))
	if slices.Contains(s.turns.sessions(), sid) {
		t.Error("a forgotten turn is still held")
	}

	// A session deleted during a turn takes its turns along.
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "smith", "", 2))
	if err := s.Delete(ctx, sid, "u-alice"); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(s.turns.sessions(), sid) {
		t.Error("a deleted session's turns are still held")
	}
	s.background.Wait()
}

// A participant done with its turn does not work, however long the queries
// list it running: it runs on between two turns (an end written over a
// store away, a relay retried). A message delivered to it and not started
// makes it work again for the queries: its turn_started may be lost.
func TestWorkingAgents_DoneOutweighsTheQueries(t *testing.T) {
	tc := &fakeTemporal{byType: map[string][]string{"ParticipantWorkflow": {sid + ":p:default"}}}
	s := newTest(&memStore{}, tc)
	now := time.Now()
	s.turns.now = func() time.Time { return now }
	s.statuses.now = func() time.Time { return now }
	ctx := context.Background()

	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "default", "Jarvis", 1))
	s.Observe(sid, turnEvent(workflow.EventTurnDone, "default", "Jarvis", 1))
	now = now.Add(10 * time.Minute)
	if got := s.WorkingAgents(ctx, sid); len(got) != 0 {
		t.Errorf("done, listed running for 10 min: %+v, want none at work", got)
	}
	if got := s.Statuses(ctx)[sid]; cmp.Or(got, StatusIdle) != StatusIdle {
		t.Errorf("status %q", got)
	}

	s.turns.expect(sid, "default", 3)
	now = now.Add(turnTrust)
	if got := names(s.WorkingAgents(ctx, sid)); got != "default=" {
		t.Errorf("a message delivered and never seen started: %q, want the queries' word", got)
	}
	s.Observe(sid, turnEvent(workflow.EventTurnStarted, "default", "Jarvis", 3))
	s.Observe(sid, turnEvent(workflow.EventTurnDone, "default", "Jarvis", 3))
	now = now.Add(time.Hour)
	if got := s.WorkingAgents(ctx, sid); len(got) != 0 {
		t.Errorf("done again: %+v", got)
	}
	// Forgotten past a day: the queries alone again.
	now = now.Add(turnForget)
	s.Observe("0e1c2a9e-3b4d-4e5f-8a7b-0c1d2e3f4a5b", turnEvent(workflow.EventTurnStarted, "x", "", 1))
	if got := names(s.WorkingAgents(ctx, sid)); got != "default=" {
		t.Errorf("past a day: %q", got)
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
