package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

var (
	alice = &store.User{ID: "u-alice"}
	bob   = &store.User{ID: "u-bob"}
)

// creatorSession is a session Alice created.
func creatorSession() *store.Session {
	return &store.Session{SessionID: sid, CreatedBy: alice.ID}
}

// answering is a participant's state on a member's message.
func answering(agentID string, message int64, userID string) workflow.ParticipantState {
	return workflow.ParticipantState{Current: &workflow.CurrentMessage{
		MessageID: message, Turn: store.TurnKey(message, agentID), UserID: userID,
	}, Background: []string{}}
}

// startedBy is a turn_started event on a member's message.
func startedBy(agentID, name string, message int64, user *store.User, userName string) activity.SSEEvent {
	data, _ := json.Marshal(workflow.TurnEvent{AgentID: agentID, AgentName: name, Turn: store.TurnKey(message, agentID), UserID: user.ID, UserName: userName})
	return activity.SSEEvent{Type: workflow.EventTurnStarted, Data: data}
}

// stopsSent are the stop-turn signals sent: participant workflow ID = turn
// named.
func stopsSent(tc *fakeTemporal) []string {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	var out []string
	for i, name := range tc.signalNames {
		if name == workflow.SignalStopTurn {
			out = append(out, fmt.Sprintf("%s=%s", tc.signals[i], tc.signalArgs[i].(workflow.StopTurn).TurnKey))
		}
	}
	return out
}

// A turn is stopped by the author of the message it answers, or by the
// session's creator; by no other member. The author is read from the
// participant at the click, not from what the page showed: a turn that
// changed is not stopped, and the stop names the turn it is for.
func TestStopTurn_Rights(t *testing.T) {
	jarvis := sid + ":p:jarvis"
	ctx := context.Background()
	for name, c := range map[string]struct {
		me      *store.User
		state   *workflow.ParticipantState // nil: not running
		turn    string
		err     error
		stopped string // the stop sent, "" for none
		queried bool
	}{
		"the creator, another's turn":   {me: alice, state: ptr(answering("jarvis", 4, "u-carol")), turn: "m4.jarvis", stopped: jarvis + "=m4.jarvis", queried: true},
		"the creator, the turn changed": {me: alice, state: ptr(answering("jarvis", 6, bob.ID)), turn: "m4.jarvis", err: ErrTurnOver, queried: true},
		"the creator, nothing runs":     {me: alice, turn: "m4.jarvis", err: ErrTurnOver, queried: true},
		"the creator, whichever runs":   {me: alice, stopped: jarvis + "="},
		"the author":                    {me: bob, state: ptr(answering("jarvis", 4, bob.ID)), turn: "m4.jarvis", stopped: jarvis + "=m4.jarvis", queried: true},
		"the author, no turn named":     {me: bob, state: ptr(answering("jarvis", 4, bob.ID)), stopped: jarvis + "=m4.jarvis", queried: true},
		"another member's turn":         {me: bob, state: ptr(answering("jarvis", 4, "u-carol")), turn: "m4.jarvis", err: ErrStopNotAllowed, queried: true},
		"the turn changed":              {me: bob, state: ptr(answering("jarvis", 6, bob.ID)), turn: "m4.jarvis", err: ErrTurnOver, queried: true},
		"between two turns":             {me: bob, state: &workflow.ParticipantState{Queued: 1}, turn: "m4.jarvis", err: ErrTurnOver, queried: true},
		"not running":                   {me: bob, err: ErrNothingToStop, queried: true},
	} {
		t.Run(name, func(t *testing.T) {
			tc := &fakeTemporal{states: map[string]interface{}{}}
			if c.state != nil {
				tc.states[jarvis] = *c.state
			}
			s := newTest(&memStore{}, tc)
			if err := s.StopTurn(ctx, creatorSession(), "jarvis", c.turn, c.me); !errors.Is(err, c.err) {
				t.Fatalf("StopTurn = %v, want %v", err, c.err)
			}
			if got := fmt.Sprint(stopsSent(tc)); got != fmt.Sprint(nonEmpty(c.stopped)) {
				t.Errorf("stops %s, want %q", got, c.stopped)
			}
			if (len(tc.queried) > 0) != c.queried {
				t.Errorf("queried %v, want %v", tc.queried, c.queried)
			}
		})
	}

	// A name that is no participant's names no workflow of the session.
	tc := &fakeTemporal{}
	s := newTest(&memStore{}, tc)
	if err := s.StopTurn(ctx, creatorSession(), "jarvis:m4:tool:ask_user:1", "", alice); !errors.Is(err, ErrNothingToStop) || len(tc.signals) != 0 {
		t.Errorf("StopTurn on a forged name: %v, signalled %v", err, tc.signals)
	}
	for _, forged := range []string{"jarvis:m4:tool:ask_user:1", ""} {
		if err := s.Clear(ctx, creatorSession(), forged, alice); !errors.Is(err, ErrNothingToStop) || len(tc.signals) != 0 {
			t.Errorf("Clear on %q: %v, signalled %v", forged, err, tc.signals)
		}
	}
}

func ptr[T any](v T) *T { return &v }

func nonEmpty(s string) []string {
	if s == "" {
		return nil
	}
	return []string{s}
}

// Dropping a participant's waiting messages drops other members' too: the
// session's creator alone may.
func TestClear_CreatorOnly(t *testing.T) {
	tc := &fakeTemporal{}
	s := newTest(&memStore{}, tc)
	if err := s.Clear(context.Background(), creatorSession(), "jarvis", bob); !errors.Is(err, ErrClearNotAllowed) || len(tc.signals) != 0 {
		t.Errorf("a member cleared: %v, %v", err, tc.signals)
	}
	if err := s.Clear(context.Background(), creatorSession(), "jarvis", alice); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(tc.signals, tc.signalNames) != fmt.Sprintf("[%s:p:jarvis] [clear]", sid) {
		t.Errorf("signalled %v %v", tc.signals, tc.signalNames)
	}
}

// Arrêter, for the session's creator, stops every turn, whichever; for
// another member, the turns answering their messages alone, each stop
// naming its turn. A member with no turn of theirs running is refused.
func TestCancel_Rights(t *testing.T) {
	jarvis, smith := sid+":p:jarvis", sid+":p:smith"
	running := map[string][]string{"ParticipantWorkflow": {jarvis, smith}}
	states := map[string]interface{}{jarvis: answering("jarvis", 4, bob.ID), smith: answering("smith", 5, "u-carol")}
	ctx := context.Background()

	tc := &fakeTemporal{byType: running, states: states}
	if err := newTest(&memStore{}, tc).Cancel(ctx, creatorSession(), alice); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(stopsSent(tc)); got != fmt.Sprintf("[%s= %s=]", jarvis, smith) || len(tc.queried) != 0 {
		t.Errorf("the creator's stops %s, queried %v", got, tc.queried)
	}

	tc = &fakeTemporal{byType: running, states: states}
	if err := newTest(&memStore{}, tc).Cancel(ctx, creatorSession(), bob); err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(stopsSent(tc)); got != fmt.Sprintf("[%s=m4.jarvis]", jarvis) {
		t.Errorf("bob's stops %s", got)
	}

	tc = &fakeTemporal{byType: running, states: states}
	if err := newTest(&memStore{}, tc).Cancel(ctx, creatorSession(), &store.User{ID: "u-dave"}); !errors.Is(err, ErrStopNotAllowed) || len(tc.signals) != 0 {
		t.Errorf("dave: %v, signalled %v", err, tc.signals)
	}

	// A participant that cannot be read is left running: not an error when
	// another stop went, one when none did.
	tc = &fakeTemporal{byType: running, states: states, queryErrs: map[string]error{smith: errors.New("no worker")}}
	if err := newTest(&memStore{}, tc).Cancel(ctx, creatorSession(), bob); err != nil || fmt.Sprint(stopsSent(tc)) != fmt.Sprintf("[%s=m4.jarvis]", jarvis) {
		t.Errorf("bob, smith unreadable: %v, stops %v", err, stopsSent(tc))
	}
	tc = &fakeTemporal{byType: running, states: states, queryErrs: map[string]error{jarvis: errors.New("no worker")}}
	if err := newTest(&memStore{}, tc).Cancel(ctx, creatorSession(), bob); err == nil || errors.Is(err, ErrStopNotAllowed) || len(tc.signals) != 0 {
		t.Errorf("bob, his turn unreadable: %v, signalled %v", err, tc.signals)
	}
}

// The Agents panel's participants: the turn events tell of those they know,
// working or not, with whom they answer and since when; one only the
// queries see running answers its state query, once per statusesTTL; a
// question waiting marks its participant.
func TestParticipants(t *testing.T) {
	jarvis, smith := sid+":p:jarvis", sid+":p:smith"
	tc := &fakeTemporal{
		byType: map[string][]string{
			"ParticipantWorkflow": {jarvis, smith},
			"AskUserWorkflow":     {jarvis + ":m4:tool:ask_user:c1"},
		},
		states: map[string]interface{}{smith: workflow.ParticipantState{
			Current: &workflow.CurrentMessage{MessageID: 5, Turn: "m5.smith", UserID: "u-carol", UserName: "Carol"},
			Queued:  2, Background: []string{"analyse"},
		}},
	}
	s := newTest(&memStore{}, tc)
	now := time.Now()
	s.turns.now = func() time.Time { return now }
	s.Observe(sid, startedBy("jarvis", "Jarvis", 4, bob, "Bob"))
	s.turns.expect(sid, "jarvis", 7)
	s.Observe(sid, startedBy("watson", "Watson", 2, bob, "Bob"))
	s.Observe(sid, done("watson", 2))
	ctx := context.Background()

	ps := s.Participants(ctx, sid)
	if len(ps) != 3 {
		t.Fatalf("participants %+v", ps)
	}
	j, sm, w := ps[0], ps[1], ps[2]
	if !j.Working || j.Name != "Jarvis" || j.Turn != "m4.jarvis" || j.UserID != bob.ID || j.UserName != "Bob" || !j.Since.Equal(now) || j.Queued != 1 || !j.Waiting {
		t.Errorf("jarvis %+v", j)
	}
	if !sm.Working || sm.Turn != "m5.smith" || sm.UserName != "Carol" || sm.Queued != 2 || fmt.Sprint(sm.Background) != "[analyse]" || sm.Waiting {
		t.Errorf("smith %+v", sm)
	}
	if w.Participant != "watson" || w.Working || w.Turn != "" {
		t.Errorf("watson %+v", w)
	}
	if fmt.Sprint(tc.queried) != fmt.Sprintf("[%s]", smith) {
		t.Errorf("queried %v, want smith alone", tc.queried)
	}
	s.Participants(ctx, sid)
	if len(tc.queried) != 1 {
		t.Errorf("queried again within the TTL: %v", tc.queried)
	}

	// Between two turns, as its query says: not working.
	tc.states[smith] = workflow.ParticipantState{Background: []string{}}
	s.statuses.invalidate()
	if ps := s.Participants(ctx, sid); ps[1].Working {
		t.Errorf("smith between turns %+v", ps[1])
	}
	s.background.Wait()
}

// A message left pending by events lost does not wait once its participant
// has ended: it is counted while the participant runs, or for a moment
// after its delivery.
func TestParticipants_AStalePendingMessage(t *testing.T) {
	tc := &fakeTemporal{}
	s := newTest(&memStore{}, tc)
	now := time.Now()
	s.turns.now = func() time.Time { return now }
	s.turns.expect(sid, "jarvis", 3)
	ctx := context.Background()
	if ps := s.Participants(ctx, sid); len(ps) != 1 || ps[0].Queued != 1 {
		t.Errorf("just delivered: %+v", ps)
	}
	now = now.Add(time.Hour)
	s.statuses.invalidate()
	if ps := s.Participants(ctx, sid); len(ps) != 1 || ps[0].Queued != 0 || ps[0].Working {
		t.Errorf("an hour later, the participant ended: %+v", ps)
	}
	tc.mu.Lock()
	tc.byType = map[string][]string{"ParticipantWorkflow": {sid + ":p:jarvis"}}
	tc.mu.Unlock()
	s.statuses.invalidate()
	if ps := s.Participants(ctx, sid); len(ps) != 1 || ps[0].Queued != 1 || !ps[0].Working {
		t.Errorf("still running: %+v", ps)
	}
	s.background.Wait()
}

// A participant's state is asked once per TTL, by one query at a time: a
// request for one being read waits for that reading. Not found (it ended)
// and failed answers are kept like the others; one cut short by its
// request going away is not.
func TestStatusCache_ParticipantState(t *testing.T) {
	var c statusCache
	now := time.Now()
	c.now = func() time.Time { return now }
	ctx := context.Background()
	var mu sync.Mutex
	queries := 0
	var answer func(context.Context) (workflow.ParticipantState, error)
	query := func(ctx context.Context, _ string) (workflow.ParticipantState, error) {
		mu.Lock()
		queries++
		mu.Unlock()
		return answer(ctx)
	}
	count := func() int { mu.Lock(); defer mu.Unlock(); return queries }

	answer = func(context.Context) (workflow.ParticipantState, error) {
		return workflow.ParticipantState{}, serviceerror.NewNotFound("gone")
	}
	if q := c.participantState(ctx, "p1", query); !q.gone || q.ok {
		t.Errorf("not found: %+v", q)
	}
	answer = func(context.Context) (workflow.ParticipantState, error) {
		return workflow.ParticipantState{}, errors.New("no worker")
	}
	if q := c.participantState(ctx, "p1", query); !q.gone || count() != 1 {
		t.Errorf("within the TTL: %+v, %d queries", q, count())
	}
	if q := c.participantState(ctx, "p2", query); q.ok || q.gone || count() != 2 {
		t.Errorf("failed: %+v", q)
	}
	c.participantState(ctx, "p2", query)
	if count() != 2 {
		t.Errorf("a failure asked again within the TTL: %d queries", count())
	}

	// Cut short by its request: not kept.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	answer = func(ctx context.Context) (workflow.ParticipantState, error) {
		return workflow.ParticipantState{}, ctx.Err()
	}
	c.participantState(cancelled, "p3", query)
	answer = func(context.Context) (workflow.ParticipantState, error) {
		return workflow.ParticipantState{Queued: 3}, nil
	}
	if q := c.participantState(ctx, "p3", query); !q.ok || q.state.Queued != 3 || count() != 4 {
		t.Errorf("after a reading cut short: %+v, %d queries", q, count())
	}

	// Two requests for one participant: one query.
	gate := make(chan struct{})
	answer = func(context.Context) (workflow.ParticipantState, error) {
		<-gate
		return workflow.ParticipantState{Queued: 1}, nil
	}
	var wg sync.WaitGroup
	got := make([]queriedState, 2)
	for i := range got {
		wg.Add(1)
		go func() { defer wg.Done(); got[i] = c.participantState(ctx, "p4", query) }()
	}
	for count() != 5 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond) // the second request waits on the first
	close(gate)
	wg.Wait()
	if count() != 5 || !got[0].ok || !got[1].ok || got[1].state.Queued != 1 {
		t.Errorf("two requests: %d queries, %+v", count(), got)
	}

	c.invalidate()
	c.participantState(ctx, "p4", query)
	if count() != 6 {
		t.Errorf("after an invalidation: %d queries", count())
	}
}

// A participant the events leave unclear answers its state query: one
// running that no event told of, one working on a turn no event named. One
// Temporal knows no more has ended: idle if the events told of it, not
// shown otherwise.
func TestParticipants_StateQueriedWhenUnclear(t *testing.T) {
	jarvis, smith, watson := sid+":p:jarvis", sid+":p:smith", sid+":p:watson"
	tc := &fakeTemporal{
		byType: map[string][]string{"ParticipantWorkflow": {jarvis, smith, watson}},
		states: map[string]interface{}{jarvis: answering("jarvis", 3, bob.ID)},
		queryErrs: map[string]error{
			smith:  serviceerror.NewNotFound("ended"),
			watson: serviceerror.NewNotFound("ended"),
		},
	}
	s := newTest(&memStore{}, tc)
	s.turns.expect(sid, "jarvis", 3) // delivered, its turn_started lost
	s.turns.expect(sid, "watson", 5)
	ps := s.Participants(context.Background(), sid)
	if len(ps) != 2 {
		t.Fatalf("participants %+v, want smith left out", ps)
	}
	if j := ps[0]; !j.Working || j.Turn != "m3.jarvis" || j.UserID != bob.ID {
		t.Errorf("jarvis %+v", j)
	}
	if w := ps[1]; w.Participant != "watson" || w.Working || w.Queued != 0 {
		t.Errorf("watson %+v", w)
	}
	s.background.Wait()
}

// The visibility queries unread (the request went away first): the panel
// shows what the events tell.
func TestParticipants_VisibilityUnread(t *testing.T) {
	tc := &fakeTemporal{listGate: make(chan struct{})}
	defer close(tc.listGate)
	s := newTest(&memStore{}, tc)
	s.Observe(sid, startedBy("jarvis", "Jarvis", 4, bob, "Bob"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ps := s.Participants(ctx, sid)
	if len(ps) != 1 || !ps[0].Working || ps[0].Turn != "m4.jarvis" || ps[0].Waiting {
		t.Errorf("participants %+v", ps)
	}
}
