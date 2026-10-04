package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

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
		"the creator, any turn, unread": {me: alice, turn: "m4.jarvis", stopped: jarvis + "=m4.jarvis"},
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
	if err := newTest(&memStore{}, tc).StopTurn(ctx, creatorSession(), "jarvis:m4:tool:ask_user:1", "", alice); !errors.Is(err, ErrNothingToStop) || len(tc.signals) != 0 {
		t.Errorf("StopTurn on a forged name: %v, signalled %v", err, tc.signals)
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
