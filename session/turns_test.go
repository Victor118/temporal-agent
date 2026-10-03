package session

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/victor/temporal-agent/activity"
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
// session's is not one.
func TestObserve_TurnEventsInvalidate(t *testing.T) {
	tc := &fakeTemporal{}
	s := newTest(&memStore{}, tc)
	ctx := context.Background()
	s.Statuses(ctx)
	s.Observe("notifications:u1", turnEvent(workflow.EventTurnStarted, "x", ""))
	s.Observe(sid, activity.SSEEvent{Type: "tool_calls", Data: []byte(`{}`)})
	s.Statuses(ctx)
	if len(tc.lists) != 4 {
		t.Fatalf("%d queries: an event that changes nothing reloaded the statuses", len(tc.lists))
	}
	if _, ok := s.turns.get("notifications:u1"); ok {
		t.Error("a turn recorded for a topic that is not a session")
	}
	s.Observe(sid, turnEvent(workflow.EventTurnDone, "default", ""))
	s.Statuses(ctx)
	if len(tc.lists) != 8 {
		t.Errorf("%d queries after a turn event, want 8", len(tc.lists))
	}
}
