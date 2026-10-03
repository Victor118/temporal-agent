package session

import (
	"encoding/json"
	"log"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/workflow"
)

// turnTrust is how long a turn event outweighs the visibility queries.
// Those lag behind the workflows by a moment: right after a turn ends they
// may still list it running. Past that moment they are right, and they also
// cover an event that never arrived.
const turnTrust = 30 * time.Second

// turn is what the server knows of a session's agent turn, from the turn
// events as they pass through its hub. It is held in memory: after a
// restart, the server knows nothing, and the visibility queries tell.
type turn struct {
	working bool
	event   workflow.TurnEvent
	at      time.Time // when the event came
}

// turns are the sessions' turns, by session ID. The zero value is ready to
// use.
type turns struct {
	mu sync.Mutex
	m  map[string]turn
	// now is the clock; nil = time.Now. Tests set it.
	now func() time.Time
}

func (t *turns) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// set records a turn event. A turn over and no longer trusted tells nothing
// more than the visibility queries: it is dropped.
func (t *turns) set(sessionID string, working bool, e workflow.TurnEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	for id, old := range t.m {
		if !old.working && now.Sub(old.at) >= turnTrust {
			delete(t.m, id)
		}
	}
	if t.m == nil {
		t.m = map[string]turn{}
	}
	t.m[sessionID] = turn{working: working, event: e, at: now}
}

// get returns the session's turn, if the server heard of one.
func (t *turns) get(sessionID string) (turn, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tr, ok := t.m[sessionID]
	return tr, ok
}

// overlay corrects statuses read from the visibility queries with the turn
// events still trusted: a turn started is working (or waiting on a
// question); a turn over is not, its session's workflow still running. It
// returns statuses itself when there is nothing to correct, a copy
// otherwise: statuses is shared.
func (t *turns) overlay(statuses map[string]Status) map[string]Status {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	var out map[string]Status
	for id, tr := range t.m {
		if now.Sub(tr.at) >= turnTrust {
			continue
		}
		want := statuses[id]
		switch {
		case tr.working && want != StatusWaiting:
			want = StatusWorking
		case !tr.working && (want == StatusWorking || want == StatusWaiting):
			want = StatusActive
		}
		if want == statuses[id] {
			continue
		}
		if out == nil {
			out = maps.Clone(statuses)
			if out == nil {
				out = map[string]Status{}
			}
		}
		out[id] = want
	}
	if out == nil {
		return statuses
	}
	return out
}

// WorkingAgent is the agent on a turn in the session, as the turn events
// tell: its ID, and its name when the session's message named it (empty for
// the session's own agent, which the caller names). Empty when no turn
// event says one is working.
func (s *Service) WorkingAgent(sessionID string) (id, name string) {
	tr, ok := s.turns.get(sessionID)
	if !ok || !tr.working {
		return "", ""
	}
	return tr.event.AgentID, tr.event.AgentName
}

// Observe learns from an event published on the server's hub, before the
// pages it rings reload: the turn events feed the sessions' turns, and an
// event that changes what a session is doing drops the cached statuses.
func (s *Service) Observe(topic string, ev activity.SSEEvent) {
	if strings.Contains(topic, ":") { // not a session: a user's notifications, a tree
		return
	}
	switch ev.Type {
	case workflow.EventTurnStarted, workflow.EventTurnDone:
		var e workflow.TurnEvent
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			log.Printf("session %s: %s: %v", topic, ev.Type, err)
			return
		}
		s.turns.set(topic, ev.Type == workflow.EventTurnStarted, e)
		s.statuses.invalidate()
	case "ask_user", workflow.EventForkReady, workflow.EventForkFailed:
		s.statuses.invalidate()
	}
}
