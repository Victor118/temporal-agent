package session

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"slices"
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

// turnForget is how long a turn never seen ending is remembered: its end was
// lost (the server away when it came), or its session deleted during it. No
// turn runs that long; until then, the visibility queries tell whether it
// does.
const turnForget = 24 * time.Hour

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
// more than the visibility queries, nor does one forgotten: they are
// dropped.
func (t *turns) set(sessionID string, working bool, e workflow.TurnEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	for id, old := range t.m {
		if age := now.Sub(old.at); !old.working && age >= turnTrust || age >= turnForget {
			delete(t.m, id)
		}
	}
	if t.m == nil {
		t.m = map[string]turn{}
	}
	t.m[sessionID] = turn{working: working, event: e, at: now}
}

// get returns the session's turn, if the server heard of one not forgotten.
func (t *turns) get(sessionID string) (turn, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tr, ok := t.m[sessionID]
	if ok && t.clock().Sub(tr.at) >= turnForget {
		return turn{}, false
	}
	return tr, ok
}

// forget drops the session's turn: the session is gone.
func (t *turns) forget(sessionID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, sessionID)
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
// event says one is working. Past turnTrust it still names the agent, as
// long as the caller's status says a turn runs: a turn may last minutes,
// and the next one's start replaces it. A turn never seen ending is
// forgotten after turnForget.
func (s *Service) WorkingAgent(sessionID string) (id, name string) {
	tr, ok := s.turns.get(sessionID)
	if !ok || !tr.working {
		return "", ""
	}
	return tr.event.AgentID, tr.event.AgentName
}

// Observe learns from an event published on the server's hub, before the
// pages it rings reload: the turn events feed the sessions' turns; an event
// that changes what a session is doing (StateEvents) drops the cached
// statuses, and rings its members' trees. It runs on the publisher's way:
// the rings, which read the members, go in the background.
func (s *Service) Observe(topic string, ev activity.SSEEvent) {
	if !IsSessionTopic(topic) || !slices.Contains(StateEvents, ev.Type) {
		return
	}
	if ev.Type == workflow.EventTurnStarted || ev.Type == workflow.EventTurnDone {
		var e workflow.TurnEvent
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			log.Printf("session %s: %s: %v", topic, ev.Type, err)
			return
		}
		s.turns.set(topic, ev.Type == workflow.EventTurnStarted, e)
	}
	s.statuses.invalidate()
	s.inBackground(func() { s.ringTrees(context.Background(), topic) })
}

// inBackground runs f on its own goroutine, counted in s.background.
func (s *Service) inBackground(f func()) {
	s.background.Add(1)
	go func() {
		defer s.background.Done()
		f()
	}()
}

// treeRingTimeout bounds the members' lookup of a ring.
const treeRingTimeout = 5 * time.Second

// ringTrees rings the trees of the session's members, and of others (a
// member who just left): their pages reload them. Best effort: a tree that
// misses it reloads within a minute.
func (s *Service) ringTrees(ctx context.Context, sessionID string, others ...string) {
	ctx, cancel := context.WithTimeout(ctx, treeRingTimeout)
	defer cancel()
	members, err := s.store.ListSessionMembers(ctx, sessionID)
	if err != nil {
		log.Printf("session %s: ring the trees: %v", sessionID, err)
	}
	data, _ := json.Marshal(map[string]string{"session_id": sessionID})
	for _, m := range members {
		s.hub.Publish(TreeTopic(m.UserID), activity.SSEEvent{Type: EventTreeChanged, Data: data})
	}
	for _, id := range others {
		s.hub.Publish(TreeTopic(id), activity.SSEEvent{Type: EventTreeChanged, Data: data})
	}
}
