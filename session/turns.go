package session

import (
	"cmp"
	"context"
	"encoding/json"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// turnTrust is how long a turn event outweighs the visibility queries.
// Those lag behind the workflows by a moment: right after a participant
// ends they may still list it running. Past that moment they are right, and
// they also cover an event that never arrived: a turn_done lost, or a
// turn_started lost on a message this server delivered. A participant done
// with its turn and given nothing more still does not work, whatever the
// queries say: it runs on between two turns (writing an end, relaying,
// checking a message it does not answer).
const turnTrust = 30 * time.Second

// turnForget is how long what the events told of a participant is kept:
// a turn never seen ending (its end lost, the server away when it came, or
// its session deleted during it), a message delivered and never seen
// started. No turn runs that long; until then, the visibility queries tell
// whether the participant runs.
const turnForget = 24 * time.Hour

// participantTurns is what the server knows of a participant of a session,
// from the turn events as they pass through its hub and from the messages
// it delivered. It is held in memory: after a restart, the server knows
// nothing, and the visibility queries tell.
type participantTurns struct {
	working bool
	event   workflow.TurnEvent // the last turn event, of the turn working when working
	at      time.Time          // when it came
	// note is what the working turn waits for, as its last notice said
	// (activity.EventNotice): "" once said over. The next turn started
	// clears it.
	note string
	// pending are the messages the server delivered to the participant
	// and saw neither started nor done, with when it delivered them: the
	// participant's queue, as far as this server knows.
	pending map[int64]time.Time
}

// turns are the sessions' participants, by session ID then participant. The
// zero value is ready to use. A session's state is the aggregate of its
// participants': one ending does not hide another one working.
type turns struct {
	mu sync.Mutex
	m  map[string]map[string]*participantTurns
	// now is the clock; nil = time.Now. Tests set it.
	now func() time.Time
}

func (t *turns) clock() time.Time {
	if t.now != nil {
		return t.now()
	}
	return time.Now()
}

// participant returns the participant's state, created if need be. Under
// t.mu.
func (t *turns) participant(sessionID, participant string) *participantTurns {
	if t.m == nil {
		t.m = map[string]map[string]*participantTurns{}
	}
	if t.m[sessionID] == nil {
		t.m[sessionID] = map[string]*participantTurns{}
	}
	p := t.m[sessionID][participant]
	if p == nil {
		p = &participantTurns{pending: map[int64]time.Time{}}
		t.m[sessionID][participant] = p
	}
	return p
}

// prune drops what is forgotten: a participant with nothing pending, whose
// last event is older than turnForget, and messages pending as long. A
// participant done is kept until then: its turn_done outweighs the queries
// (working). Under t.mu.
func (t *turns) prune(now time.Time) {
	for sid, participants := range t.m {
		for name, p := range participants {
			for id, at := range p.pending {
				if now.Sub(at) >= turnForget {
					delete(p.pending, id)
				}
			}
			if len(p.pending) == 0 && now.Sub(p.at) >= turnForget {
				delete(participants, name)
			}
		}
		if len(participants) == 0 {
			delete(t.m, sid)
		}
	}
}

// set records a turn event. It names its participant and its message by its
// turn's key: started, the message is the participant's current one;
// done, the participant is done with it (answered, skipped or dropped),
// and works no more if it was its current one.
func (t *turns) set(sessionID string, started bool, e workflow.TurnEvent) {
	participant := cmp.Or(store.TurnParticipant(e.Turn), e.AgentID)
	message, _ := store.TurnAnchor(e.Turn)
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	t.prune(now)
	p := t.participant(sessionID, participant)
	delete(p.pending, message)
	switch {
	case started:
		*p = participantTurns{working: true, event: e, at: now, pending: p.pending}
	case !p.working || p.event.Turn == e.Turn:
		p.working, p.event, p.at, p.note = false, e, now, ""
	}
}

// expect records a message the server delivers to a participant: it waits
// in its queue until the participant starts it, or is done with it.
func (t *turns) expect(sessionID, participant string, messageID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	t.prune(now)
	t.participant(sessionID, participant).pending[messageID] = now
}

// unexpect forgets a message the server could not deliver.
func (t *turns) unexpect(sessionID, participant string, messageID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if p := t.m[sessionID][participant]; p != nil {
		delete(p.pending, messageID)
	}
}

// queued is how many messages the server delivered to a participant that
// it has not started yet; 0 when the server knows of none (after a
// restart, say): in doubt, a message goes through.
func (t *turns) queued(sessionID, participant string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.m[sessionID][participant]
	if p == nil {
		return 0
	}
	n := 0
	for _, at := range p.pending {
		if t.clock().Sub(at) < turnForget {
			n++
		}
	}
	return n
}

// current is the message a participant answers, as its last turn event
// said; 0 when none is known.
func (t *turns) current(sessionID, participant string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	p := t.m[sessionID][participant]
	if p == nil || !p.working || t.clock().Sub(p.at) >= turnForget {
		return 0
	}
	id, _ := store.TurnAnchor(p.event.Turn)
	return id
}

// setNote records what a participant's turn waits for: only on a turn
// known to be working, which the note is about. A notice that names no
// participant is for those of the session working. It changes nothing
// else of the turn, its time included: a notice says nothing of the turn's
// state.
func (t *turns) setNote(sessionID, participant, note string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for name, p := range t.m[sessionID] {
		if p.working && (participant == "" || name == participant) {
			p.note = note
		}
	}
}

// forget drops the session's participants: the session is gone.
func (t *turns) forget(sessionID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.m, sessionID)
}

// working lists the participants of a session that work: those a trusted
// event says are on a turn, and past the trust those the visibility queries
// see running that are on a turn, or have a message delivered and not
// started, as far as the events tell. A participant whose last event is its
// turn_done does not work, whatever its age: the queries list it running
// between two turns too. A turn_started lost on a message this server did
// not deliver (a relay) leaves that turn unseen until its end: a known
// limit of phase 1. running are the participants the queries see. In the
// order of their names.
func (t *turns) working(sessionID string, running []string) []Working {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock()
	var out []Working
	known := t.m[sessionID]
	for name, p := range known {
		works := p.working
		if p.at.IsZero() || now.Sub(p.at) >= turnTrust {
			works = slices.Contains(running, name) && (p.working || len(p.pending) > 0)
		}
		if !works {
			continue
		}
		w := Working{Participant: name, AgentID: name}
		if p.working && now.Sub(p.at) < turnForget {
			w.AgentID, w.Name, w.Note = p.event.AgentID, p.event.AgentName, p.note
		}
		out = append(out, w)
	}
	for _, name := range running {
		if _, ok := known[name]; !ok {
			out = append(out, Working{Participant: name, AgentID: name})
		}
	}
	slices.SortFunc(out, func(a, b Working) int { return cmp.Compare(a.Participant, b.Participant) })
	return out
}

// sessions lists the sessions the events told of.
func (t *turns) sessions() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	ids := make([]string, 0, len(t.m))
	for id := range t.m {
		ids = append(ids, id)
	}
	return ids
}

// Working is a participant at work in a session: its agent, its name when
// a turn event gave it (empty: the caller names it), and what its turn
// waits for (a coding run waiting for a free worker).
type Working struct {
	Participant string
	AgentID     string
	Name        string
	Note        string
}

// WorkingAgents are the participants at work in the session, as the turn
// events and the visibility queries tell (Statuses): several work at once.
func (s *Service) WorkingAgents(ctx context.Context, sessionID string) []Working {
	v := s.statuses.get(ctx, s.loadVisible)
	return s.turns.working(sessionID, v.runningIn(sessionID))
}

// QueuedBehind is the message a participant answers, which a message
// delivered to it now waits behind: 0 when it answers none the server
// knows of.
func (s *Service) QueuedBehind(sessionID, participant string) int64 {
	return s.turns.current(sessionID, participant)
}

// Observe learns from an event published on the server's hub, before the
// pages it rings reload: the turn events feed the sessions' participants, a
// notice their note; an event that changes what a session is doing
// (StateEvents) drops the cached statuses, and rings its members' trees. A
// notice is no such event: no status changes, no tree rings, only the
// thread reloads (ThreadEvents). It runs on the publisher's way: the rings,
// which read the members, go in the background.
func (s *Service) Observe(topic string, ev activity.SSEEvent) {
	if !IsSessionTopic(topic) {
		return
	}
	if ev.Type == activity.EventNotice {
		var n struct {
			Text        string `json:"text"`
			Participant string `json:"participant"`
		}
		if err := json.Unmarshal(ev.Data, &n); err != nil {
			log.Printf("session %s: %s: %v", topic, ev.Type, err)
			return
		}
		s.turns.setNote(topic, n.Participant, n.Text)
		return
	}
	if !slices.Contains(StateEvents, ev.Type) {
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
