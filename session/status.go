package session

import (
	"context"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/victor/temporal-agent/workflow"
)

// Status is what a session is doing, from the workflows Temporal runs for it.
type Status string

const (
	StatusIdle    Status = "idle"    // no participant works
	StatusWorking Status = "working" // a participant is on its messages, or a fork's summary is written
	StatusWaiting Status = "waiting" // a question waits for a member's answer
)

// rank orders statuses by how much they call for attention.
func (s Status) rank() int {
	switch s {
	case StatusWaiting:
		return 2
	case StatusWorking:
		return 1
	}
	return 0
}

// Stronger returns the status of the two that calls for more attention.
func (s Status) Stronger(o Status) Status {
	if o.rank() > s.rank() {
		return o
	}
	return s
}

// statusesTTL is how long the session states are reused. Every page and every
// tree refresh (each open tab of each member, on every turn event) needs
// them, and they cost three visibility queries over every running workflow:
// shared for a few seconds, that load no longer grows with the number of
// tabs.
const statusesTTL = 3 * time.Second

// statusesLoadTimeout bounds one load of the session states. The load belongs
// to no request: every request waiting for it shares it.
const statusesLoadTimeout = 5 * time.Second

// statusCache holds the last session states read from Temporal. The zero
// value is ready to use. The map it hands out is shared: read it, never
// write to it.
//
// One load runs at a time, in the background, under its own context: a
// request that triggered it and goes away does not cancel it for the others.
// While it runs, requests get the previous states if there are any; only a
// request with nothing to show waits for it.
type statusCache struct {
	mu    sync.Mutex
	at    time.Time
	value *visible
	// loading is closed when the running load ends; nil when none runs.
	loading chan struct{}
	// gen moves on every invalidation: a load started before one must not
	// store states that predate the action.
	gen uint64
	// workflows are single workflows' states, by ID, kept as long as the
	// session states and dropped with them: the fork pages read them on
	// every refresh (its summary's workflow, its report's).
	workflows map[string]workflowState
	// states are participants' answers to their state query, by workflow
	// ID, kept and dropped likewise: the Agents panel reads those the turn
	// events do not tell of on every refresh.
	states map[string]queriedState
	// now is the clock the states age by; nil = time.Now. Tests set it.
	now func() time.Time
}

// clock is the time now, as the cache tells it.
func (c *statusCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// workflowState is where a workflow's latest run stands.
type workflowState struct {
	status enumspb.WorkflowExecutionStatus // unspecified: Temporal knows of none, or cannot tell
	closed time.Time                       // when it ended; zero while it runs
	at     time.Time                       // when it was read
}

// queriedState is a participant's answer to its state query; ok false when
// it gave none.
type queriedState struct {
	state workflow.ParticipantState
	ok    bool
	at    time.Time
}

// get returns the cached states, reloading them when they are older than
// statusesTTL. It waits for a load only when it has nothing else to return,
// and no longer than ctx allows: a cancelled request gets nil.
func (c *statusCache) get(ctx context.Context, load func(context.Context) *visible) *visible {
	for {
		c.mu.Lock()
		if c.value != nil && c.clock().Sub(c.at) < statusesTTL {
			v := c.value
			c.mu.Unlock()
			return v
		}
		if c.loading == nil {
			c.loading = make(chan struct{})
			go c.refresh(c.gen, c.loading, load)
		}
		stale, done := c.value, c.loading
		c.mu.Unlock()

		if stale != nil {
			return stale
		}
		select {
		case <-done:
			// Loaded, or invalidated meanwhile: look again.
		case <-ctx.Done():
			return nil
		}
	}
}

// refresh runs one load and stores its result, unless the states were
// invalidated since it started.
func (c *statusCache) refresh(gen uint64, done chan struct{}, load func(context.Context) *visible) {
	ctx, cancel := context.WithTimeout(context.Background(), statusesLoadTimeout)
	defer cancel()
	v := load(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen == gen {
		c.value, c.at = v, c.clock()
	}
	if c.loading == done { // not replaced by a load started after an invalidation
		c.loading = nil
	}
	close(done)
}

// workflow returns a workflow's state, read by describe at most once per
// statusesTTL. Unlike the session states, each request reads a stale one
// itself: one Describe, not four visibility queries.
func (c *statusCache) workflow(ctx context.Context, id string, describe func(context.Context, string) workflowState) workflowState {
	c.mu.Lock()
	if w, ok := c.workflows[id]; ok && c.clock().Sub(w.at) < statusesTTL {
		c.mu.Unlock()
		return w
	}
	gen := c.gen
	c.mu.Unlock()

	at := c.clock()
	w := describe(ctx, id)
	w.at = at

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen == gen { // not read before an action that changed it
		now := c.clock()
		for k, old := range c.workflows {
			if now.Sub(old.at) >= statusesTTL {
				delete(c.workflows, k)
			}
		}
		if c.workflows == nil {
			c.workflows = map[string]workflowState{}
		}
		c.workflows[id] = w
	}
	return w
}

// participantState returns a participant's state, asked by query at most
// once per statusesTTL, like workflow.
func (c *statusCache) participantState(ctx context.Context, id string, query func(context.Context, string) (workflow.ParticipantState, error)) (workflow.ParticipantState, bool) {
	c.mu.Lock()
	if q, ok := c.states[id]; ok && c.clock().Sub(q.at) < statusesTTL {
		c.mu.Unlock()
		return q.state, q.ok
	}
	gen := c.gen
	c.mu.Unlock()

	at := c.clock()
	st, err := query(ctx, id)
	q := queriedState{state: st, ok: err == nil, at: at}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen == gen {
		now := c.clock()
		for k, old := range c.states {
			if now.Sub(old.at) >= statusesTTL {
				delete(c.states, k)
			}
		}
		if c.states == nil {
			c.states = map[string]queriedState{}
		}
		c.states[id] = q
	}
	return q.state, q.ok
}

// invalidate drops the cached states: after an action that changes them, the
// page it renders must not show the state from before — neither the cached
// one nor one a load already running read before the action.
func (c *statusCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = nil
	c.workflows = nil
	c.states = nil
	c.gen++
	// A running load now stores nothing; the next request starts its own.
	c.loading = nil
}

// visible is what the visibility queries tell of the sessions. Its maps
// are shared: read them, never write to them.
type visible struct {
	// statuses are the sessions' statuses but their participants': a
	// question waiting, a fork's summary being written.
	statuses map[string]Status
	// participants are the participants running, by session.
	participants map[string][]string
	// asking are the participants with a question waiting, by session.
	asking map[string][]string
}

// runningIn is the participants v sees running in a session; none for a nil
// v (a request cancelled before any load).
func (v *visible) runningIn(sessionID string) []string {
	if v == nil {
		return nil
	}
	return v.participants[sessionID]
}

// askingIn is the participants v sees with a question waiting in a
// session.
func (v *visible) askingIn(sessionID string) []string {
	if v == nil {
		return nil
	}
	return v.asking[sessionID]
}

// Statuses tells what each session is doing: from Temporal, cached for
// statusesTTL, and corrected by the turn events the server heard (turns.go).
// A session works while one of its participants does: one ending does not
// hide another one at work. The map is the caller's.
func (s *Service) Statuses(ctx context.Context) map[string]Status {
	v := s.statuses.get(ctx, s.loadVisible)
	statuses := map[string]Status{}
	if v != nil {
		for id, st := range v.statuses {
			statuses[id] = st
		}
	}
	sessions := s.turns.sessions()
	if v != nil {
		for id := range v.participants {
			sessions = append(sessions, id)
		}
	}
	for _, id := range sessions {
		if len(s.turns.working(id, v.runningIn(id))) > 0 {
			statuses[id] = statuses[id].Stronger(StatusWorking)
		}
	}
	return statuses
}

// loadVisible reads what the sessions are doing from the workflows running
// for them: their participants, a question waiting, a fork's summary being
// written. Three visibility queries, whatever the number of sessions. A
// failed query degrades the states shown, nothing else.
func (s *Service) loadVisible(ctx context.Context) *visible {
	v := &visible{statuses: map[string]Status{}, participants: map[string][]string{}, asking: map[string][]string{}}
	each := func(workflowType string, of func(id string)) {
		resp, err := s.temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.cfg.Namespace,
			Query:     fmt.Sprintf("WorkflowType = '%s' AND ExecutionStatus = 'Running'", workflowType),
			PageSize:  1000,
		})
		if err != nil {
			log.Printf("session: list running %s: %v", workflowType, err)
			return
		}
		for _, e := range resp.Executions {
			of(e.Execution.WorkflowId)
		}
	}
	// "<session>:p:<agent>"
	each("ParticipantWorkflow", func(id string) {
		if sid := sessionOf(id); sid != "" {
			if agent, ok := workflow.ParticipantOf(id); ok {
				v.participants[sid] = append(v.participants[sid], agent)
			}
		}
	})
	// A fork's summary being written: "fork-<session>".
	each("ForkSessionWorkflow", func(id string) {
		if sid := strings.TrimPrefix(id, "fork-"); checkSessionID(sid) == nil {
			v.statuses[sid] = v.statuses[sid].Stronger(StatusWorking)
		}
	})
	// "<turn>:tool:ask_user:…", from an agent or a sub-agent of it.
	each("AskUserWorkflow", func(id string) {
		if sid := sessionOf(id); sid != "" {
			v.statuses[sid] = v.statuses[sid].Stronger(StatusWaiting)
			if agent, ok := workflow.ParticipantOf(id); ok && !slices.Contains(v.asking[sid], agent) {
				v.asking[sid] = append(v.asking[sid], agent)
			}
		}
	})
	return v
}

// sessionOf returns the session (a canonical UUID) a workflow ID belongs to,
// what precedes its first ':' (workflow.SessionOf), or "".
func sessionOf(id string) string {
	sid, ok := workflow.SessionOf(id)
	if !ok || checkSessionID(sid) != nil {
		return ""
	}
	return sid
}
