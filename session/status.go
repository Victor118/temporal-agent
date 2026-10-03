package session

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// Status is what a session is doing, from the workflows Temporal runs for it.
type Status string

const (
	StatusIdle    Status = "idle"    // no workflow: the session sleeps until a message
	StatusActive  Status = "active"  // its workflow runs, waiting for messages
	StatusWorking Status = "working" // the agent is on a turn
	StatusWaiting Status = "waiting" // a question waits for a member's answer
)

// rank orders statuses by how much they call for attention.
func (s Status) rank() int {
	switch s {
	case StatusWaiting:
		return 3
	case StatusWorking:
		return 2
	case StatusActive:
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
// tree refresh (each open tab, every 8 s) needs them, and they cost four
// visibility queries over every running workflow: shared for a few seconds,
// that load no longer grows with the number of tabs.
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
	value map[string]Status
	// loading is closed when the running load ends; nil when none runs.
	loading chan struct{}
	// gen moves on every invalidation: a load started before one must not
	// store states that predate the action.
	gen uint64
	// workflows are single workflows' states, by ID, kept as long as the
	// session states and dropped with them: the fork pages read them on
	// every refresh (its summary's workflow, its report's).
	workflows map[string]workflowState
}

// workflowState is where a workflow's latest run stands.
type workflowState struct {
	status enumspb.WorkflowExecutionStatus // unspecified: Temporal knows of none, or cannot tell
	closed time.Time                       // when it ended; zero while it runs
	at     time.Time                       // when it was read
}

// get returns the cached states, reloading them when they are older than
// statusesTTL. It waits for a load only when it has nothing else to return,
// and no longer than ctx allows: a cancelled request gets nil.
func (c *statusCache) get(ctx context.Context, load func(context.Context) map[string]Status) map[string]Status {
	for {
		c.mu.Lock()
		if c.value != nil && time.Since(c.at) < statusesTTL {
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
func (c *statusCache) refresh(gen uint64, done chan struct{}, load func(context.Context) map[string]Status) {
	ctx, cancel := context.WithTimeout(context.Background(), statusesLoadTimeout)
	defer cancel()
	v := load(ctx)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen == gen {
		c.value, c.at = v, time.Now()
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
	if w, ok := c.workflows[id]; ok && time.Since(w.at) < statusesTTL {
		c.mu.Unlock()
		return w
	}
	gen := c.gen
	c.mu.Unlock()

	at := time.Now()
	w := describe(ctx, id)
	w.at = at

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gen == gen { // not read before an action that changed it
		for k, old := range c.workflows {
			if time.Since(old.at) >= statusesTTL {
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

// invalidate drops the cached states: after an action that changes them, the
// page it renders must not show the state from before — neither the cached
// one nor one a load already running read before the action.
func (c *statusCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = nil
	c.workflows = nil
	c.gen++
	// A running load now stores nothing; the next request starts its own.
	c.loading = nil
}

// Statuses tells what each session is doing, cached for statusesTTL. The map
// is shared: read it, never write to it.
func (s *Service) Statuses(ctx context.Context) map[string]Status {
	return s.statuses.get(ctx, s.loadStatuses)
}

// loadStatuses tells what each session is doing, from the workflows running
// for it: a question waiting, an agent turn, or the session's own workflow
// waiting for messages. Four visibility queries, whatever the number of
// sessions. A failed query degrades the states shown, nothing else.
func (s *Service) loadStatuses(ctx context.Context) map[string]Status {
	statuses := map[string]Status{}
	mark := func(workflowType string, status Status, sessionOf func(id string) string) {
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
			if sid := sessionOf(e.Execution.WorkflowId); sid != "" {
				statuses[sid] = statuses[sid].Stronger(status)
			}
		}
	}
	// "session-<id>" or "session-<id>-<unix time>" for a resumed run.
	mark("SessionWorkflow", StatusActive, func(id string) string { return uuidPrefix(strings.TrimPrefix(id, "session-")) })
	// "<id>-turn-<n>", and sub-agents "<id>-tool-…".
	mark("AgentWorkflow", StatusWorking, uuidPrefix)
	// A fork's summary being written.
	mark("ForkSessionWorkflow", StatusWorking, func(id string) string { return uuidPrefix(strings.TrimPrefix(id, "fork-")) })
	// "<id>-tool-ask_user-…", from the session's agent or a sub-agent of it.
	mark("AskUserWorkflow", StatusWaiting, uuidPrefix)
	return statuses
}

// uuidPrefix returns the session ID (a UUID, 36 characters) a workflow ID
// starts with, or "".
func uuidPrefix(id string) string {
	if len(id) < 36 || (len(id) > 36 && id[36] != '-') {
		return ""
	}
	return id[:36]
}
