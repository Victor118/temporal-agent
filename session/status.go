package session

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

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

// statusCache holds the last session states read from Temporal. The zero
// value is ready to use. The map it hands out is shared: read it, never
// write to it.
type statusCache struct {
	mu    sync.Mutex
	at    time.Time
	value map[string]Status
}

// get returns the cached states, or loads them when they are older than
// statusesTTL. Loading holds the lock: requests arriving meanwhile wait for
// that one load rather than starting their own.
func (c *statusCache) get(ctx context.Context, load func(context.Context) map[string]Status) map[string]Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.value != nil && time.Since(c.at) < statusesTTL {
		return c.value
	}
	v := load(ctx)
	if ctx.Err() == nil { // a cancelled request read nothing worth sharing
		c.value, c.at = v, time.Now()
	}
	return v
}

// invalidate drops the cached states: after an action that changes them, the
// page it renders must not show the state from before.
func (c *statusCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = nil
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
