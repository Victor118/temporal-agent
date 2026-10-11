package session

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/victor/temporal-agent/access"
	"github.com/victor/temporal-agent/store"
)

// Which agents answer whom (docs/design/permissions.md §4.4). The rules are
// enforced by the turn itself (the workers' catalog): here they spare a
// member a message no agent will answer, a session whose agent never
// answers, a list of agents they cannot call. A rule changed meanwhile only
// makes the server wrong for a moment, never opens anything.

// ErrAgentNotOpen refuses a session or a fork whose agent is closed to the
// member, and a member no agent is open to.
var ErrAgentNotOpen = errors.New("agent not open to you")

// accessTTL is how long the rules are reused: every message, page and list
// of agents reads them, as the sessions' states.
const accessTTL = statusesTTL

// accessCache holds the access rules read last. The zero value is ready.
type accessCache struct {
	mu   sync.Mutex
	at   time.Time
	snap access.Snapshot
}

// access returns the rules and users, read at most accessTTL ago. A read
// that fails leaves the agents open here, logged: the turn decides.
func (s *Service) access(ctx context.Context) access.Snapshot {
	c := &s.rules
	c.mu.Lock()
	defer c.mu.Unlock()
	now := s.statuses.clock()
	if !c.at.IsZero() && now.Sub(c.at) < accessTTL {
		return c.snap
	}
	snap, err := s.store.AccessSnapshot(ctx)
	if err != nil {
		log.Printf("Access rules not read: %v", err)
		return c.snap
	}
	c.snap, c.at = snap, now
	return snap
}

// AgentOpen reports whether agentID answers userID.
func (s *Service) AgentOpen(ctx context.Context, userID, agentID string) bool {
	return s.access(ctx).Allowed(userID, access.Agent(agentID))
}

// OpenAgents are the agents of agents that answer userID, in order.
func (s *Service) OpenAgents(ctx context.Context, userID string, agents []store.Agent) []store.Agent {
	snap := s.access(ctx)
	var open []store.Agent
	for _, a := range agents {
		if snap.Allowed(userID, access.Agent(a.ID)) {
			open = append(open, a)
		}
	}
	return open
}
