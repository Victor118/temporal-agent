package main

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/victor/temporal-agent/access"
	"github.com/victor/temporal-agent/store"
)

// sweepRows is what the access sweep reads, in memory, with the store's
// marks: matched, told.
type sweepRows struct {
	rules   []store.AccessRule
	tools   []store.ToolRecord
	agents  []store.Agent
	users   []store.User
	matched map[int64]bool
	told    map[int64]bool
}

func (s *sweepRows) ListAccessRules(context.Context) ([]store.AccessRule, error) { return s.rules, nil }
func (s *sweepRows) ListTools(context.Context) ([]store.ToolRecord, error)       { return s.tools, nil }
func (s *sweepRows) ListAgents(context.Context) ([]store.Agent, error)           { return s.agents, nil }
func (s *sweepRows) ListUsers(context.Context) ([]store.User, error)             { return s.users, nil }
func (s *sweepRows) SetAccessRulesMatched(_ context.Context, ids []int64) error {
	for _, id := range ids {
		s.matched[id], s.told[id] = true, false
	}
	return nil
}
func (s *sweepRows) ClaimUnmatchedAlert(_ context.Context, id int64) (bool, error) {
	if !s.matched[id] || s.told[id] {
		return false, nil
	}
	s.told[id] = true
	return true, nil
}

// A rule that targeted tools and targets none any more (its server renamed)
// is told to the active admins, once; one that never targeted anything is
// not, nor one on an agent that is there.
func TestSweepAccessRules_TellsARuleThatTargetsNothing(t *testing.T) {
	now := time.Now()
	rows := &sweepRows{
		rules: []store.AccessRule{
			{Rule: access.Rule{ID: 1, Kind: access.KindMCPServer, Target: "crm", Users: []string{"u-alice"}}},
			{Rule: access.Rule{ID: 2, Kind: access.KindPattern, Target: "agent_commercial", Users: []string{"u-alice"}}},
			{Rule: access.Rule{ID: 3, Kind: access.KindPattern, Target: "never_*", Users: []string{"u-alice"}}},
		},
		tools:   []store.ToolRecord{{Name: "crm_search", MCPServer: "crm"}},
		agents:  []store.Agent{{ID: "commercial"}},
		users:   []store.User{{ID: "u-root", Role: store.UserRoleAdmin}, {ID: "u-old", Role: store.UserRoleAdmin, DisabledAt: &now}, {ID: "u-alice", Role: store.UserRoleStandard}},
		matched: map[int64]bool{}, told: map[int64]bool{},
	}
	var alerts []string
	alert := func(_ context.Context, userID, text string) { alerts = append(alerts, userID+": "+text) }

	sweepAccessRules(context.Background(), rows, alert)
	if len(alerts) != 0 || !rows.matched[1] || !rows.matched[2] || rows.matched[3] {
		t.Fatalf("first sweep: alerts %v, matched %v", alerts, rows.matched)
	}

	// The server renamed: its tools come back under another name.
	rows.tools = []store.ToolRecord{{Name: "sales_search", MCPServer: "sales"}}
	sweepAccessRules(context.Background(), rows, alert)
	sweepAccessRules(context.Background(), rows, alert)
	want := []string{"u-root: " + UnmatchedRuleText(rows.rules[0].Rule)}
	if !slices.Equal(alerts, want) {
		t.Errorf("alerts %q, want %q", alerts, want)
	}
}
