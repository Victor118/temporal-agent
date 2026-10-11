package main

import (
	"context"
	"errors"
	"testing"

	"github.com/victor/temporal-agent/access"
	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
)

// catalogRows is a catalog source in memory.
type catalogRows struct {
	agents    []store.Agent
	tools     []store.ToolRecord
	access    access.Snapshot
	accessErr error
}

func (c *catalogRows) ListAgents(context.Context) ([]store.Agent, error)     { return c.agents, nil }
func (c *catalogRows) ListTools(context.Context) ([]store.ToolRecord, error) { return c.tools, nil }
func (c *catalogRows) AccessSnapshot(context.Context) (access.Snapshot, error) {
	return c.access, c.accessErr
}

// A catalog takes its agents and tools once it has read the rules: before,
// everything would be open to everyone. Read once, a failure keeps them.
func TestRefreshCatalog_RulesFirst(t *testing.T) {
	rows := &catalogRows{
		agents:    []store.Agent{{ID: "commercial", Tools: []string{"*"}}},
		tools:     []store.ToolRecord{{Name: "crm_search", MCPServer: "crm"}},
		accessErr: errors.New("db down"),
	}
	catalog := activity.NewCatalog()
	refreshCatalog(rows, catalog)
	if len(catalog.Agents()) != 0 || len(catalog.Tools()) != 0 {
		t.Fatalf("catalog loaded without the rules: %v %v", catalog.Agents(), catalog.Tools())
	}

	rows.accessErr = nil
	rows.access = access.Snapshot{Rules: []access.Rule{{Kind: access.KindMCPServer, Target: "crm", Users: []string{"alice"}}}}
	refreshCatalog(rows, catalog)
	if len(catalog.Agents()) != 1 || len(catalog.Tools()) != 1 {
		t.Fatalf("catalog not loaded with the rules: %v %v", catalog.Agents(), catalog.Tools())
	}
	if out := catalog.AllowedToolsFor("commercial", "bob"); len(out.Tools) != 0 || len(out.Closed) != 1 {
		t.Errorf("bob's tools %+v", out)
	}

	rows.accessErr = errors.New("db down")
	refreshCatalog(rows, catalog)
	if snap, loaded := catalog.Access(); !loaded || len(snap.Rules) != 1 {
		t.Errorf("rules lost on a failed reload: %+v", snap)
	}
}
