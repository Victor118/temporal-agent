package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/victor/temporal-agent/access"
	"github.com/victor/temporal-agent/store"
)

// accessSweepStore is what the sweep of the access rules reads and writes.
type accessSweepStore interface {
	ListAccessRules(ctx context.Context) ([]store.AccessRule, error)
	ListTools(ctx context.Context) ([]store.ToolRecord, error)
	ListAgents(ctx context.Context) ([]store.Agent, error)
	ListUsers(ctx context.Context) ([]store.User, error)
	SetAccessRulesMatched(ctx context.Context, ids []int64) error
	ClaimUnmatchedAlert(ctx context.Context, id int64) (bool, error)
}

// accessSweepEvery is how often the server looks for a rule that targets
// nothing any more.
const accessSweepEvery = time.Minute

// runAccessSweep sweeps the access rules every minute until ctx ends.
func runAccessSweep(ctx context.Context, st accessSweepStore, alert func(ctx context.Context, userID, text string)) {
	tick := time.NewTicker(accessSweepEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			sweepAccessRules(sctx, st, alert)
			cancel()
		}
	}
}

// sweepAccessRules tells the admins, once, of a rule that targeted a
// published tool or an agent and targets none any more
// (docs/design/permissions.md §3.1): an MCP server renamed in its workers'
// configuration publishes its tools under another name, and what the rule
// closed is open to everyone, no rule having changed. A rule that targets
// something is marked so: should it target nothing later, the admins are
// told again.
func sweepAccessRules(ctx context.Context, st accessSweepStore, alert func(ctx context.Context, userID, text string)) {
	rules, err := st.ListAccessRules(ctx)
	if err != nil || len(rules) == 0 {
		if err != nil {
			log.Printf("Access sweep: %v", err)
		}
		return
	}
	tools, err := publishedRefs(ctx, st)
	if err != nil {
		log.Printf("Access sweep: %v", err)
		return
	}
	var matched []int64
	var unmatched []store.AccessRule
	for _, r := range rules {
		if len(access.Targets(r.Rule, tools)) > 0 {
			matched = append(matched, r.ID)
		} else {
			unmatched = append(unmatched, r)
		}
	}
	if err := st.SetAccessRulesMatched(ctx, matched); err != nil {
		log.Printf("Access sweep: %v", err)
	}
	var admins []string
	for _, r := range unmatched {
		claimed, err := st.ClaimUnmatchedAlert(ctx, r.ID)
		if err != nil {
			log.Printf("Access sweep: rule %d: %v", r.ID, err)
			continue
		}
		if !claimed {
			continue
		}
		text := UnmatchedRuleText(r.Rule)
		log.Printf("Access sweep: %s", text)
		if admins == nil {
			if admins, err = activeAdmins(ctx, st); err != nil {
				log.Printf("Access sweep: %v", err)
				continue
			}
		}
		for _, id := range admins {
			alert(ctx, id, text)
		}
	}
}

// UnmatchedRuleText is what the admins read of a rule that targets nothing
// any more.
func UnmatchedRuleText(r access.Rule) string {
	target := fmt.Sprintf("le motif « %s »", r.Target)
	if r.Kind == access.KindMCPServer {
		target = fmt.Sprintf("le serveur MCP « %s »", r.Target)
	}
	return fmt.Sprintf("La règle d'accès sur %s ne vise plus aucun outil : ce qu'elle fermait serait ouvert à tous s'il revenait sous un autre nom. "+
		"Un serveur MCP renommé dans worker.yaml ? Refais ses règles dans /admin/access.", target)
}

// publishedRefs are the published tools and the agents, as the rules see
// them.
func publishedRefs(ctx context.Context, st accessSweepStore) ([]access.ToolRef, error) {
	records, err := st.ListTools(ctx)
	if err != nil {
		return nil, fmt.Errorf("list tools: %w", err)
	}
	agents, err := st.ListAgents(ctx)
	if err != nil {
		return nil, fmt.Errorf("list agents: %w", err)
	}
	refs := make([]access.ToolRef, 0, len(records)+len(agents))
	for _, t := range records {
		refs = append(refs, access.ToolRef{Name: t.Name, MCPServer: t.MCPServer})
	}
	for _, a := range agents {
		refs = append(refs, access.Agent(a.ID))
	}
	return refs, nil
}

// activeAdmins lists the admins who may still log in.
func activeAdmins(ctx context.Context, st accessSweepStore) ([]string, error) {
	users, err := st.ListUsers(ctx)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	ids := []string{}
	for _, u := range users {
		if u.IsAdmin() && u.DisabledAt == nil {
			ids = append(ids, u.ID)
		}
	}
	return ids, nil
}
