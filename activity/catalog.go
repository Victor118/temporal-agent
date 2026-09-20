package activity

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// Catalog is a worker's in-memory copy of the DB agents and tools catalogs.
// It is shared by the worker's activities and refreshed by polling.
type Catalog struct {
	mu     sync.RWMutex
	agents []AgentCatalogEntry
	tools  []store.ToolRecord // sorted by name
}

func NewCatalog() *Catalog {
	return &Catalog{}
}

func (c *Catalog) SetAgents(agents []AgentCatalogEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.agents = agents
}

func (c *Catalog) SetTools(tools []store.ToolRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tools = tools
}

func (c *Catalog) Agents() []AgentCatalogEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.agents
}

func (c *Catalog) Tools() []store.ToolRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tools
}

// AllowedTools returns the tools agentID may use, in catalog order (sorted by
// name, so the prompt prefix stays stable), with how to dispatch each one.
// An agent without an allowlist, or unknown, gets every tool.
func (c *Catalog) AllowedTools(agentID string) ListToolsOutput {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var allowlist []string
	for _, a := range c.agents {
		if a.ID == agentID {
			allowlist = a.Tools
			break
		}
	}

	out := ListToolsOutput{
		Tools:       []provider.ToolDefinition{},
		Resolutions: make(map[string]ToolResolution),
	}
	delegatable := delegatableAgents(c.agents, agentID)

	for _, t := range c.tools {
		if allowlist != nil && !tool.MatchAny(allowlist, t.Name) {
			continue
		}
		schema := t.InputSchema
		if t.Name == SpawnToolName {
			// Delegation is restricted by value, not by tool name: the allowlist
			// can only say whether spawn_session exists, so the legal targets are
			// pinned in the schema the model decodes against.
			if len(delegatable) == 0 {
				continue // nobody to delegate to: an empty enum is unsatisfiable
			}
			ids := make([]string, len(delegatable))
			for i, e := range delegatable {
				ids[i] = e.ID
			}
			restricted, err := withAgentIDEnum(t.InputSchema, ids)
			if err != nil {
				// Publishing the unrestricted schema would silently widen what the
				// agent may spawn, so drop the tool instead.
				log.Printf("Warning: %s schema not restricted for agent %q, tool dropped: %v", t.Name, agentID, err)
				continue
			}
			schema = restricted
		}
		out.Tools = append(out.Tools, provider.ToolDefinition{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: schema,
		})
		out.Resolutions[t.Name] = ToolResolution{
			Kind:          t.Kind,
			WorkflowName:  t.WorkflowName,
			TaskQueue:     t.TaskQueue,
			FireAndForget: t.FireAndForget,
		}
	}
	return out
}

// withAgentIDEnum returns schema with agent_id constrained to ids and marked
// required. Both matter: an enum on an optional field constrains nothing, since
// omitting the field bypasses it. It is applied on read rather than stored,
// because the tools table is published by workers and is agent-agnostic — and
// because a worker running an older binary may still publish a schema without
// the required field.
//
// The enum only steers decoding; it is not a guarantee. buildChildInput
// validates the target again before spawning anything.
func withAgentIDEnum(schema json.RawMessage, ids []string) (json.RawMessage, error) {
	var doc map[string]any
	if err := json.Unmarshal(schema, &doc); err != nil {
		return nil, fmt.Errorf("parse schema: %w", err)
	}
	props, ok := doc["properties"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("schema has no properties object")
	}
	field, ok := props["agent_id"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("schema has no agent_id property")
	}
	field["enum"] = ids

	required := []string{"agent_id"}
	if existing, ok := doc["required"].([]any); ok {
		for _, r := range existing {
			if name, ok := r.(string); ok && name != "agent_id" {
				required = append(required, name)
			}
		}
	}
	sort.Strings(required) // stable output: this schema is part of the prompt prefix
	doc["required"] = required

	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encode schema: %w", err)
	}
	return out, nil
}
