package activity

import (
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
	for _, t := range c.tools {
		if allowlist != nil && !tool.MatchAny(allowlist, t.Name) {
			continue
		}
		out.Tools = append(out.Tools, provider.ToolDefinition{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: t.InputSchema,
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
