package activity

import (
	"cmp"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/victor/temporal-agent/conversation"
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

// AgentToolPrefix names the tool that delegates to an agent: agent_<id>. These
// tools are generated from the agents catalog, never published by a worker, so
// the allowlist governs delegation like any other tool: "agent_code-reviewer"
// for one agent, "agent_*" for all of them.
const AgentToolPrefix = "agent_"

// AgentToolName returns the name of the tool that delegates to agentID.
func AgentToolName(agentID string) string { return AgentToolPrefix + agentID }

// AgentToolSchema is the input of every agent_<id> tool. The target is the
// tool itself, so there is no agent_id to fill in, and nothing to invent.
const AgentToolSchema = `{"type":"object","properties":{"task":{"type":"string","description":"The task for the agent, with all the context it needs: it does not see this conversation."},"model":{"type":"string","description":"Optional model override for the agent. Defaults to yours."}},"required":["task"]}`

// AllowedTools returns the tools agentID may use, sorted by name so the prompt
// prefix stays stable, with how to dispatch each one: the published tools its
// allowlist matches, and the agent_<id> tools of the other agents it matches.
// Access is denied by default: an empty allowlist, or an unknown agent, gets
// no tool at all. Granting everything takes an explicit "*".
func (c *Catalog) AllowedTools(agentID string) ListToolsOutput {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var allowlist []string
	known := false
	for _, a := range c.agents {
		if a.ID == agentID {
			allowlist, known = a.Tools, true
			break
		}
	}
	if !known {
		log.Printf("Warning: agent %q not in catalog, no tool allowed", agentID)
	}

	out := ListToolsOutput{
		Tools:       []provider.ToolDefinition{},
		Resolutions: make(map[string]ToolResolution),
	}

	for _, t := range c.tools {
		if strings.HasPrefix(t.Name, AgentToolPrefix) {
			// The prefix belongs to the generated agent tools: a published tool
			// using it would be shadowed, or would shadow an agent.
			continue
		}
		if !tool.MatchAny(allowlist, t.Name) {
			continue
		}
		out.Tools = append(out.Tools, publishedDefinition(t))
		out.Resolutions[t.Name] = ToolResolution{
			Kind:             t.Kind,
			WorkflowName:     t.WorkflowName,
			TaskQueue:        t.TaskQueue,
			FireAndForget:    t.FireAndForget,
			PrivateInput:     t.PrivateInput,
			NeedsCallContext: t.NeedsCallContext,
			Timeout:          t.Timeout,
		}
	}

	for _, a := range c.agents {
		name := AgentToolName(a.ID)
		// Never itself: an agent that delegates to itself only loops.
		if a.ID == agentID || !tool.MatchAny(allowlist, name) {
			continue
		}
		out.Tools = append(out.Tools, agentDefinition(a))
		// No task queue: a sub-agent runs where its parent runs, which only
		// the dispatching workflow knows.
		out.Resolutions[name] = ToolResolution{Kind: string(tool.ToolKindWorkflow), AgentID: a.ID}
	}

	sort.Slice(out.Tools, func(i, j int) bool { return out.Tools[i].Name < out.Tools[j].Name })
	return out
}

func publishedDefinition(t store.ToolRecord) provider.ToolDefinition {
	return provider.ToolDefinition{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema}
}

func agentDefinition(a AgentCatalogEntry) provider.ToolDefinition {
	return provider.ToolDefinition{
		Name:        AgentToolName(a.ID),
		Description: AgentToolDescription(a),
		InputSchema: json.RawMessage(AgentToolSchema),
	}
}

// ToolDefinitions returns the definitions of the tools named, in their
// order, as AllowedTools gives them, and the names the catalog no longer
// knows. The allowlist is not applied again: the names are what a turn may
// dispatch, decided at its start, and the model is offered exactly those.
func (c *Catalog) ToolDefinitions(names []string) (defs []provider.ToolDefinition, missing []string) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, name := range names {
		if def, ok := c.definition(name); ok {
			defs = append(defs, def)
		} else {
			missing = append(missing, name)
		}
	}
	return defs, missing
}

// definition finds name among the agent tools, or else the published ones.
// The caller holds the lock.
func (c *Catalog) definition(name string) (provider.ToolDefinition, bool) {
	if id, ok := strings.CutPrefix(name, AgentToolPrefix); ok {
		for _, a := range c.agents {
			if a.ID == id {
				return agentDefinition(a), true
			}
		}
		return provider.ToolDefinition{}, false
	}
	i := sort.Search(len(c.tools), func(i int) bool { return c.tools[i].Name >= name })
	if i < len(c.tools) && c.tools[i].Name == name {
		return publishedDefinition(c.tools[i]), true
	}
	return provider.ToolDefinition{}, false
}

// AgentLabels names every agent of the catalog by ID, its ID when it has no
// name: the history an agent reads holds the others' turns.
func (c *Catalog) AgentLabels() map[string]conversation.Label {
	c.mu.RLock()
	defer c.mu.RUnlock()
	labels := make(map[string]conversation.Label, len(c.agents))
	for _, a := range c.agents {
		labels[a.ID] = conversation.Label{Name: cmp.Or(a.Name, a.ID), Mention: a.Mention}
	}
	return labels
}

// PrivateInput reports whether a published tool keeps its input from the
// session's members (tool.PrivateInputs).
func (c *Catalog) PrivateInput(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	i := sort.Search(len(c.tools), func(i int) bool { return c.tools[i].Name >= name })
	return i < len(c.tools) && c.tools[i].Name == name && c.tools[i].PrivateInput
}

// AgentToolDescription is what the model reads about agent a: its tool
// description stands in for a directory of agents in the prompt.
func AgentToolDescription(a AgentCatalogEntry) string {
	d := fmt.Sprintf("Delegate a task to the agent %q. It works on its own, with its own tools and skills, and returns its final answer.", a.Name)
	if a.Description != "" {
		d += " Its role: " + a.Description
	}
	return d
}
