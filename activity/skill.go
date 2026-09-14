package activity

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/skill"
)

const defaultSystemPrompt = `You are a helpful AI assistant with access to tools. You MUST use your tools proactively to accomplish the user's goals — do not just describe what you could do, actually do it.

Key behaviors:
- When the user asks for information you don't have, use web_fetch to look it up.
- When the user asks you to work with files, use read_file, write_file, edit_file, grep, glob.
- When the user asks you to run a command, use exec.
- When a task requires specialized expertise, use spawn_session to delegate to a sub-agent.
- Always prefer action over explanation. If you can answer by using a tool, do it.
- NEVER write a file as a way to deliver your answer. Respond directly in the conversation. Only use write_file/edit_file when the user explicitly asks you to create or modify a file.
- Use save_user_memory to remember important facts about the user (role, preferences, expertise, projects) that would be useful in future conversations. Each call replaces the full memory, so include everything. Only save when you learn something genuinely new and useful.

`

// AgentCatalogEntry describes a logical agent (persona) for the directory shown in prompts.
type AgentCatalogEntry struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Description  string   `json:"description"`
	Skills       []string `json:"skills"`
	DefaultQueue string   `json:"default_queue"`
}

// SkillActivities provides per-agent system prompt loading as a Temporal activity.
// Prompts are built at worker startup from pre-loaded skills, indexed by agent ID.
// The catalog is fetched from the DB and can be updated at runtime.
type SkillActivities struct {
	mu      sync.RWMutex
	prompts map[string]string // agent_id → base system prompt (skills only, no directory)
	catalog []AgentCatalogEntry
}

func (a *SkillActivities) setPrompts(prompts map[string]string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.prompts = prompts
}

func (a *SkillActivities) setCatalog(catalog []AgentCatalogEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.catalog = catalog
}

// NewSkillActivities creates a SkillActivities with initial prompts and catalog.
func NewSkillActivities(prompts map[string]string, catalog []AgentCatalogEntry) *SkillActivities {
	a := &SkillActivities{}
	a.setPrompts(prompts)
	a.setCatalog(catalog)
	return a
}

// SetPrompts is a package-level wrapper so external packages can update prompts
// without exposing a method that Temporal would register as an activity.
func SetPrompts(a *SkillActivities, prompts map[string]string) {
	a.setPrompts(prompts)
}

// SetCatalog is a package-level wrapper so external packages can update the catalog
// without exposing a method that Temporal would register as an activity.
func SetCatalog(a *SkillActivities, catalog []AgentCatalogEntry) {
	a.setCatalog(catalog)
}

type LoadSkillsForAgentInput struct {
	AgentID string `json:"agent_id"`
}

type LoadSkillsForAgentOutput struct {
	SystemPrompt   string            `json:"system_prompt"`
	QueueToAgentID map[string]string `json:"queue_to_agent_id"` // default_queue → agent_id, used to route spawn_session
}

// LoadSkillsForAgent returns the full system prompt for the given agent (its base
// skills prompt plus the directory of all OTHER agents, self excluded), and a
// queue→agent_id map used by the workflow to resolve spawn_session targets.
func (a *SkillActivities) LoadSkillsForAgent(ctx context.Context, input LoadSkillsForAgentInput) (LoadSkillsForAgentOutput, error) {
	a.mu.RLock()
	basePrompt := a.prompts[input.AgentID]
	catalog := a.catalog
	a.mu.RUnlock()

	if basePrompt == "" {
		basePrompt = defaultSystemPrompt
	}

	directory := buildAgentsDirectory(catalog, input.AgentID)

	queueMap := make(map[string]string, len(catalog))
	for _, e := range catalog {
		if e.DefaultQueue != "" {
			queueMap[e.DefaultQueue] = e.ID
		}
	}

	return LoadSkillsForAgentOutput{
		SystemPrompt:   basePrompt + directory,
		QueueToAgentID: queueMap,
	}, nil
}

// ResolveAgentByQueue returns the agent_id whose default_queue matches the given queue.
// Used at workflow startup to resolve a fallback agent identity when AgentID is not
// provided in the input (e.g. legacy session start).
func (a *SkillActivities) ResolveAgentByQueue(ctx context.Context, queue string) (string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, e := range a.catalog {
		if e.DefaultQueue == queue {
			return e.ID, nil
		}
	}
	return "", nil
}

// BuildSkillPrompts builds a map of agent_id → base system prompt (without agents directory).
// The directory is appended at runtime by LoadSkillsForAgent (so it can filter out self).
func BuildSkillPrompts(allSkills []skill.Skill, agentDefs []config.AgentDefinition) map[string]string {
	skillsByName := make(map[string]skill.Skill, len(allSkills))
	for _, s := range allSkills {
		skillsByName[s.Name] = s
	}

	prompts := make(map[string]string, len(agentDefs))
	for _, def := range agentDefs {
		var matched []skill.Skill
		for _, name := range def.Skills {
			if s, ok := skillsByName[name]; ok {
				matched = append(matched, s)
			}
		}
		prompts[def.ID] = buildSystemPrompt(matched)
	}
	return prompts
}

// CatalogFromDefinitions converts AgentDefinitions to catalog entries.
// Used in dev mode where we don't query the DB to build the catalog.
func CatalogFromDefinitions(defs []config.AgentDefinition) []AgentCatalogEntry {
	out := make([]AgentCatalogEntry, len(defs))
	for i, d := range defs {
		out[i] = AgentCatalogEntry{
			ID:           d.ID,
			Name:         d.Name,
			Description:  d.Description,
			Skills:       d.Skills,
			DefaultQueue: d.DefaultQueue,
		}
	}
	return out
}

// buildAgentsDirectory generates a prompt section listing all available specialized agents.
// currentAgentID is excluded from the list so an agent never spawns a copy of itself.
func buildAgentsDirectory(catalog []AgentCatalogEntry, currentAgentID string) string {
	var filtered []AgentCatalogEntry
	for _, entry := range catalog {
		if entry.ID != currentAgentID {
			filtered = append(filtered, entry)
		}
	}

	if len(filtered) == 0 {
		return "## Specialized Agents\n\nNo specialized sub-agents are currently available. Do not invent agent IDs that are not listed here.\n\n"
	}

	var sb strings.Builder
	sb.WriteString("## Available Specialized Agents\n\n")
	sb.WriteString("You can delegate tasks to specialized agents using the `spawn_session` tool with the appropriate `task_queue` (the agent's default queue). Only use task queues listed below — do not invent others.\n\n")

	for _, entry := range filtered {
		sb.WriteString(fmt.Sprintf("- **%s** (`task_queue=%s`)", entry.Name, entry.DefaultQueue))
		if entry.Description != "" {
			sb.WriteString(" — " + entry.Description)
		}
		if len(entry.Skills) > 0 {
			sb.WriteString(fmt.Sprintf(" _(skills: %s)_", strings.Join(entry.Skills, ", ")))
		}
		sb.WriteString("\n")
	}
	sb.WriteString("\nIMPORTANT: The `task_queue` parameter must be the exact value shown above (e.g. `market-analyst`), NOT a skill name.\n")
	sb.WriteString("\n")

	return sb.String()
}

func buildSystemPrompt(skills []skill.Skill) string {
	var sb strings.Builder
	sb.WriteString(defaultSystemPrompt)

	if len(skills) > 0 {
		sb.WriteString("## Skills\n\n")
		for _, s := range skills {
			sb.WriteString(fmt.Sprintf("### %s\n", s.Name))
			if s.Description != "" {
				sb.WriteString(fmt.Sprintf("%s\n\n", s.Description))
			}
			if s.Content != "" {
				sb.WriteString(s.Content)
				sb.WriteString("\n\n")
			}
		}
	}

	return sb.String()
}
