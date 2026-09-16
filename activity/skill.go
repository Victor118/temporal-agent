package activity

import (
	"context"
	"fmt"
	"strings"
	"sync"

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
	Tools        []string `json:"tools"` // Allowed tool name globs; nil = all tools
	DefaultQueue string   `json:"default_queue"`
}

// SkillActivities provides per-agent system prompt loading as a Temporal activity.
// It holds the loaded skills and the agent catalog (read from the DB); both can be
// replaced at runtime, and prompts are built on demand from the current state.
type SkillActivities struct {
	mu      sync.RWMutex
	skills  map[string]skill.Skill // skill name → skill
	catalog []AgentCatalogEntry
}

func (a *SkillActivities) setSkills(skills []skill.Skill) {
	byName := make(map[string]skill.Skill, len(skills))
	for _, s := range skills {
		byName[s.Name] = s
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.skills = byName
}

func (a *SkillActivities) setCatalog(catalog []AgentCatalogEntry) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.catalog = catalog
}

// NewSkillActivities creates a SkillActivities with initial skills and catalog.
func NewSkillActivities(skills []skill.Skill, catalog []AgentCatalogEntry) *SkillActivities {
	a := &SkillActivities{}
	a.setSkills(skills)
	a.setCatalog(catalog)
	return a
}

// SetSkills is a package-level wrapper so external packages can update skills
// without exposing a method that Temporal would register as an activity.
func SetSkills(a *SkillActivities, skills []skill.Skill) {
	a.setSkills(skills)
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
	basePrompt := defaultSystemPrompt
	catalog := a.catalog
	for _, e := range catalog {
		if e.ID == input.AgentID {
			basePrompt = buildSystemPrompt(matchSkills(a.skills, e.Skills))
			break
		}
	}
	a.mu.RUnlock()

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

// matchSkills returns the skills named in names, in order, skipping unknown ones.
func matchSkills(byName map[string]skill.Skill, names []string) []skill.Skill {
	var matched []skill.Skill
	for _, name := range names {
		if s, ok := byName[name]; ok {
			matched = append(matched, s)
		}
	}
	return matched
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
