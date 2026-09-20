package activity

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/victor/temporal-agent/skill"
)

// SpawnToolName is the tool through which an agent delegates to another agent.
// Its schema is the only one specialized per agent (see Catalog.AllowedTools).
const SpawnToolName = "spawn_session"

const promptIntro = "You are a helpful AI assistant with access to tools. You MUST use your tools proactively to accomplish the user's goals — do not just describe what you could do, actually do it.\n\n"

// promptRule is a behavior guideline shown only if the agent may use at least
// one of its tools (%s is replaced by the allowed ones). A rule without tools
// is always shown.
type promptRule struct {
	tools []string
	text  string
}

var promptRules = []promptRule{
	{[]string{"web_search", "web_fetch"}, "When the user asks for information you don't have, look it up with %s."},
	{[]string{"read_file", "write_file", "edit_file", "list_directory", "grep", "glob"}, "When the user asks you to work with files, use %s."},
	{[]string{"exec"}, "When the user asks you to run a command, use %s."},
	{[]string{SpawnToolName}, "When a task requires specialized expertise, use %s to delegate to a sub-agent."},
	{nil, "Always prefer action over explanation. If you can answer by using a tool, do it."},
	{nil, "Only use the tools you are given. If a task needs a tool you don't have, say so instead of pretending."},
	{[]string{"write_file", "edit_file"}, "NEVER write a file as a way to deliver your answer. Respond directly in the conversation. Only use %s when the user explicitly asks you to create or modify a file."},
	{[]string{"save_user_memory"}, "Use %s to remember important facts about the user (role, preferences, expertise, projects) that would be useful in future conversations. Each call replaces the full memory, so include everything. Only save when you learn something genuinely new and useful."},
}

// buildBehaviors returns the "Key behaviors" section for the allowed tools.
func buildBehaviors(allowed map[string]bool) string {
	var sb strings.Builder
	sb.WriteString(promptIntro)
	sb.WriteString("Key behaviors:\n")
	for _, r := range promptRules {
		if r.tools == nil {
			sb.WriteString("- " + r.text + "\n")
			continue
		}
		var names []string
		for _, t := range r.tools {
			if allowed[t] {
				names = append(names, t)
			}
		}
		if len(names) > 0 {
			sb.WriteString("- " + fmt.Sprintf(r.text, strings.Join(names, ", ")) + "\n")
		}
	}
	sb.WriteString("\n")
	return sb.String()
}

// AgentCatalogEntry describes a logical agent (persona) for the directory shown in prompts.
type AgentCatalogEntry struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Skills      []string `json:"skills"`
	Tools       []string `json:"tools"` // Allowed tool name globs; nil = all tools
}

// SkillActivities provides per-agent system prompt loading as a Temporal activity.
// It holds the loaded skills and reads agents from the shared catalog; both can
// change at runtime, and prompts are built on demand from the current state.
type SkillActivities struct {
	mu      sync.RWMutex
	skills  map[string]skill.Skill // skill name → skill
	catalog *Catalog
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

// NewSkillActivities creates a SkillActivities with initial skills and the shared catalog.
func NewSkillActivities(skills []skill.Skill, catalog *Catalog) *SkillActivities {
	a := &SkillActivities{catalog: catalog}
	a.setSkills(skills)
	return a
}

// SetSkills is a package-level wrapper so external packages can update skills
// without exposing a method that Temporal would register as an activity.
func SetSkills(a *SkillActivities, skills []skill.Skill) {
	a.setSkills(skills)
}

type LoadSkillsForAgentInput struct {
	AgentID string `json:"agent_id"`
}

type LoadSkillsForAgentOutput struct {
	SystemPrompt string `json:"system_prompt"`
	// Agents this one may delegate to: the catalog minus itself. The dispatch
	// validates spawn_session targets against this list, so it must hold exactly
	// what the agents directory in the prompt advertises.
	DelegatableAgentIDs []string `json:"delegatable_agent_ids"`
}

// LoadSkillsForAgent returns the full system prompt for the given agent (behaviors
// for its allowed tools, its skills, and — if it may delegate — the directory of
// all OTHER agents), and the IDs of the agents it may delegate to.
func (a *SkillActivities) LoadSkillsForAgent(ctx context.Context, input LoadSkillsForAgentInput) (LoadSkillsForAgentOutput, error) {
	catalog := a.catalog.Agents()
	allowed := make(map[string]bool)
	for name := range a.catalog.AllowedTools(input.AgentID).Resolutions {
		allowed[name] = true
	}

	var agentSkills []string
	for _, e := range catalog {
		if e.ID == input.AgentID {
			agentSkills = e.Skills
			break
		}
	}
	a.mu.RLock()
	prompt := buildSystemPrompt(matchSkills(a.skills, agentSkills), allowed)
	a.mu.RUnlock()

	// One list feeds both the prose and the validation, so they cannot drift:
	// an agent that is not in the directory is not a legal spawn target either.
	delegatable := delegatableAgents(catalog, input.AgentID)

	// The agents directory is only useful to an agent that can delegate
	if allowed[SpawnToolName] {
		prompt += buildAgentsDirectory(delegatable)
	}

	ids := make([]string, len(delegatable))
	for i, e := range delegatable {
		ids[i] = e.ID
	}

	return LoadSkillsForAgentOutput{
		SystemPrompt:        prompt,
		DelegatableAgentIDs: ids,
	}, nil
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

// delegatableAgents returns the agents currentAgentID may delegate to: every
// other agent in the catalog, sorted by ID. Sorting matters: this list is
// rendered into the system prompt and into the spawn_session schema, both sent
// on every turn, and an unstable order would break the LLM prompt cache prefix.
func delegatableAgents(catalog []AgentCatalogEntry, currentAgentID string) []AgentCatalogEntry {
	var filtered []AgentCatalogEntry
	for _, entry := range catalog {
		if entry.ID != currentAgentID {
			filtered = append(filtered, entry)
		}
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].ID < filtered[j].ID })
	return filtered
}

// buildAgentsDirectory generates a prompt section listing the agents that may be
// delegated to, as returned by delegatableAgents.
func buildAgentsDirectory(filtered []AgentCatalogEntry) string {
	if len(filtered) == 0 {
		return "## Agents Directory\n\nNo other agent is currently available to delegate to. Do not invent agent IDs that are not listed here.\n\n"
	}

	var sb strings.Builder
	sb.WriteString("## Agents Directory\n\n")
	sb.WriteString("You can delegate tasks to specialized agents using the `spawn_session` tool with the agent's `agent_id`. Only use agent IDs listed below — do not invent others.\n\n")

	for _, entry := range filtered {
		sb.WriteString(fmt.Sprintf("- **%s** (`agent_id=%s`)", entry.Name, entry.ID))
		if entry.Description != "" {
			sb.WriteString(" — " + entry.Description)
		}
		if len(entry.Skills) > 0 {
			sb.WriteString(fmt.Sprintf(" _(skills: %s)_", strings.Join(entry.Skills, ", ")))
		}
		sb.WriteString("\n")
	}
	sb.WriteString("\nIMPORTANT: The `agent_id` parameter must be the exact value shown above (e.g. `market-analyst`), NOT a skill name.\n")
	sb.WriteString("\n")

	return sb.String()
}

// buildSystemPrompt builds an agent's base prompt: behaviors for its allowed
// tools, then its skills.
func buildSystemPrompt(skills []skill.Skill, allowed map[string]bool) string {
	var sb strings.Builder
	sb.WriteString(buildBehaviors(allowed))

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
