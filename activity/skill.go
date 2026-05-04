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

// AgentCatalogEntry describes a specialized agent registered on a task queue.
type AgentCatalogEntry struct {
	TaskQueue string   `json:"task_queue"`
	Skills    []string `json:"skills"`
}

// SkillActivities provides skill loading as a Temporal activity.
// Prompts are built at worker startup from pre-loaded skills.
// The agents catalog is fetched from the server and can be updated at runtime.
type SkillActivities struct {
	mu      sync.RWMutex
	prompts map[string]string // queue → system prompt (skills only, no catalog)
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

type LoadSkillsForQueueInput struct {
	TaskQueue string `json:"task_queue"`
}

type LoadSkillsForQueueOutput struct {
	SystemPrompt string `json:"system_prompt"`
}

func (a *SkillActivities) LoadSkillsForQueue(ctx context.Context, input LoadSkillsForQueueInput) (LoadSkillsForQueueOutput, error) {
	a.mu.RLock()
	basePrompt := a.prompts[input.TaskQueue]
	catalog := a.catalog
	a.mu.RUnlock()

	if basePrompt == "" {
		basePrompt = defaultSystemPrompt
	}

	// Append agents directory so every agent knows about the others (excluding self)
	directory := buildAgentsDirectory(catalog, input.TaskQueue)
	return LoadSkillsForQueueOutput{SystemPrompt: basePrompt + directory}, nil
}

// BuildSkillPrompts builds a map of task queue → base system prompt (without agents directory).
// The agents directory is appended at runtime by LoadSkillsForQueue.
func BuildSkillPrompts(allSkills []skill.Skill, queueSkills map[string][]string) map[string]string {
	skillsByName := make(map[string]skill.Skill, len(allSkills))
	for _, s := range allSkills {
		skillsByName[s.Name] = s
	}

	prompts := make(map[string]string, len(queueSkills))
	for queue, skillNames := range queueSkills {
		var matched []skill.Skill
		for _, name := range skillNames {
			if s, ok := skillsByName[name]; ok {
				matched = append(matched, s)
			}
		}
		prompts[queue] = buildSystemPrompt(matched)
	}
	return prompts
}

// buildAgentsDirectory generates a prompt section listing all available specialized agents.
// currentQueue is excluded from the list so an agent never spawns a copy of itself.
func buildAgentsDirectory(catalog []AgentCatalogEntry, currentQueue string) string {
	var filtered []AgentCatalogEntry
	for _, entry := range catalog {
		if entry.TaskQueue != currentQueue {
			filtered = append(filtered, entry)
		}
	}

	if len(filtered) == 0 {
		return "## Specialized Agents\n\nNo specialized sub-agents are currently available. Do not invent agent names or task queues that are not listed here.\n\n"
	}

	var sb strings.Builder
	sb.WriteString("## Available Specialized Agents\n\n")
	sb.WriteString("You can delegate tasks to specialized agents using the `spawn_session` tool with the appropriate `task_queue`. Only use task queues listed below — do not invent others.\n\n")

	for _, entry := range filtered {
		sb.WriteString(fmt.Sprintf("- task_queue=`%s` (skills: %s)\n", entry.TaskQueue, strings.Join(entry.Skills, ", ")))
	}
	sb.WriteString("\nIMPORTANT: The `task_queue` parameter must be the exact task queue name (e.g. `market-analyst`), NOT a skill name (e.g. `market-research`).\n")
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
