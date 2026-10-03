package activity

import (
	"cmp"
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/victor/temporal-agent/skill"
)

const promptIntro = "You are a helpful AI assistant with access to tools. You MUST use your tools proactively to accomplish the user's goals — do not just describe what you could do, actually do it.\n\n"

// promptRule is a behavior guideline shown only if the agent may use at least
// one of its tools (%s is replaced by the allowed ones). A tool may be a glob,
// which lists every allowed tool it matches. A rule without tools is always
// shown.
type promptRule struct {
	tools []string
	text  string
}

var promptRules = []promptRule{
	{[]string{"web_search", "web_fetch"}, "When the user asks for information you don't have, look it up with %s."},
	{[]string{"read_file", "write_file", "edit_file", "list_directory", "grep", "glob"}, "When the user asks you to work with files, use %s."},
	{[]string{"exec"}, "When the user asks you to run a command, use %s."},
	{[]string{AgentToolPrefix + "*"}, "When a task requires specialized expertise, delegate it to the agent suited for it: %s. The agent does not see this conversation, so give it the full context."},
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
			if !strings.ContainsAny(t, "*?[") {
				if allowed[t] {
					names = append(names, t)
				}
				continue
			}
			var matched []string
			for name := range allowed {
				if ok, _ := path.Match(t, name); ok {
					matched = append(matched, name)
				}
			}
			sort.Strings(matched) // the prompt is a cached prefix: keep it stable
			names = append(names, matched...)
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
	Mention     string   `json:"mention"` // what calls it in a session; never empty once loaded
	Description string   `json:"description"`
	Skills      []string `json:"skills"`
	Tools       []string `json:"tools"` // Allowed tool name globs; empty = no tool, "*" = all
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
	// Name is the agent's name, which signs its messages; its ID when the
	// catalog does not know it.
	Name string `json:"name,omitempty"`
	// Agents names every agent of the catalog by ID: the history the agent
	// reads holds other agents' turns, shown under their current name and
	// mention.
	Agents map[string]AgentLabel `json:"agents,omitempty"`
}

// AgentLabel is how an agent is named to another one: its name, and the
// mention that calls it.
type AgentLabel struct {
	Name    string `json:"name"`
	Mention string `json:"mention"`
}

// LoadSkillsForAgent returns the system prompt for the given agent: behaviors
// for its allowed tools, then its skills. The agents it may delegate to need no
// section of their own: each agent_<id> tool carries its agent's description.
func (a *SkillActivities) LoadSkillsForAgent(ctx context.Context, input LoadSkillsForAgentInput) (LoadSkillsForAgentOutput, error) {
	catalog := a.catalog.Agents()
	allowed := make(map[string]bool)
	for name := range a.catalog.AllowedTools(input.AgentID).Resolutions {
		allowed[name] = true
	}

	var self AgentCatalogEntry
	labels := make(map[string]AgentLabel, len(catalog))
	for _, e := range catalog {
		if e.ID == input.AgentID {
			self = e
		}
		labels[e.ID] = AgentLabel{Name: cmp.Or(e.Name, e.ID), Mention: e.Mention}
	}
	a.mu.RLock()
	prompt := identitySection(self) + buildSystemPrompt(matchSkills(a.skills, self.Skills), allowed)
	a.mu.RUnlock()

	name := self.Name
	if name == "" {
		name = input.AgentID
	}
	return LoadSkillsForAgentOutput{SystemPrompt: prompt, Name: name, Agents: labels}, nil
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

// identitySection tells the agent its name and how members call it: in a
// shared session the messages it answers carry "@<mention>", which it would
// otherwise take for someone else. It also tells how the conversation shows
// who speaks: people and other agents write in it too.
func identitySection(self AgentCatalogEntry) string {
	var sb strings.Builder
	sb.WriteString("## Identity\n\n")
	if self.Name != "" && self.Mention != "" {
		fmt.Fprintf(&sb, "Your name is %s. In a conversation, people address you by writing @%s. ", self.Name, self.Mention)
	}
	sb.WriteString(conversationNote)
	return sb.String()
}

// conversationNote explains the prefixes the conversation carries
// (workflow.convertMessages).
const conversationNote = "Several people and several AI agents may write in a conversation. " +
	"A person's message starts with their name in brackets: [Alice]. " +
	"Another agent's turn reaches you as text in a user message, starting with [agent Name (@mention)], its tool calls and their results included: " +
	"it is that agent's work, not yours and not a person's. Your own messages carry no prefix: never write one.\n\n"

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
