package activity

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

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
	// LLMOnMachine is where its turns call their model
	// (store.LLMOnMachine*); empty = never.
	LLMOnMachine string `json:"llm_on_machine,omitempty"`
}

// SkillActivities serves an agent's prompt and name to the workflows, built
// from the shared Prompts.
type SkillActivities struct {
	// Prompts builds the prompts; the LLM activity shares it, to build the
	// prompt of each call.
	Prompts *Prompts
	catalog *Catalog
}

// NewSkillActivities creates a SkillActivities with initial skills and the shared catalog.
func NewSkillActivities(skills []skill.Skill, catalog *Catalog) *SkillActivities {
	return &SkillActivities{Prompts: NewPrompts(skills, catalog), catalog: catalog}
}

// SetSkills is a package-level wrapper so external packages can update skills
// without exposing a method that Temporal would register as an activity;
// version is what they were loaded from (skill.Version).
func SetSkills(a *SkillActivities, skills []skill.Skill, version string) {
	a.Prompts.SetSkills(skills)
	a.Prompts.SetVersion(version)
}

type LoadSkillsForAgentInput struct {
	AgentID string `json:"agent_id"`
}

type LoadSkillsForAgentOutput struct {
	// SystemPrompt is the agent's base prompt, as of now: the back-office
	// shows it. A turn's calls build theirs (LLMActivities.CallLLM).
	SystemPrompt string `json:"system_prompt"`
	// Name is the agent's name, which signs its messages; its ID when the
	// catalog does not know it.
	Name string `json:"name,omitempty"`
	// LLMOnMachine is where its turns call their model
	// (store.LLMOnMachine*); empty = never, the server's key.
	LLMOnMachine string `json:"llm_on_machine,omitempty"`
	// RunSkills are the names of its skills marked "runs: true", which its
	// coding runs take along (tool.CallContext.RunSkills).
	RunSkills []string `json:"run_skills,omitempty"`
}

// LoadSkillsForAgent returns the agent's name, its prompt for the tools its
// allowlist grants, where its turns call their model, and the skills its
// coding runs take along.
func (a *SkillActivities) LoadSkillsForAgent(ctx context.Context, input LoadSkillsForAgentInput) (LoadSkillsForAgentOutput, error) {
	var tools []string
	for name := range a.catalog.AllowedTools(input.AgentID).Resolutions {
		tools = append(tools, name)
	}
	name, onMachine := input.AgentID, ""
	var runSkills []string
	for _, e := range a.catalog.Agents() {
		if e.ID != input.AgentID {
			continue
		}
		if e.Name != "" {
			name = e.Name
		}
		onMachine = e.LLMOnMachine
		runSkills = a.Prompts.runSkillNames(e.Skills)
	}
	return LoadSkillsForAgentOutput{SystemPrompt: a.Prompts.AgentPrompt(input.AgentID, tools), Name: name, LLMOnMachine: onMachine,
		RunSkills: runSkills}, nil
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
// (conversation.Convert).
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
