package activity

import (
	"context"
	"strings"
	"testing"

	"github.com/victor/temporal-agent/conversation"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/store"
)

func TestLoadSkillsForAgent_PromptFollowsAllowlist(t *testing.T) {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{
		{ID: "default", Name: "Default", Tools: []string{"*"}},
		{ID: "analyst", Name: "Analyst", Skills: []string{"market"}, Tools: []string{"web_*"}},
	})
	c.SetTools([]store.ToolRecord{
		{Name: "exec"},
		{Name: "web_fetch"}, {Name: "write_file"},
	})
	a := NewSkillActivities([]skill.Skill{{Name: "market", Content: "MARKET SKILL"}}, c)

	out, err := a.LoadSkillsForAgent(context.Background(), LoadSkillsForAgentInput{AgentID: "analyst"})
	if err != nil {
		t.Fatal(err)
	}
	p := out.SystemPrompt
	for _, want := range []string{"look it up with web_fetch.", "MARKET SKILL", "Only use the tools you are given"} {
		if !strings.Contains(p, want) {
			t.Errorf("analyst prompt missing %q", want)
		}
	}
	for _, unwanted := range []string{"exec", "agent_", "delegate", "write_file", "web_search"} {
		if strings.Contains(p, unwanted) {
			t.Errorf("analyst prompt must not mention %q", unwanted)
		}
	}

	out, err = a.LoadSkillsForAgent(context.Background(), LoadSkillsForAgentInput{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	// "default" may delegate to "analyst" only: never to itself.
	for _, want := range []string{"use exec.", "delegate it to the agent suited for it: agent_analyst.", "Only use write_file when"} {
		if !strings.Contains(out.SystemPrompt, want) {
			t.Errorf("default prompt missing %q", want)
		}
	}
	if strings.Contains(out.SystemPrompt, "agent_default") {
		t.Error("default prompt offers delegating to itself")
	}
}

// The agent knows its name and what calls it: in a shared session the
// messages it answers start with its mention.
func TestLoadSkillsForAgent_SaysWhatCallsIt(t *testing.T) {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{{ID: "default", Name: "Jarvis", Mention: "jarvis"}})
	out, err := NewSkillActivities(nil, c).LoadSkillsForAgent(context.Background(), LoadSkillsForAgentInput{AgentID: "default"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.SystemPrompt, "Your name is Jarvis. In a conversation, people address you by writing @jarvis.") {
		t.Errorf("prompt %q", out.SystemPrompt)
	}
	// Other agents' turns reach it as prefixed text: it must know what they are.
	if !strings.Contains(out.SystemPrompt, "[agent Name (@mention)]") {
		t.Errorf("prompt %q does not explain other agents' turns", out.SystemPrompt)
	}
}

// The agent's name signs its messages; an agent the catalog does not know
// signs with its ID. Every agent of the catalog is named, to show the others'
// turns.
func TestLoadSkillsForAgent_GivesTheName(t *testing.T) {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{{ID: "default", Name: "Jarvis", Mention: "jarvis"}, {ID: "smith", Mention: "smith"}})
	a := NewSkillActivities(nil, c)
	for id, want := range map[string]string{"default": "Jarvis", "gone": "gone"} {
		out, err := a.LoadSkillsForAgent(context.Background(), LoadSkillsForAgentInput{AgentID: id})
		if err != nil || out.Name != want {
			t.Errorf("%s: name %q (%v), want %q", id, out.Name, err, want)
		}
	}
	labels := c.AgentLabels()
	if labels["default"] != (conversation.Label{Name: "Jarvis", Mention: "jarvis"}) || labels["smith"] != (conversation.Label{Name: "smith", Mention: "smith"}) {
		t.Errorf("agents %+v", labels)
	}
}

// The skills an agent's coding runs take along: its own, marked runs, by
// name; read again by name where a run is prepared.
func TestRunSkills(t *testing.T) {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{{ID: "coder", Skills: []string{"tdd", "review", "ghost"}}})
	a := NewSkillActivities(nil, c)
	SetSkills(a, []skill.Skill{
		{Name: "tdd", Description: "Test first", Content: "RED, GREEN, REFACTOR.", Runs: true},
		{Name: "review", Content: "Look twice."},
	}, "c0ffee")
	out, err := a.LoadSkillsForAgent(context.Background(), LoadSkillsForAgentInput{AgentID: "coder"})
	if err != nil || len(out.RunSkills) != 1 || out.RunSkills[0] != "tdd" {
		t.Fatalf("%+v %v", out.RunSkills, err)
	}
	// A run's skill is for the CLI that codes, never in the agent's prompt.
	if strings.Contains(out.SystemPrompt, "RED, GREEN, REFACTOR.") || !strings.Contains(out.SystemPrompt, "Look twice.") {
		t.Errorf("prompt:\n%s", out.SystemPrompt)
	}
	set := a.Prompts.RunSkills([]string{"tdd", "review", "tdd", "ghost"})
	if len(set.Skills) != 1 || set.Skills[0].Name != "tdd" || set.Skills[0].Content != "RED, GREEN, REFACTOR." || set.Skills[0].Description != "Test first" ||
		len(set.Missing) != 2 || set.Missing[0] != "review" || set.Missing[1] != "ghost" || set.Version != "c0ffee" {
		t.Errorf("%+v", set)
	}
}
