package activity

import (
	"context"
	"strings"
	"testing"

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
		if out.Agents["default"] != (AgentLabel{Name: "Jarvis", Mention: "jarvis"}) || out.Agents["smith"] != (AgentLabel{Name: "smith", Mention: "smith"}) {
			t.Errorf("%s: agents %+v", id, out.Agents)
		}
	}
}
