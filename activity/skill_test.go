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
