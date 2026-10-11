package activity

import (
	"context"
	"strings"
	"testing"

	"github.com/victor/temporal-agent/access"
	"github.com/victor/temporal-agent/conversation"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/store"
)

// basePrompt is agentID's prompt for its whole list of tools, as the
// back-office shows it.
func basePrompt(a *SkillActivities, c *Catalog, agentID string) string {
	return a.Prompts.AgentPrompt(agentID, names(c.AllowedTools(agentID)))
}

func TestAgentPrompt_FollowsAllowlist(t *testing.T) {
	c := readyCatalog()
	c.SetAgents([]AgentCatalogEntry{
		{ID: "default", Name: "Default", Tools: []string{"*"}},
		{ID: "analyst", Name: "Analyst", Skills: []string{"market"}, Tools: []string{"web_*"}},
	})
	c.SetTools([]store.ToolRecord{
		{Name: "exec"},
		{Name: "web_fetch"}, {Name: "write_file"},
	})
	a := NewSkillActivities([]skill.Skill{{Name: "market", Content: "MARKET SKILL"}}, c)

	p := basePrompt(a, c, "analyst")
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

	p = basePrompt(a, c, "default")
	// "default" may delegate to "analyst" only: never to itself.
	for _, want := range []string{"use exec.", "delegate it to the agent suited for it: agent_analyst.", "Only use write_file when"} {
		if !strings.Contains(p, want) {
			t.Errorf("default prompt missing %q", want)
		}
	}
	if strings.Contains(p, "agent_default") {
		t.Error("default prompt offers delegating to itself")
	}
}

// The agent knows its name and what calls it: in a shared session the
// messages it answers start with its mention.
func TestAgentPrompt_SaysWhatCallsIt(t *testing.T) {
	c := readyCatalog()
	c.SetAgents([]AgentCatalogEntry{{ID: "default", Name: "Jarvis", Mention: "jarvis"}})
	p := basePrompt(NewSkillActivities(nil, c), c, "default")
	if !strings.Contains(p, "Your name is Jarvis. In a conversation, people address you by writing @jarvis.") {
		t.Errorf("prompt %q", p)
	}
	// Other agents' turns reach it as prefixed text: it must know what they are.
	if !strings.Contains(p, "[agent Name (@mention)]") {
		t.Errorf("prompt %q does not explain other agents' turns", p)
	}
}

// The agent's name signs its messages. Every agent of the catalog is named,
// to show the others' turns.
func TestLoadSkillsForAgent_GivesTheName(t *testing.T) {
	c := readyCatalog()
	c.SetAgents([]AgentCatalogEntry{{ID: "default", Name: "Jarvis", Mention: "jarvis"}, {ID: "smith", Mention: "smith"}})
	a := NewSkillActivities(nil, c)
	if out, err := a.LoadSkillsForAgent(context.Background(), LoadSkillsForAgentInput{AgentID: "default"}); err != nil || out.Name != "Jarvis" {
		t.Errorf("name %q (%v), want Jarvis", out.Name, err)
	}
	labels := c.AgentLabels()
	if labels["default"] != (conversation.Label{Name: "Jarvis", Mention: "jarvis"}) || labels["smith"] != (conversation.Label{Name: "smith", Mention: "smith"}) {
		t.Errorf("agents %+v", labels)
	}
}

// The skills an agent's coding runs take along: its own, marked runs, by
// name; read again by name where a run is prepared.
func TestRunSkills(t *testing.T) {
	c := readyCatalog()
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
	if p := basePrompt(a, c, "coder"); strings.Contains(p, "RED, GREEN, REFACTOR.") || !strings.Contains(p, "Look twice.") {
		t.Errorf("prompt:\n%s", p)
	}
	set := a.Prompts.RunSkills([]string{"tdd", "review", "tdd", "ghost"})
	if len(set.Skills) != 1 || set.Skills[0].Name != "tdd" || set.Skills[0].Content != "RED, GREEN, REFACTOR." || set.Skills[0].Description != "Test first" ||
		len(set.Missing) != 2 || set.Missing[0] != "review" || set.Missing[1] != "ghost" || set.Version != "c0ffee" {
		t.Errorf("%+v", set)
	}
}

// LoadSkillsForAgent answers only once the catalog can tell: read once at
// least (agents and rules), and knowing the agent (created less than a
// reload ago, it does not yet). Answered empty, the turn would sign with
// the ID, call its model with the server's key, run its code without its
// skills.
func TestLoadSkillsForAgent_WaitsForTheCatalog(t *testing.T) {
	c := NewCatalog()
	a := NewSkillActivities(nil, c)
	in := LoadSkillsForAgentInput{AgentID: "coder"}
	if _, err := a.LoadSkillsForAgent(context.Background(), in); err == nil {
		t.Error("an empty catalog answered")
	}
	c.SetAgents([]AgentCatalogEntry{{ID: "default"}})
	c.SetAccess(access.Snapshot{})
	if _, err := a.LoadSkillsForAgent(context.Background(), in); err == nil || !strings.Contains(err.Error(), "agent coder unknown") {
		t.Errorf("an agent unknown to the catalog: %v", err)
	}
	c.SetAgents([]AgentCatalogEntry{{ID: "coder", Name: "Coder", LLMOnMachine: "require"}})
	if out, err := a.LoadSkillsForAgent(context.Background(), in); err != nil || out.Name != "Coder" || out.LLMOnMachine != "require" {
		t.Errorf("ready: %+v, %v", out, err)
	}
}
