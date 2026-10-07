package config

import (
	"strings"
	"testing"
)

func TestLoadAgentDefinitions_ToolsDefaultToNone(t *testing.T) {
	p := writeFile(t, `
agents:
  - id: bare
    name: Bare
  - id: scoped
    name: Scoped
    tools: ["github_*"]
`)
	defs, err := LoadAgentDefinitions(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(defs[0].Tools) != 0 {
		t.Errorf("bare: tools = %v, want none", defs[0].Tools)
	}
	if len(defs[1].Tools) != 1 || defs[1].Tools[0] != "github_*" {
		t.Errorf("scoped: tools = %v, want [github_*]", defs[1].Tools)
	}
}

func TestLoadAgentDefinitions_InvalidToolPattern(t *testing.T) {
	p := writeFile(t, `
agents:
  - id: broken
    name: Broken
    tools: ["github_[*"]
`)
	_, err := LoadAgentDefinitions(p)
	if err == nil || !strings.Contains(err.Error(), "invalid tool pattern") {
		t.Errorf("err = %v, want invalid tool pattern", err)
	}
}

// A mention must be writable as an @mention in a message: no space, no
// accent, nothing a message's @mention would stop at.
func TestAgentDefinition_Mention(t *testing.T) {
	for mention, ok := range map[string]bool{
		"": true, "jarvis": true, "Jarvis_2": true, "mon-agent": true,
		"mon agent": false, "élodie": false, "@jarvis": false, "a.b": false,
		strings.Repeat("x", 33): false,
	} {
		err := AgentDefinition{ID: "default", Name: "Default", Mention: mention}.Validate()
		if (err == nil) != ok {
			t.Errorf("mention %q: %v, want ok %v", mention, err, ok)
		}
	}
}

// Where an agent's model runs: one of three words, empty = never.
func TestAgentDefinition_LLMOnMachine(t *testing.T) {
	for v, ok := range map[string]bool{"": true, "never": true, "prefer": true, "require": true, "always": false, "Prefer": false} {
		err := AgentDefinition{ID: "default", Name: "Default", LLMOnMachine: v}.Validate()
		if (err == nil) != ok {
			t.Errorf("llm_on_machine %q: %v, want ok %v", v, err, ok)
		}
	}
}
