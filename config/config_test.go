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
