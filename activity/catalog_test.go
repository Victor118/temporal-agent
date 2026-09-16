package activity

import (
	"testing"

	"github.com/victor/temporal-agent/store"
)

func testCatalog() *Catalog {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{
		{ID: "open"},
		{ID: "restricted", Tools: []string{"github_*", "web_fetch"}},
		{ID: "none", Tools: []string{}},
	})
	c.SetTools([]store.ToolRecord{
		{Name: "exec", Kind: "activity", TaskQueue: "tools-core"},
		{Name: "github_list", Kind: "activity", TaskQueue: "tools-github"},
		{Name: "spawn_session", Kind: "workflow", WorkflowName: "AgentWorkflow", TaskQueue: "tools-core"},
		{Name: "web_fetch", Kind: "activity", TaskQueue: "tools-core"},
	})
	return c
}

func names(out ListToolsOutput) []string {
	var n []string
	for _, t := range out.Tools {
		n = append(n, t.Name)
	}
	return n
}

func TestCatalog_AllowedTools(t *testing.T) {
	c := testCatalog()
	cases := map[string][]string{
		"open":       {"exec", "github_list", "spawn_session", "web_fetch"},
		"unknown":    {"exec", "github_list", "spawn_session", "web_fetch"},
		"restricted": {"github_list", "web_fetch"},
		"none":       nil,
	}
	for agent, want := range cases {
		out := c.AllowedTools(agent)
		got := names(out)
		if len(got) != len(want) {
			t.Errorf("%s: got %v, want %v", agent, got, want)
			continue
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("%s: got %v, want %v", agent, got, want)
				break
			}
		}
		if len(out.Resolutions) != len(want) {
			t.Errorf("%s: %d resolutions, want %d", agent, len(out.Resolutions), len(want))
		}
	}
}

func TestCatalog_AllowedTools_Resolution(t *testing.T) {
	out := testCatalog().AllowedTools("restricted")
	res, ok := out.Resolutions["github_list"]
	if !ok || res.TaskQueue != "tools-github" || res.Kind != "activity" {
		t.Errorf("github_list resolution = %+v, ok=%v", res, ok)
	}
	if _, ok := out.Resolutions["exec"]; ok {
		t.Error("exec must not be resolvable for a restricted agent")
	}
}
