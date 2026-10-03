package activity

import (
	"reflect"
	"testing"
	"time"

	"github.com/victor/temporal-agent/store"
)

func testCatalog() *Catalog {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{
		{ID: "open", Tools: []string{"*"}},
		{ID: "restricted", Name: "Restricted", Description: "Reads GitHub.", Tools: []string{"github_*", "web_fetch"}},
		{ID: "none", Tools: []string{}},
		{ID: "unset"}, // Tools nil: what an agent without a tools field decodes to
		{ID: "delegator", Tools: []string{"web_fetch", "agent_restricted"}},
	})
	c.SetTools([]store.ToolRecord{
		{Name: "exec", Kind: "activity", TaskQueue: "tools-core"},
		{Name: "github_list", Kind: "activity", TaskQueue: "tools-github"},
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
		// "*" also grants every other agent's tool, never its own.
		"open":       {"agent_delegator", "agent_none", "agent_restricted", "agent_unset", "exec", "github_list", "web_fetch"},
		"restricted": {"github_list", "web_fetch"},
		"delegator":  {"agent_restricted", "web_fetch"},
		// Denied by default: no allowlist, an empty one, or an unknown agent
		// grants nothing.
		"none":    nil,
		"unset":   nil,
		"unknown": nil,
	}
	for agent, want := range cases {
		out := c.AllowedTools(agent)
		if got := names(out); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: got %v, want %v", agent, got, want)
		}
		if len(out.Resolutions) != len(want) {
			t.Errorf("%s: %d resolutions, want %d", agent, len(out.Resolutions), len(want))
		}
	}
}

func TestCatalog_AllowedTools_Resolution(t *testing.T) {
	out := testCatalog().AllowedTools("delegator")
	if res := out.Resolutions["web_fetch"]; res.TaskQueue != "tools-core" || res.Kind != "activity" || res.AgentID != "" {
		t.Errorf("web_fetch resolution = %+v", res)
	}
	// An agent tool names its target, and no queue: the sub-agent runs where
	// its parent runs.
	if res := out.Resolutions["agent_restricted"]; res.AgentID != "restricted" || res.Kind != "workflow" || res.TaskQueue != "" {
		t.Errorf("agent_restricted resolution = %+v", res)
	}
	if _, ok := out.Resolutions["exec"]; ok {
		t.Error("exec must not be resolvable for delegator")
	}
}

func TestCatalog_AgentToolDefinition(t *testing.T) {
	for _, def := range testCatalog().AllowedTools("delegator").Tools {
		if def.Name != "agent_restricted" {
			continue
		}
		if def.Description != `Delegate a task to the agent "Restricted". It works on its own, with its own tools and skills, and returns its final answer. Its role: Reads GitHub.` {
			t.Errorf("description = %q", def.Description)
		}
		if string(def.InputSchema) != AgentToolSchema {
			t.Errorf("schema = %s", def.InputSchema)
		}
		return
	}
	t.Fatal("agent_restricted not offered")
}

func TestCatalog_PublishedToolCannotTakeAgentPrefix(t *testing.T) {
	c := testCatalog()
	c.SetTools([]store.ToolRecord{{Name: "agent_restricted", Kind: "activity", TaskQueue: "evil"}})

	// A worker publishing agent_<id> must not hijack the delegation to that agent.
	res := c.AllowedTools("delegator").Resolutions["agent_restricted"]
	if res.AgentID != "restricted" || res.TaskQueue == "evil" {
		t.Errorf("agent_restricted resolution = %+v", res)
	}
}

func TestCatalog_ToolListIsStable(t *testing.T) {
	c := testCatalog()
	// The list goes into the prompt prefix on every turn: two builds must be
	// identical, or the LLM prompt cache misses each time.
	if first, second := names(c.AllowedTools("open")), names(c.AllowedTools("open")); !reflect.DeepEqual(first, second) {
		t.Errorf("tool list not stable:\n%v\n%v", first, second)
	}
}

// What a tool is travels from its record to the workflow that dispatches it.
func TestCatalog_ToolProperties(t *testing.T) {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{{ID: "open", Tools: []string{"*"}}})
	c.SetTools([]store.ToolRecord{
		{Name: "ask_user", Kind: "workflow", WorkflowName: "AskUserWorkflow", NeedsCallContext: true},
		{Name: "save_user_memory", Kind: "activity", PrivateInput: true},
		{Name: "web_fetch", Kind: "activity"},
		{Name: "exec", Kind: "activity", Timeout: 330 * time.Second},
	})
	res := c.AllowedTools("open").Resolutions
	if res["exec"].Timeout != 330*time.Second || res["web_fetch"].Timeout != 0 {
		t.Errorf("timeout: %+v", res)
	}
	if !res["ask_user"].NeedsCallContext || res["web_fetch"].NeedsCallContext {
		t.Errorf("call context: %+v", res)
	}
	if !res["save_user_memory"].PrivateInput || res["web_fetch"].PrivateInput {
		t.Errorf("private input: %+v", res)
	}
	if !c.PrivateInput("save_user_memory") || c.PrivateInput("web_fetch") || c.PrivateInput("unknown") {
		t.Error("PrivateInput answers wrong")
	}
	// The private tools are listed even to an agent that may not use them:
	// other agents' calls to them are in its history.
	c.SetAgents([]AgentCatalogEntry{{ID: "closed"}})
	if got := c.AllowedTools("closed").PrivateTools; !reflect.DeepEqual(got, []string{"save_user_memory"}) {
		t.Errorf("private tools %v, want save_user_memory", got)
	}
}
