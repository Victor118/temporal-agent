package activity

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/victor/temporal-agent/store"
)

// spawnTestSchema mirrors what tool.RegisterSpawnTool publishes, trimmed to the
// fields AllowedTools rewrites.
const spawnTestSchema = `{"type":"object","properties":{"task":{"type":"string"},"agent_id":{"type":"string"}},"required":["agent_id","task"]}`

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
		{Name: SpawnToolName, Kind: "workflow", WorkflowName: "AgentWorkflow", TaskQueue: "tools-core",
			InputSchema: []byte(spawnTestSchema)},
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

// spawnSchema returns the spawn_session schema as built for agentID, or "" if
// the tool is not in that agent's list.
func spawnSchema(t *testing.T, c *Catalog, agentID string) string {
	t.Helper()
	for _, def := range c.AllowedTools(agentID).Tools {
		if def.Name == SpawnToolName {
			return string(def.InputSchema)
		}
	}
	return ""
}

func TestCatalog_SpawnSchemaPinsDelegationTargets(t *testing.T) {
	c := testCatalog()
	c.SetTools([]store.ToolRecord{{
		Name: SpawnToolName, Kind: "workflow", WorkflowName: "AgentWorkflow", TaskQueue: "tools-core",
		InputSchema: []byte(`{"type":"object","properties":{"task":{"type":"string"},"agent_id":{"type":"string"}},"required":["task"]}`),
	}}) // published without agent_id in required: the rewrite must add it

	var doc struct {
		Properties struct {
			AgentID struct {
				Enum []string `json:"enum"`
			} `json:"agent_id"`
		} `json:"properties"`
		Required []string `json:"required"`
	}
	if err := json.Unmarshal([]byte(spawnSchema(t, c, "open")), &doc); err != nil {
		t.Fatal(err)
	}

	// Every other agent, sorted, and never the agent itself.
	want := []string{"none", "restricted"}
	if !reflect.DeepEqual(doc.Properties.AgentID.Enum, want) {
		t.Errorf("enum = %v, want %v", doc.Properties.AgentID.Enum, want)
	}
	// An enum on an optional field constrains nothing: agent_id must be required
	// even though the published schema did not say so.
	if !reflect.DeepEqual(doc.Required, []string{"agent_id", "task"}) {
		t.Errorf("required = %v, want [agent_id task]", doc.Required)
	}
}

func TestCatalog_SpawnSchemaIsStable(t *testing.T) {
	c := testCatalog()
	// The schema goes into the prompt prefix on every turn: two builds must be
	// byte-identical, or the LLM prompt cache misses each time.
	if first, second := spawnSchema(t, c, "open"), spawnSchema(t, c, "open"); first != second {
		t.Errorf("schema not stable:\n%s\n%s", first, second)
	}
}

func TestCatalog_SpawnDroppedWithoutTargets(t *testing.T) {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{{ID: "solo"}})
	c.SetTools([]store.ToolRecord{
		{Name: "exec", Kind: "activity", TaskQueue: "tools-core"},
		{Name: SpawnToolName, Kind: "workflow", TaskQueue: "tools-core",
			InputSchema: []byte(`{"type":"object","properties":{"agent_id":{"type":"string"}}}`)},
	})

	// The only agent has nobody to delegate to: an empty enum is unsatisfiable,
	// so the tool must not be offered at all.
	out := c.AllowedTools("solo")
	if got := names(out); len(got) != 1 || got[0] != "exec" {
		t.Errorf("tools = %v, want [exec]", got)
	}
	if _, ok := out.Resolutions[SpawnToolName]; ok {
		t.Error("spawn_session must not be resolvable without a delegation target")
	}
}

func TestCatalog_SpawnDroppedOnUnusableSchema(t *testing.T) {
	c := testCatalog()
	c.SetTools([]store.ToolRecord{{
		Name: SpawnToolName, Kind: "workflow", TaskQueue: "tools-core",
		InputSchema: []byte(`{"type":"object","properties":{"task":{"type":"string"}}}`), // no agent_id
	}})

	// Falling back to the published schema would let the agent spawn anything.
	if got := spawnSchema(t, c, "open"); got != "" {
		t.Errorf("spawn_session offered with an unrestricted schema: %s", got)
	}
}
