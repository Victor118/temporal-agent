package activity

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/victor/temporal-agent/store"
)

// A session turn is offered the background field on the tools that allow
// it: the workflow tools, the agents', not a private one, not an activity,
// not one whose schema has a field of that name. Elsewhere, on none.
func TestCatalog_OffersBackgroundOnEligibleTools(t *testing.T) {
	c := NewCatalog()
	c.SetAgents([]AgentCatalogEntry{{ID: "jarvis", Name: "Jarvis"}, {ID: "smith", Name: "Smith"}})
	c.SetTools([]store.ToolRecord{
		{Name: "analyze_repo", Kind: "workflow", InputSchema: json.RawMessage(`{"type":"object","properties":{"repo":{"type":"string"}},"required":["repo"]}`)},
		{Name: "secret_flow", Kind: "workflow", PrivateInput: true, InputSchema: json.RawMessage(`{"type":"object"}`)},
		{Name: "own_flag", Kind: "workflow", InputSchema: json.RawMessage(`{"type":"object","properties":{"background":{"type":"string"}}}`)},
		{Name: "exec", Kind: "activity", InputSchema: json.RawMessage(`{"type":"object"}`)},
	})
	names := []string{"agent_smith", "analyze_repo", "exec", "own_flag", "secret_flow"}
	defs, _, backgrounds := c.ToolDefinitions(names, true)
	if !backgrounds {
		t.Error("no tool offered the background")
	}
	offered := map[string]bool{}
	for _, d := range defs {
		var s struct {
			Properties map[string]struct {
				Type string `json:"type"`
			} `json:"properties"`
			Required []string `json:"required"`
		}
		if err := json.Unmarshal(d.InputSchema, &s); err != nil {
			t.Fatalf("%s: %v", d.Name, err)
		}
		offered[d.Name] = s.Properties[BackgroundField].Type == "boolean"
		if d.Name == "analyze_repo" && (s.Properties["repo"].Type != "string" || len(s.Required) != 1) {
			t.Errorf("analyze_repo's own schema changed: %s", d.InputSchema)
		}
	}
	for name, want := range map[string]bool{"agent_smith": true, "analyze_repo": true, "exec": false, "own_flag": false, "secret_flow": false} {
		if offered[name] != want {
			t.Errorf("%s offered the background: %v", name, offered[name])
		}
	}
	// The same bytes on every call: a cached prefix.
	again, _, _ := c.ToolDefinitions(names, true)
	for i := range defs {
		if string(defs[i].InputSchema) != string(again[i].InputSchema) {
			t.Errorf("%s: not the same schema twice", defs[i].Name)
		}
	}
	if _, _, backgrounds := c.ToolDefinitions(names, false); backgrounds {
		t.Error("offered outside a session turn")
	}
	plain, _, _ := c.ToolDefinitions([]string{"agent_smith"}, false)
	if strings.Contains(string(plain[0].InputSchema), BackgroundField) {
		t.Errorf("offered outside a session turn: %s", plain[0].InputSchema)
	}

	// The dispatch knows the same from the resolutions.
	c.SetAgents([]AgentCatalogEntry{{ID: "jarvis", Tools: []string{"*"}}, {ID: "smith"}})
	res := c.AllowedTools("jarvis").Resolutions
	for name, want := range map[string]bool{"agent_smith": true, "analyze_repo": true, "exec": false, "own_flag": false, "secret_flow": false} {
		if res[name].Background != want {
			t.Errorf("%s resolves background %v", name, res[name].Background)
		}
	}
}

func TestTakeBackground(t *testing.T) {
	for _, c := range []struct {
		in, out    string
		background bool
		err        bool
	}{
		{in: `{"task":"x","background":true}`, out: `{"task":"x"}`, background: true},
		{in: `{"task":"x","background":false}`, out: `{"task":"x"}`},
		{in: `{"task":"x"}`, out: `{"task":"x"}`},
		{in: `{"task":"background"}`, out: `{"task":"background"}`},
		{in: `"not an object"`, out: `"not an object"`},
		{in: `{"background":"yes"}`, err: true},
	} {
		out, background, err := TakeBackground(json.RawMessage(c.in))
		if (err != nil) != c.err || background != c.background || (!c.err && string(out) != c.out) {
			t.Errorf("%s: %s, %v, %v", c.in, out, background, err)
		}
	}
}
