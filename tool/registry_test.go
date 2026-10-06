package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	r.Register(&Tool{
		Name:        "test_tool",
		Description: "A test tool",
		InputSchema: json.RawMessage(`{"type":"object"}`),
		Kind:        ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			return "ok", nil
		},
	})

	tool, ok := r.Get("test_tool")
	if !ok {
		t.Fatal("tool not found")
	}
	if tool.Name != "test_tool" {
		t.Errorf("got name %q", tool.Name)
	}
}

func TestRegistry_GetMissing(t *testing.T) {
	r := NewRegistry()
	_, ok := r.Get("nonexistent")
	if ok {
		t.Fatal("expected false for missing tool")
	}
}

func TestRegistry_Execute(t *testing.T) {
	r := NewRegistry()
	r.Register(&Tool{
		Name:        "echo",
		Description: "Echo input",
		InputSchema: json.RawMessage(`{"type":"object"}`),
		Kind:        ToolKindActivity,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			return string(input), nil
		},
	})

	result, err := r.Execute(context.Background(), "echo", json.RawMessage(`{"msg":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	if result != `{"msg":"hi"}` {
		t.Errorf("got %q", result)
	}
}

func TestRegistry_ExecuteUnknown(t *testing.T) {
	r := NewRegistry()
	_, err := r.Execute(context.Background(), "unknown", nil)
	if err == nil {
		t.Fatal("expected error for unknown tool")
	}
}

func TestRegistry_List(t *testing.T) {
	r := NewRegistry()
	r.Register(&Tool{
		Name:        "a",
		Description: "tool a",
		InputSchema: json.RawMessage(`{}`),
		Kind:        ToolKindActivity,
	})
	r.Register(&Tool{
		Name:        "b",
		Description: "tool b",
		InputSchema: json.RawMessage(`{}`),
		Kind:        ToolKindActivity,
	})

	defs := r.List()
	if len(defs) != 2 {
		t.Errorf("expected 2 tools, got %d", len(defs))
	}
}

func TestRegistry_Retain(t *testing.T) {
	r := NewRegistry()
	for _, name := range []string{"github_list", "github_create", "read_file", "exec"} {
		r.Register(&Tool{Name: name})
	}

	removed := r.Retain([]string{"github_*", "read_file"})

	if got, want := fmt.Sprint(removed), "[exec]"; got != want {
		t.Errorf("removed = %s, want %s", got, want)
	}
	var names []string
	for _, tl := range r.All() {
		names = append(names, tl.Name)
	}
	if got, want := fmt.Sprint(names), "[github_create github_list read_file]"; got != want {
		t.Errorf("remaining = %s, want %s", got, want)
	}
}

func TestMatchAny(t *testing.T) {
	cases := []struct {
		globs []string
		name  string
		want  bool
	}{
		{[]string{"*"}, "anything", true},
		{[]string{"memory_*"}, "memory_save", true},
		{[]string{"memory_*"}, "web_fetch", false},
		{nil, "web_fetch", false},
		{[]string{"[bad"}, "web_fetch", false},
	}
	for _, c := range cases {
		if got := MatchAny(c.globs, c.name); got != c.want {
			t.Errorf("MatchAny(%v, %q) = %v, want %v", c.globs, c.name, got, c.want)
		}
	}
}

func TestTool_SchemaHash(t *testing.T) {
	a := &Tool{Name: "x", Description: "d", InputSchema: json.RawMessage(`{"type":"object"}`)}
	b := &Tool{Name: "x", Description: "d", InputSchema: json.RawMessage(`{"type":"object"}`)}
	c := &Tool{Name: "x", Description: "other", InputSchema: json.RawMessage(`{"type":"object"}`)}

	if a.SchemaHash() != b.SchemaHash() {
		t.Error("identical tools must have the same hash")
	}
	if a.SchemaHash() == c.SchemaHash() {
		t.Error("different descriptions must change the hash")
	}
}

func names(tools []*Tool) string {
	var n []string
	for _, t := range tools {
		n = append(n, t.Name)
	}
	return fmt.Sprint(n)
}

// A server's tools follow what it gives: added, changed when their contract
// changes (not when it stays the same), removed when it no longer gives them.
func TestRegistry_SyncSource(t *testing.T) {
	r := NewRegistry()
	r.Register(&Tool{Name: "read_file"})

	c := r.SyncSource("gh", []*Tool{{Name: "gh_a"}, {Name: "gh_b"}})
	if names(c.Changed) != "[gh_a gh_b]" || len(c.Removed) != 0 || len(c.Refused) != 0 {
		t.Errorf("first sync: %+v", c)
	}
	c = r.SyncSource("gh", []*Tool{{Name: "gh_a"}, {Name: "gh_b", Description: "new"}, {Name: "gh_c"}})
	if names(c.Changed) != "[gh_b gh_c]" || len(c.Removed) != 0 {
		t.Errorf("second sync: %+v", c)
	}
	c = r.SyncSource("gh", []*Tool{{Name: "gh_c"}})
	if len(c.Changed) != 0 || fmt.Sprint(c.Removed) != "[gh_a gh_b]" {
		t.Errorf("third sync: %+v", c)
	}
	if got := names(r.All()); got != "[gh_c read_file]" {
		t.Errorf("registry = %s", got)
	}
}

// A name is its holder's: a server cannot take a built-in tool's name, nor
// another server's, and the refusal says who holds it. Once the holder
// drops the name, another server may have it.
func TestRegistry_SyncSourceNeverReplacesAnotherSource(t *testing.T) {
	r := NewRegistry()
	r.Register(&Tool{Name: "web_fetch", Description: "built in"})
	r.SyncSource("a", []*Tool{{Name: "a_b_c", Description: "from a"}})

	c := r.SyncSource("web", []*Tool{{Name: "web_fetch", Description: "from web"}, {Name: "web_x"}})
	if len(c.Refused) != 1 || c.Refused[0].String() != "web_fetch (a built-in tool)" || names(c.Changed) != "[web_x]" {
		t.Errorf("built-in name: %+v", c)
	}
	c = r.SyncSource("a_b", []*Tool{{Name: "a_b_c", Description: "from a_b"}})
	if len(c.Refused) != 1 || c.Refused[0].String() != "a_b_c (held by MCP server a)" {
		t.Errorf("another server's name: %+v", c)
	}
	if got, _ := r.Get("web_fetch"); got.Description != "built in" {
		t.Errorf("web_fetch replaced: %q", got.Description)
	}
	if got, _ := r.Get("a_b_c"); got.Description != "from a" {
		t.Errorf("a_b_c replaced: %q", got.Description)
	}

	// The refused server's sync removes nothing of the holder's.
	r.SyncSource("a_b", nil)
	if _, ok := r.Get("a_b_c"); !ok {
		t.Error("a server removed a tool it did not hold")
	}

	r.SyncSource("a", nil)
	c = r.SyncSource("a_b", []*Tool{{Name: "a_b_c", Description: "from a_b"}})
	if len(c.Refused) != 0 || names(c.Changed) != "[a_b_c]" {
		t.Errorf("after a let it go: %+v", c)
	}
}

// Activities read the registry while servers' tools come and go (go test
// -race reports a data race otherwise).
func TestRegistry_ConcurrentReadsAndWrites(t *testing.T) {
	r := NewRegistry()
	r.Register(&Tool{Name: "builtin", Execute: func(context.Context, json.RawMessage) (string, error) { return "ok", nil }})
	done := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				r.List()
				r.All()
				r.WorkflowTools()
				if _, ok := r.Get("builtin"); !ok {
					t.Error("built-in tool lost")
					return
				}
				if out, err := r.Execute(context.Background(), "builtin", nil); err != nil || out != "ok" {
					t.Errorf("execute = %q, %v", out, err)
					return
				}
				r.Execute(context.Background(), "s_t1", nil) // there or not
			}
		}()
	}
	exec := func(context.Context, json.RawMessage) (string, error) { return "", nil }
	for i := range 500 {
		var tools []*Tool
		for j := range i % 7 {
			tools = append(tools, &Tool{Name: fmt.Sprintf("s_t%d", j), Execute: exec})
		}
		r.SyncSource("s", tools)
		if i%100 == 0 {
			r.Retain([]string{"*"})
		}
	}
	close(done)
	wg.Wait()
}

// implement_feature says where it runs, as analyze_repo does, and gets the
// call's context (whose machine, which turn its files go to).
func TestImplementFeatureSaysWhereItRuns(t *testing.T) {
	for _, c := range []struct {
		route     CodingRoute
		want, not string
	}{
		{CodingRoute{Machines: true, Fallback: true}, "lets its runs push", "fails at once"},
		{CodingRoute{Machines: true}, "fails at once saying so", "coding workers"},
		{CodingRoute{Fallback: true}, "installation's coding workers", "own machine"},
	} {
		r := NewRegistry()
		RegisterImplementFeatureTool(r, func() {}, c.route)
		tl, ok := r.Get("implement_feature")
		if !ok || !tl.NeedsCallContext || !tl.Sensitive || !strings.Contains(tl.Description, c.want) || strings.Contains(tl.Description, c.not) {
			t.Errorf("%+v: %q", c.route, tl.Description)
		}
	}
}

// analyze_repo says where it runs: the user's machine, the installation's
// fallback, or both.
func TestAnalyzeRepoSaysWhereItRuns(t *testing.T) {
	for _, c := range []struct {
		route     CodingRoute
		want, not string
	}{
		{CodingRoute{Machines: true, Fallback: true}, "user's own machine when one is connected", "fails at once"},
		{CodingRoute{Machines: true}, "fails at once saying so", "coding workers"},
		{CodingRoute{Fallback: true}, "installation's coding workers", "own machine"},
	} {
		r := NewRegistry()
		RegisterAnalyzeRepoTool(r, func() {}, c.route)
		tl, ok := r.Get("analyze_repo")
		if !ok || !tl.NeedsCallContext || !strings.Contains(tl.Description, c.want) || strings.Contains(tl.Description, c.not) {
			t.Errorf("%+v: %q", c.route, tl.Description)
		}
	}
}

// A tool with a limit of its own outlasts it: stopped by Temporal first, a
// call would return nothing of what it did.
func TestTool_TimeoutCoversItsOwnLimit(t *testing.T) {
	r := NewRegistry()
	RegisterExecTool(r, t.TempDir(), nil, nil, nil)
	RegisterWebTools(r)
	RegisterGrepTool(r, t.TempDir())
	f := newFakeMCP(t, nil)
	mcpTools := discover(t, NewMCPClient(f.config("srv")))
	get := func(name string) *Tool {
		tl, ok := r.Get(name)
		if !ok {
			t.Fatalf("%s not registered", name)
		}
		return tl
	}

	for _, c := range []struct {
		tool  *Tool
		limit time.Duration
	}{
		// The command, then publishing its files.
		{get("exec"), execMaxTimeout + publishBudget},
		{get("web_fetch"), fetchTimeout},
		{get("grep"), grepTimeout},
		{mcpTools[0], mcpCallTimeout},
	} {
		if c.tool.Timeout != c.limit+TimeoutMargin {
			t.Errorf("%s: timeout %s, want its limit %s and the margin", c.tool.Name, c.tool.Timeout, c.limit)
		}
	}
}
