package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
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
