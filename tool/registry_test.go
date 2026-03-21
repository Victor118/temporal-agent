package tool

import (
	"context"
	"encoding/json"
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
