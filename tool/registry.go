package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"runtime"
	"sort"
	"strings"

	"github.com/victor/temporal-agent/provider"
)

type ToolKind string

const (
	ToolKindActivity ToolKind = "activity"
	ToolKindWorkflow ToolKind = "workflow"
	ToolKindMCP      ToolKind = "mcp"
)

type ExecuteFunc func(ctx context.Context, input json.RawMessage) (string, error)

type Tool struct {
	Name          string          `json:"name"`
	Description   string          `json:"description"`
	InputSchema   json.RawMessage `json:"input_schema"`
	Kind          ToolKind        `json:"-"`
	Execute       ExecuteFunc     `json:"-"`
	WorkflowFunc  interface{}     `json:"-"` // Workflow function for ToolKindWorkflow
	TaskQueue     string          `json:"-"` // Target task queue for workflow tools
	FireAndForget bool            `json:"-"` // If true, don't wait for workflow result
}

// WorkflowName returns the function name used by Temporal to identify the workflow.
func (t *Tool) WorkflowName() string {
	if t.WorkflowFunc == nil {
		return ""
	}
	fullName := runtime.FuncForPC(reflect.ValueOf(t.WorkflowFunc).Pointer()).Name()
	// Extract short name (e.g. "AgentWorkflow" from "github.com/.../workflow.AgentWorkflow")
	if idx := strings.LastIndex(fullName, "."); idx >= 0 {
		return fullName[idx+1:]
	}
	return fullName
}

// SchemaHash identifies the tool's contract (description, input schema, kind,
// workflow). Two workers exposing the same tool must produce the same hash.
func (t *Tool) SchemaHash() string {
	h := sha256.New()
	for _, part := range []string{
		t.Description,
		string(t.InputSchema),
		string(t.Kind),
		t.WorkflowName(),
		fmt.Sprint(t.FireAndForget),
	} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// MatchAny reports whether name matches at least one glob (path.Match syntax).
// Invalid patterns never match; validate them when loading config.
func MatchAny(globs []string, name string) bool {
	for _, g := range globs {
		if ok, _ := path.Match(g, name); ok {
			return true
		}
	}
	return false
}

type Registry struct {
	tools map[string]*Tool
}

func NewRegistry() *Registry {
	return &Registry{
		tools: make(map[string]*Tool),
	}
}

func (r *Registry) Register(t *Tool) {
	r.tools[t.Name] = t
}

func (r *Registry) Get(name string) (*Tool, bool) {
	t, ok := r.tools[name]
	return t, ok
}

var emptySchema = json.RawMessage(`{"type":"object","properties":{}}`)

// List returns tool definitions sorted by name. The order must be stable:
// tools are the start of the LLM prompt prefix, so any reordering invalidates
// the prompt cache (tools, system prompt and history).
func (r *Registry) List() []provider.ToolDefinition {
	names := r.sortedNames()
	defs := make([]provider.ToolDefinition, 0, len(r.tools))
	for _, name := range names {
		t := r.tools[name]
		schema := t.InputSchema
		if len(schema) == 0 {
			schema = emptySchema
		}
		defs = append(defs, provider.ToolDefinition{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: schema,
		})
	}
	return defs
}

// All returns the registered tools sorted by name.
func (r *Registry) All() []*Tool {
	names := r.sortedNames()
	tools := make([]*Tool, len(names))
	for i, name := range names {
		tools[i] = r.tools[name]
	}
	return tools
}

// Retain removes every tool whose name matches none of the globs and returns
// the removed names, sorted.
func (r *Registry) Retain(globs []string) []string {
	var removed []string
	for _, name := range r.sortedNames() {
		if !MatchAny(globs, name) {
			delete(r.tools, name)
			removed = append(removed, name)
		}
	}
	return removed
}

func (r *Registry) sortedNames() []string {
	names := make([]string, 0, len(r.tools))
	for name := range r.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// WorkflowTools returns all tools of kind workflow, for registration at worker startup.
func (r *Registry) WorkflowTools() []*Tool {
	var tools []*Tool
	for _, t := range r.tools {
		if t.Kind == ToolKindWorkflow && t.WorkflowFunc != nil {
			tools = append(tools, t)
		}
	}
	return tools
}

func (r *Registry) Execute(ctx context.Context, name string, input json.RawMessage) (string, error) {
	t, ok := r.tools[name]
	if !ok {
		return "", fmt.Errorf("unknown tool: %s", name)
	}
	return t.Execute(ctx, input)
}
