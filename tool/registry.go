package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
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
	Name         string          `json:"name"`
	Description  string          `json:"description"`
	InputSchema  json.RawMessage `json:"input_schema"`
	Kind         ToolKind        `json:"-"`
	Execute      ExecuteFunc     `json:"-"`
	WorkflowFunc  interface{} `json:"-"` // Workflow function for ToolKindWorkflow
	TaskQueue     string     `json:"-"` // Target task queue for workflow tools
	FireAndForget bool       `json:"-"` // If true, don't wait for workflow result
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

func (r *Registry) List() []provider.ToolDefinition {
	defs := make([]provider.ToolDefinition, 0, len(r.tools))
	for _, t := range r.tools {
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
