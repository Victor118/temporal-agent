package activity

import (
	"context"
	"encoding/json"

	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/tool"
)

type ToolActivities struct {
	Registry *tool.Registry
}

type ExecuteToolInput struct {
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	SessionID string          `json:"session_id,omitempty"`
}

type ExecuteToolOutput struct {
	Content string `json:"content"`
	IsError bool   `json:"is_error"`
}

type ResolveToolKindsInput struct {
	Names []string `json:"names"`
}

type ToolResolution struct {
	Kind          string `json:"kind"`
	WorkflowName  string `json:"workflow_name,omitempty"`
	TaskQueue     string `json:"task_queue,omitempty"`
	FireAndForget bool   `json:"fire_and_forget,omitempty"`
}

type ResolveToolKindsOutput struct {
	Tools map[string]ToolResolution `json:"tools"`
}

type ListToolsOutput struct {
	Tools []provider.ToolDefinition `json:"tools"`
}

func (a *ToolActivities) ListTools(ctx context.Context) (ListToolsOutput, error) {
	return ListToolsOutput{Tools: a.Registry.List()}, nil
}

func (a *ToolActivities) ResolveToolKinds(ctx context.Context, input ResolveToolKindsInput) (ResolveToolKindsOutput, error) {
	tools := make(map[string]ToolResolution, len(input.Names))
	for _, name := range input.Names {
		t, ok := a.Registry.Get(name)
		if !ok {
			tools[name] = ToolResolution{Kind: string(tool.ToolKindActivity)}
			continue
		}
		tools[name] = ToolResolution{
			Kind:          string(t.Kind),
			WorkflowName:  t.WorkflowName(),
			TaskQueue:     t.TaskQueue,
			FireAndForget: t.FireAndForget,
		}
	}
	return ResolveToolKindsOutput{Tools: tools}, nil
}

func (a *ToolActivities) ExecuteTool(ctx context.Context, input ExecuteToolInput) (ExecuteToolOutput, error) {
	if input.SessionID != "" {
		ctx = tool.WithSessionID(ctx, input.SessionID)
	}
	result, err := a.Registry.Execute(ctx, input.Name, input.Input)
	if err != nil {
		// Return error as content to the LLM, not as a Temporal error
		return ExecuteToolOutput{
			Content: err.Error(),
			IsError: true,
		}, nil
	}
	return ExecuteToolOutput{
		Content: result,
		IsError: false,
	}, nil
}
