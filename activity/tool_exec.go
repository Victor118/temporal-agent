package activity

import (
	"context"
	"encoding/json"

	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/tool"
)

type ToolActivities struct {
	Registry *tool.Registry // Tools this worker executes
	Catalog  *Catalog       // Tools published by all workers
}

type ExecuteToolInput struct {
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	SessionID string          `json:"session_id,omitempty"`
	AgentID   string          `json:"agent_id,omitempty"` // Agent calling the tool
	UserID    string          `json:"user_id,omitempty"`  // User the turn answers
}

type ExecuteToolOutput struct {
	Content string `json:"content"`
	IsError bool   `json:"is_error"`
}

// ToolResolution tells the workflow how to dispatch a tool call.
type ToolResolution struct {
	Kind          string `json:"kind"`
	WorkflowName  string `json:"workflow_name,omitempty"`
	TaskQueue     string `json:"task_queue"`
	FireAndForget bool   `json:"fire_and_forget,omitempty"`
	// AgentID is set on an agent_<id> tool: the agent the call delegates to.
	AgentID string `json:"agent_id,omitempty"`
	// PrivateInput: the call's input is hidden from the session's members.
	PrivateInput bool `json:"private_input,omitempty"`
	// NeedsCallContext: the workflow tool gets tool.CallContext in its input.
	NeedsCallContext bool `json:"needs_call_context,omitempty"`
}

type ListToolsInput struct {
	AgentID string `json:"agent_id"`
}

type ListToolsOutput struct {
	Tools       []provider.ToolDefinition `json:"tools"`       // Definitions sent to the LLM, sorted by name
	Resolutions map[string]ToolResolution `json:"resolutions"` // Tool name → dispatch info; absent = not allowed
}

// ListTools returns the tools the agent may use, from the worker's catalog.
func (a *ToolActivities) ListTools(ctx context.Context, input ListToolsInput) (ListToolsOutput, error) {
	return a.Catalog.AllowedTools(input.AgentID), nil
}

func (a *ToolActivities) ExecuteTool(ctx context.Context, input ExecuteToolInput) (ExecuteToolOutput, error) {
	if input.SessionID != "" {
		ctx = tool.WithSessionID(ctx, input.SessionID)
	}
	if input.AgentID != "" {
		ctx = tool.WithAgentID(ctx, input.AgentID)
	}
	if input.UserID != "" {
		ctx = tool.WithUserID(ctx, input.UserID)
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
