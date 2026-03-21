package provider

import (
	"context"
	"encoding/json"
)

type LLMProvider interface {
	Chat(ctx context.Context, request ChatRequest) (ChatResponse, error)
}

type ChatRequest struct {
	Model     string           `json:"model"`
	System    string           `json:"system,omitempty"`
	Messages  []ChatMessage    `json:"messages"`
	Tools     []ToolDefinition `json:"tools,omitempty"`
	MaxTokens int              `json:"max_tokens"`
}

type ChatMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	ToolCalls  []ToolCallInfo  `json:"tool_calls,omitempty"`
	ToolResult *ToolResultInfo `json:"tool_result,omitempty"`
}

type ToolCallInfo struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

type ToolResultInfo struct {
	ToolCallID string `json:"tool_call_id"`
	Content    string `json:"content"`
	IsError    bool   `json:"is_error,omitempty"`
}

type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type ChatResponse struct {
	Content    string         `json:"content"`
	ToolCalls  []ToolCallInfo `json:"tool_calls,omitempty"`
	StopReason string         `json:"stop_reason"`
}
