package store

import "encoding/json"

type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

type ToolCall struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

type ToolResult struct {
	ToolCallID string `json:"tool_call_id"`
	Content    string `json:"content"`
	IsError    bool   `json:"is_error,omitempty"`
}

type Message struct {
	Role       Role         `json:"role"`
	Content    string       `json:"content,omitempty"`
	ToolCalls  []ToolCall   `json:"tool_calls,omitempty"`
	ToolResult *ToolResult  `json:"tool_result,omitempty"`
}

type MessageWithID struct {
	ID int64 `json:"id"`
	Message
}
