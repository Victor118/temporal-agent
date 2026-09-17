package store

import (
	"encoding/json"
	"fmt"
)

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
	Role       Role        `json:"role"`
	Content    string      `json:"content,omitempty"`
	ToolCalls  []ToolCall  `json:"tool_calls,omitempty"`
	ToolResult *ToolResult `json:"tool_result,omitempty"`
}

type MessageWithID struct {
	ID int64 `json:"id"`
	Message
}

// TurnMessageKey is the idempotency key of the index-th message produced by a
// conversation turn. turnKey identifies the turn globally, not just within one
// workflow run: a session whose workflow timed out is resumed under the same
// session ID with its turn counter back to zero, so a counter alone would make
// the new turn 1 collide with the old one and drop its messages.
func TurnMessageKey(turnKey string, index int) string {
	return fmt.Sprintf("%s:%d", turnKey, index)
}

// ScheduledMessageKey is the idempotency key of a scheduled task result. A cron
// schedule fires repeatedly under the same ID, so the run timestamp is part of
// the key: retries of one run dedupe, successive runs do not.
func ScheduledMessageKey(scheduleID string, runUnixMilli int64) string {
	return fmt.Sprintf("sched:%s:%d", scheduleID, runUnixMilli)
}
