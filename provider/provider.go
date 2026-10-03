package provider

import (
	"context"
	"encoding/json"
	"errors"
	"time"
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

	// CacheSystem marks the system prompt as a cache breakpoint.
	// Providers that support prompt caching will use this hint.
	CacheSystem bool `json:"cache_system,omitempty"`
}

type ChatMessage struct {
	Role       string          `json:"role"`
	Content    json.RawMessage `json:"content"`
	ToolCalls  []ToolCallInfo  `json:"tool_calls,omitempty"`
	ToolResult *ToolResultInfo `json:"tool_result,omitempty"`

	// CacheBreakpoint marks this message as a cache breakpoint.
	// Providers that support prompt caching will cache up to this point.
	CacheBreakpoint bool `json:"cache_breakpoint,omitempty"`
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

	// CacheBreakpoint marks this tool as a cache breakpoint.
	CacheBreakpoint bool `json:"cache_breakpoint,omitempty"`
}

type ChatResponse struct {
	Content    string         `json:"content"`
	ToolCalls  []ToolCallInfo `json:"tool_calls,omitempty"`
	StopReason string         `json:"stop_reason"`
}

// ErrContextTooLong is in the error of a request the model refuses for its
// size: the conversation no longer fits its context window. It comes wrapped
// in a PermanentAPIError: the same request would be refused again.
var ErrContextTooLong = errors.New("context too long for the model")

// PermanentAPIError wraps API errors that should not be retried (auth, billing, bad request, etc.)
type PermanentAPIError struct {
	Err error
}

func (e *PermanentAPIError) Error() string { return e.Err.Error() }
func (e *PermanentAPIError) Unwrap() error { return e.Err }

// RetryAfterError is a passing API error whose answer said how long to wait
// before the next attempt (Retry-After): an attempt sooner would be refused
// again. The caller decides how to wait; the provider knows no retry policy.
type RetryAfterError struct {
	Err   error
	Delay time.Duration
}

func (e *RetryAfterError) Error() string { return e.Err.Error() }
func (e *RetryAfterError) Unwrap() error { return e.Err }
