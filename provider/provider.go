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
	// Model is the model that answered, as its API names it; empty when it
	// does not say.
	Model string `json:"model,omitempty"`
	// Usage is what the call took, in tokens; nil when the API does not say.
	Usage *Usage `json:"usage,omitempty"`
}

// Usage is what a call took, in tokens: what it read (the cached part apart)
// and what it wrote.
type Usage struct {
	InputTokens              int64 `json:"input_tokens"`
	OutputTokens             int64 `json:"output_tokens"`
	CacheCreationInputTokens int64 `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     int64 `json:"cache_read_input_tokens,omitempty"`
}

// ErrContextTooLong is in the error of a request the model refuses for its
// size: the conversation no longer fits its context window. It comes wrapped
// in a PermanentAPIError: the same request would be refused again.
var ErrContextTooLong = errors.New("context too long for the model")

// PermanentAPIError wraps API errors that should not be retried (auth, billing, bad request, etc.)
type PermanentAPIError struct {
	Err error
	// Credentials: the API refused the key itself, or its account has no
	// credit left. Every request with it would be refused, whatever it holds.
	Credentials bool
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
