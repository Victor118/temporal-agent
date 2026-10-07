package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const anthropicAPIURL = "https://api.anthropic.com/v1/messages"

type AnthropicProvider struct {
	apiKey       string
	defaultModel string
	client       *http.Client
	url          string // the Messages API; a test server in tests
}

// NewAnthropicProvider creates a provider. defaultModel is used for requests
// that don't name a model, so a config change applies to running sessions.
func NewAnthropicProvider(apiKey, defaultModel string) *AnthropicProvider {
	return &AnthropicProvider{
		apiKey:       apiKey,
		defaultModel: defaultModel,
		client:       &http.Client{},
		url:          anthropicAPIURL,
	}
}

// transientStatus tells an API error worth retrying — a timeout (408), the
// rate limit (429), the API failing or overloaded (5xx, 529 included) — from
// one the same request would get again: a bad request, a refused key, an
// unknown model, no credit left (400, 401, 403, 404…).
func transientStatus(code int) bool {
	return code == http.StatusRequestTimeout || code == http.StatusTooManyRequests || code >= 500
}

// anthropicError is the body of an API error.
type anthropicError struct {
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

// contextOverflow tells an API error that refuses the request for its size:
// a prompt longer than the model's window ("prompt is too long: … tokens >
// … maximum"), a prompt that leaves no room for max_tokens ("input length
// and `max_tokens` exceed context limit: … + … > …", the one a growing
// conversation meets first) — both invalid_request_error — or a request over
// the API's size limit (413, request_too_large).
func contextOverflow(status int, body []byte) bool {
	var e anthropicError
	if json.Unmarshal(body, &e) != nil {
		return status == http.StatusRequestEntityTooLarge
	}
	switch {
	case status == http.StatusRequestEntityTooLarge || e.Error.Type == "request_too_large":
		return true
	case e.Error.Type == "invalid_request_error":
		msg := strings.ToLower(e.Error.Message)
		return strings.Contains(msg, "prompt is too long") || strings.Contains(msg, "exceed context limit")
	}
	return false
}

// refusedCredentials tells an API error that refuses the key itself (401,
// 403), or the account behind it: no credit left. Any request with that key
// would get it again, whatever it holds.
func refusedCredentials(status int, body []byte) bool {
	switch status {
	case http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusForbidden:
		return true
	}
	var e anthropicError
	if json.Unmarshal(body, &e) != nil {
		return false
	}
	switch e.Error.Type {
	case "authentication_error", "permission_error", "billing_error":
		return true
	}
	return strings.Contains(strings.ToLower(e.Error.Message), "credit balance")
}

// resolveModel returns the requested model, or the provider default.
func (p *AnthropicProvider) resolveModel(requested string) (string, error) {
	if requested != "" {
		return requested, nil
	}
	if p.defaultModel != "" {
		return p.defaultModel, nil
	}
	return "", &PermanentAPIError{Err: fmt.Errorf("no model: set LLM_MODEL or pass a model")}
}

// Anthropic API types

type anthropicRequest struct {
	Model     string             `json:"model"`
	System    interface{}        `json:"system,omitempty"` // string or []anthropicContentBlock for cache_control
	Messages  []anthropicMessage `json:"messages"`
	Tools     []anthropicTool    `json:"tools,omitempty"`
	MaxTokens int                `json:"max_tokens"`
}

type anthropicMessage struct {
	Role    string      `json:"role"`
	Content interface{} `json:"content"`
}

type anthropicTool struct {
	Name         string                 `json:"name"`
	Description  string                 `json:"description"`
	InputSchema  json.RawMessage        `json:"input_schema"`
	CacheControl *anthropicCacheControl `json:"cache_control,omitempty"`
}

type anthropicCacheControl struct {
	Type string `json:"type"`
}

type anthropicResponse struct {
	Content    []anthropicContentBlock `json:"content"`
	StopReason string                  `json:"stop_reason"`
	Model      string                  `json:"model"`
	Usage      *Usage                  `json:"usage"`
}

type anthropicContentBlock struct {
	Type  string          `json:"type"`
	Text  string          `json:"text,omitempty"`
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
}

func (p *AnthropicProvider) Chat(ctx context.Context, request ChatRequest) (ChatResponse, error) {
	// Convert messages to Anthropic format
	messages := make([]anthropicMessage, 0, len(request.Messages))
	for _, msg := range request.Messages {
		aMsg := convertToAnthropicMessage(msg)
		messages = append(messages, aMsg)
	}

	// Convert tools
	tools := make([]anthropicTool, 0, len(request.Tools))
	for _, t := range request.Tools {
		at := anthropicTool{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: t.InputSchema,
		}
		if t.CacheBreakpoint {
			at.CacheControl = &anthropicCacheControl{Type: "ephemeral"}
		}
		tools = append(tools, at)
	}

	maxTokens := request.MaxTokens
	if maxTokens == 0 {
		maxTokens = 4096
	}

	model, err := p.resolveModel(request.Model)
	if err != nil {
		return ChatResponse{}, err
	}

	// System prompt: use structured content block if caching is requested
	var system interface{}
	if request.System != "" {
		if request.CacheSystem {
			system = []map[string]interface{}{
				{
					"type":          "text",
					"text":          request.System,
					"cache_control": map[string]string{"type": "ephemeral"},
				},
			}
		} else {
			system = request.System
		}
	}

	reqBody := anthropicRequest{
		Model:     model,
		System:    system,
		Messages:  messages,
		Tools:     tools,
		MaxTokens: maxTokens,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return ChatResponse{}, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", p.url, bytes.NewReader(body))
	if err != nil {
		return ChatResponse{}, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-API-Key", p.apiKey)
	httpReq.Header.Set("Anthropic-Version", "2023-06-01")
	httpReq.Header.Set("Anthropic-Beta", "prompt-caching-2024-07-31")

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return ChatResponse{}, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return ChatResponse{}, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("anthropic API error (status %d): %s", resp.StatusCode, string(respBody))
		if contextOverflow(resp.StatusCode, respBody) {
			return ChatResponse{}, &PermanentAPIError{Err: fmt.Errorf("%w: %w", ErrContextTooLong, err)}
		}
		if !transientStatus(resp.StatusCode) {
			return ChatResponse{}, &PermanentAPIError{Err: err, Credentials: refusedCredentials(resp.StatusCode, respBody)}
		}
		// Rate limited or overloaded (429, 529, 503), the API may say when
		// to come back.
		if d, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
			return ChatResponse{}, &RetryAfterError{Err: err, Delay: d}
		}
		return ChatResponse{}, err
	}

	var aResp anthropicResponse
	if err := json.Unmarshal(respBody, &aResp); err != nil {
		return ChatResponse{}, fmt.Errorf("unmarshal response: %w", err)
	}

	// Convert response
	result := ChatResponse{StopReason: aResp.StopReason, Model: aResp.Model, Usage: aResp.Usage}

	for _, block := range aResp.Content {
		switch block.Type {
		case "text":
			result.Content += block.Text
		case "tool_use":
			result.ToolCalls = append(result.ToolCalls, ToolCallInfo{
				ID:    block.ID,
				Name:  block.Name,
				Input: block.Input,
			})
		}
	}

	return result, nil
}

func convertToAnthropicMessage(msg ChatMessage) anthropicMessage {
	cacheCtl := cacheControlFor(msg.CacheBreakpoint)

	// If the message has tool calls, build content blocks
	if len(msg.ToolCalls) > 0 {
		var blocks []interface{}

		// Add text content if present
		var text string
		_ = json.Unmarshal(msg.Content, &text)
		if text != "" {
			blocks = append(blocks, map[string]interface{}{
				"type": "text",
				"text": text,
			})
		}

		for i, tc := range msg.ToolCalls {
			block := map[string]interface{}{
				"type":  "tool_use",
				"id":    tc.ID,
				"name":  tc.Name,
				"input": tc.Input,
			}
			// Cache breakpoint goes on the last content block
			if cacheCtl != nil && i == len(msg.ToolCalls)-1 {
				block["cache_control"] = cacheCtl
			}
			blocks = append(blocks, block)
		}
		return anthropicMessage{Role: msg.Role, Content: blocks}
	}

	// If this is a tool result message
	if msg.ToolResult != nil {
		block := map[string]interface{}{
			"type":        "tool_result",
			"tool_use_id": msg.ToolResult.ToolCallID,
			"content":     msg.ToolResult.Content,
			"is_error":    msg.ToolResult.IsError,
		}
		if cacheCtl != nil {
			block["cache_control"] = cacheCtl
		}
		blocks := []map[string]interface{}{block}
		return anthropicMessage{Role: "user", Content: blocks}
	}

	// Simple text message
	var text string
	if err := json.Unmarshal(msg.Content, &text); err != nil {
		// Content is already structured, pass through
		return anthropicMessage{Role: msg.Role, Content: msg.Content}
	}

	if cacheCtl != nil {
		blocks := []map[string]interface{}{
			{
				"type":          "text",
				"text":          text,
				"cache_control": cacheCtl,
			},
		}
		return anthropicMessage{Role: msg.Role, Content: blocks}
	}
	return anthropicMessage{Role: msg.Role, Content: text}
}

func cacheControlFor(breakpoint bool) map[string]string {
	if !breakpoint {
		return nil
	}
	return map[string]string{"type": "ephemeral"}
}
