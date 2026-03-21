package tool

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MCPServerConfig describes an MCP server to connect to.
type MCPServerConfig struct {
	Name     string `json:"name"`      // Prefix for tool names (e.g. "github")
	URL      string `json:"url"`       // Base URL of the MCP server
	APIKey   string `json:"api_key"`   // Optional auth token
	Transport string `json:"transport"` // "http" (default) or "sse"
}

// MCPClient handles communication with a single MCP server.
type MCPClient struct {
	config MCPServerConfig
	client *http.Client
	nextID atomic.Int64
}

// JSON-RPC types for MCP protocol

type jsonRPCRequest struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      int64       `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type mcpToolInfo struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type mcpToolListResult struct {
	Tools []mcpToolInfo `json:"tools"`
}

type mcpCallToolParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type mcpCallToolResult struct {
	Content []mcpContentBlock `json:"content"`
	IsError bool              `json:"isError,omitempty"`
}

type mcpContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

// NewMCPClient creates a client for the given MCP server.
func NewMCPClient(config MCPServerConfig) *MCPClient {
	if config.Transport == "" {
		config.Transport = "http"
	}
	return &MCPClient{
		config: config,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// DiscoverAndRegister connects to the MCP server, discovers tools, and registers them in the registry.
func (c *MCPClient) DiscoverAndRegister(ctx context.Context, registry *Registry) error {
	tools, err := c.listTools(ctx)
	if err != nil {
		return fmt.Errorf("mcp %s: discover tools: %w", c.config.Name, err)
	}

	for _, t := range tools {
		mcpTool := t // capture
		prefixedName := c.config.Name + "_" + mcpTool.Name

		registry.Register(&Tool{
			Name:        prefixedName,
			Description: fmt.Sprintf("[MCP:%s] %s", c.config.Name, mcpTool.Description),
			InputSchema: mcpTool.InputSchema,
			Kind:        ToolKindMCP,
			Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
				return c.callTool(ctx, mcpTool.Name, input)
			},
		})
	}

	return nil
}

func (c *MCPClient) listTools(ctx context.Context) ([]mcpToolInfo, error) {
	resp, err := c.rpcCall(ctx, "tools/list", nil)
	if err != nil {
		return nil, err
	}

	var result mcpToolListResult
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, fmt.Errorf("parse tools/list result: %w", err)
	}

	return result.Tools, nil
}

func (c *MCPClient) callTool(ctx context.Context, name string, arguments json.RawMessage) (string, error) {
	params := mcpCallToolParams{
		Name:      name,
		Arguments: arguments,
	}

	resp, err := c.rpcCall(ctx, "tools/call", params)
	if err != nil {
		return "", err
	}

	var result mcpCallToolResult
	if err := json.Unmarshal(resp, &result); err != nil {
		return "", fmt.Errorf("parse tools/call result: %w", err)
	}

	// Concatenate text content blocks
	var sb strings.Builder
	for _, block := range result.Content {
		if block.Type == "text" {
			sb.WriteString(block.Text)
		}
	}

	text := sb.String()
	if result.IsError {
		return "", fmt.Errorf("mcp tool error: %s", text)
	}

	return text, nil
}

func (c *MCPClient) rpcCall(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	switch c.config.Transport {
	case "sse":
		return c.rpcCallSSE(ctx, method, params)
	default:
		return c.rpcCallHTTP(ctx, method, params)
	}
}

func (c *MCPClient) rpcCallHTTP(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      c.nextID.Add(1),
		Method:  method,
		Params:  params,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.config.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	if c.config.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("mcp http call: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("mcp read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("mcp server error (status %d): %s", resp.StatusCode, string(respBody))
	}

	var rpcResp jsonRPCResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return nil, fmt.Errorf("mcp parse response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("mcp rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return rpcResp.Result, nil
}

func (c *MCPClient) rpcCallSSE(ctx context.Context, method string, params interface{}) (json.RawMessage, error) {
	reqBody := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      c.nextID.Add(1),
		Method:  method,
		Params:  params,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.config.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "text/event-stream")
	if c.config.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.config.APIKey)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("mcp sse call: %w", err)
	}
	defer resp.Body.Close()

	// Parse SSE stream, collect the JSON-RPC response from "message" events
	scanner := bufio.NewScanner(resp.Body)
	var resultData string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			resultData = strings.TrimPrefix(line, "data: ")
			// Try to parse — the last valid JSON-RPC response is our answer
			var rpcResp jsonRPCResponse
			if json.Unmarshal([]byte(resultData), &rpcResp) == nil && rpcResp.ID == reqBody.ID {
				if rpcResp.Error != nil {
					return nil, fmt.Errorf("mcp rpc error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
				}
				return rpcResp.Result, nil
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("mcp sse read: %w", err)
	}

	return nil, fmt.Errorf("mcp sse: no valid response received")
}

// RegisterMCPServers discovers and registers tools from all configured MCP servers.
func RegisterMCPServers(ctx context.Context, registry *Registry, servers []MCPServerConfig) []error {
	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)

	for _, srv := range servers {
		wg.Add(1)
		go func(s MCPServerConfig) {
			defer wg.Done()
			client := NewMCPClient(s)
			if err := client.DiscoverAndRegister(ctx, registry); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(srv)
	}

	wg.Wait()
	return errs
}
