package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MCPServerConfig describes an MCP server to connect to.
type MCPServerConfig struct {
	Name      string `json:"name"`      // Prefix for tool names (e.g. "github")
	URL       string `json:"url"`       // Base URL of the MCP server
	APIKey    string `json:"api_key"`   // Optional auth token, sent as a Bearer token
	Transport string `json:"transport"` // "http" (Streamable HTTP, default) or "sse" (HTTP+SSE, 2024-11-05)
}

// mcpVersions are the MCP protocol versions this client speaks, latest
// first: it asks for the first, and accepts any of them in answer.
var mcpVersions = []string{"2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"}

const (
	// mcpRequestTimeout bounds a handshake and a tools/list.
	mcpRequestTimeout = 30 * time.Second
	// mcpCallTimeout bounds a tools/call whose context has no deadline (an
	// activity's has one).
	mcpCallTimeout = 2 * time.Minute
	// mcpMaxPages bounds a paginated tools/list.
	mcpMaxPages = 100
)

// mcpConn is one MCP session over a transport, as MCPClient uses it.
type mcpConn interface {
	// call sends a request and returns its result; a response larger than
	// limit bytes is an error that wraps errTooLarge.
	call(ctx context.Context, method string, params any, limit int) (json.RawMessage, error)
	notify(ctx context.Context, method string, params any) error
	setVersion(v string) // the negotiated version, before the conn is shared
	alive() bool         // false once the conn cannot carry a request again
	close()
}

// MCPClient talks to one MCP server. It opens a session on first use (the
// initialize handshake), keeps it for every request after, and opens a new
// one when the server forgot it. Safe for concurrent use.
type MCPClient struct {
	config  MCPServerConfig
	http    *http.Client
	ids     atomic.Int64
	timeout time.Duration // bounds a handshake and a tools/list

	handshake chan struct{} // one handshake at a time
	mu        sync.Mutex
	conn      mcpConn
}

// NewMCPClient creates a client for the given MCP server.
func NewMCPClient(config MCPServerConfig) *MCPClient {
	if config.Transport == "" {
		config.Transport = "http"
	}
	return &MCPClient{
		config: config,
		// No overall timeout: an event stream stays open. Each request
		// has its own deadline instead.
		http:      &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()},
		timeout:   mcpRequestTimeout,
		handshake: make(chan struct{}, 1),
	}
}

// Name is the server's name in the worker config.
func (c *MCPClient) Name() string { return c.config.Name }

// Close ends the session, if one is open.
func (c *MCPClient) Close() {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()
	if conn != nil {
		conn.close()
	}
}

// Discover asks the server for its tools, named after the server. It
// registers nothing: what to do with them is the caller's decision. A list
// this worker cannot take whole (see checkTools) is refused whole, with an
// error that wraps errToolsRefused.
func (c *MCPClient) Discover(ctx context.Context) ([]*Tool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	infos, err := c.listTools(ctx)
	if err == nil {
		err = checkTools(c.config.Name, infos)
	}
	if err != nil {
		return nil, fmt.Errorf("mcp %s: discover tools: %w", c.config.Name, err)
	}

	tools := make([]*Tool, 0, len(infos))
	for _, info := range infos {
		tools = append(tools, &Tool{
			Name:        exposedName(c.config.Name, info.Name),
			Description: fmt.Sprintf("[MCP:%s] %s", c.config.Name, info.Description),
			InputSchema: info.InputSchema,
			Kind:        ToolKindMCP,
			Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
				return c.callTool(ctx, info.Name, input) // the server's own name
			},
		})
	}
	return tools, nil
}

// What a server may give in a tools/list. Its tools go to every agent the
// allowlist lets see them, in each LLM request: a name the model's API
// refuses, or a list too big, would break all those agents, not just this
// server's tools.
const (
	mcpMaxTools       = 500      // tools a server may give
	mcpMaxDescription = 8 << 10  // bytes of a tool's description
	mcpMaxSchema      = 64 << 10 // bytes of a tool's input schema
	mcpMaxList        = 4 << 20  // bytes of all the pages of a tools/list
	mcpMaxHandshake   = 1 << 20  // bytes of an initialize response
)

// errToolsRefused: the server answered tools/list with a list this worker
// does not take. Nothing of it is registered; the server keeps the tools it
// had.
var errToolsRefused = errors.New("tools list refused")

// toolNamePattern is what a tool name may be for the model's API.
var toolNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// exposedName is the name the model knows a server's tool by: the server's
// name, then the tool's with every character a name may not have replaced by
// _ (MCP allows "search.web"). Calls still use the server's own name.
func exposedName(server, name string) string {
	return server + "_" + strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			return r
		}
		return '_'
	}, name)
}

// checkTools refuses a list that has a tool this worker cannot take: no
// name, a name that does not fit the model's API once exposed, two tools
// exposed under one name, a description or schema too big, a schema that is
// not a JSON object. One such tool refuses the list: half a server's tools
// would be a list that moves with whatever the server sends. (listTools
// already bounded the number of tools and the bytes.)
func checkTools(server string, infos []mcpToolInfo) error {
	exposed := make(map[string]string, len(infos))
	for _, info := range infos {
		if info.Name == "" {
			return fmt.Errorf("%w: a tool has no name", errToolsRefused)
		}
		name := exposedName(server, info.Name)
		if !toolNamePattern.MatchString(name) {
			return fmt.Errorf("%w: tool %.100q: exposed as %.100q, which is not 1 to 64 letters, digits, _ or -", errToolsRefused, info.Name, name)
		}
		if other, ok := exposed[name]; ok {
			return fmt.Errorf("%w: tools %.100q and %.100q are both exposed as %q", errToolsRefused, other, info.Name, name)
		}
		exposed[name] = info.Name
		if len(info.Description) > mcpMaxDescription {
			return fmt.Errorf("%w: tool %q: description of %d bytes, at most %d", errToolsRefused, info.Name, len(info.Description), mcpMaxDescription)
		}
		if len(info.InputSchema) > mcpMaxSchema {
			return fmt.Errorf("%w: tool %q: input schema of %d bytes, at most %d", errToolsRefused, info.Name, len(info.InputSchema), mcpMaxSchema)
		}
		if s := bytes.TrimSpace(info.InputSchema); len(s) == 0 || s[0] != '{' {
			return fmt.Errorf("%w: tool %q: input schema is not a JSON object", errToolsRefused, info.Name)
		}
	}
	return nil
}

type mcpToolInfo struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

type mcpListParams struct {
	Cursor string `json:"cursor,omitempty"`
}

type mcpToolListResult struct {
	Tools      []mcpToolInfo `json:"tools"`
	NextCursor string        `json:"nextCursor,omitempty"`
}

// listTools reads every page of tools/list. The pages share one budget of
// bytes, which bounds each one as it is read, and one of tools.
func (c *MCPClient) listTools(ctx context.Context) ([]mcpToolInfo, error) {
	var all []mcpToolInfo
	var params any // the first page takes no cursor
	budget := mcpMaxList
	for range mcpMaxPages {
		raw, err := c.request(ctx, "tools/list", params, budget)
		if errors.Is(err, errTooLarge) {
			return nil, fmt.Errorf("%w: more than %d bytes", errToolsRefused, mcpMaxList)
		}
		if err != nil {
			return nil, err
		}
		budget -= len(raw)
		var page mcpToolListResult
		if err := json.Unmarshal(raw, &page); err != nil {
			return nil, fmt.Errorf("parse tools/list result: %w", err)
		}
		all = append(all, page.Tools...)
		if len(all) > mcpMaxTools {
			return nil, fmt.Errorf("%w: more than %d tools", errToolsRefused, mcpMaxTools)
		}
		if page.NextCursor == "" {
			return all, nil
		}
		if budget <= 0 {
			return nil, fmt.Errorf("%w: more than %d bytes", errToolsRefused, mcpMaxList)
		}
		params = mcpListParams{Cursor: page.NextCursor}
	}
	return nil, fmt.Errorf("%w: more than %d pages", errToolsRefused, mcpMaxPages)
}

type mcpCallToolParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type mcpCallToolResult struct {
	Content           []mcpContentBlock `json:"content"`
	StructuredContent json.RawMessage   `json:"structuredContent,omitempty"`
	IsError           bool              `json:"isError,omitempty"`
}

type mcpContentBlock struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	MimeType string `json:"mimeType,omitempty"`
	URI      string `json:"uri,omitempty"`
	Resource *struct {
		URI  string `json:"uri"`
		Text string `json:"text,omitempty"`
	} `json:"resource,omitempty"`
}

func (c *MCPClient) callTool(ctx context.Context, name string, arguments json.RawMessage) (string, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, mcpCallTimeout)
		defer cancel()
	}
	if len(arguments) == 0 || string(arguments) == "null" {
		arguments = json.RawMessage(`{}`)
	}

	raw, err := c.request(ctx, "tools/call", mcpCallToolParams{Name: name, Arguments: arguments}, mcpMaxMessage)
	if err != nil {
		return "", fmt.Errorf("mcp %s: %w", c.config.Name, err)
	}
	var result mcpCallToolResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("mcp %s: parse tools/call result: %w", c.config.Name, err)
	}
	text := result.text()
	if result.IsError {
		return "", fmt.Errorf("mcp tool error: %s", text)
	}
	return text, nil
}

// text is what the model reads of a result: the text blocks, a mention of
// the others, or the structured content when there is nothing else.
func (r mcpCallToolResult) text() string {
	var parts []string
	for _, b := range r.Content {
		switch {
		case b.Type == "text":
			parts = append(parts, b.Text)
		case b.Type == "resource" && b.Resource != nil && b.Resource.Text != "":
			parts = append(parts, b.Resource.Text)
		case b.Type == "resource_link":
			parts = append(parts, fmt.Sprintf("[resource: %s]", b.URI))
		default:
			parts = append(parts, fmt.Sprintf("[%s content omitted]", b.Type))
		}
	}
	if len(parts) == 0 && len(r.StructuredContent) > 0 {
		return string(r.StructuredContent)
	}
	return strings.Join(parts, "\n")
}

// request sends a request in the current session, opening one if needed. A
// server that forgot the session did not process the request: it is sent
// again, once, in a new session.
// limit bounds the response, in bytes.
func (c *MCPClient) request(ctx context.Context, method string, params any, limit int) (json.RawMessage, error) {
	for attempt := 0; ; attempt++ {
		conn, err := c.connect(ctx)
		if err != nil {
			return nil, err
		}
		res, err := conn.call(ctx, method, params, limit)
		if errors.Is(err, errSessionExpired) && attempt == 0 {
			c.drop(conn)
			continue
		}
		return res, err
	}
}

// connect returns the open session, or opens one. Concurrent callers wait
// for a single handshake.
func (c *MCPClient) connect(ctx context.Context) (mcpConn, error) {
	if conn := c.current(); conn != nil {
		return conn, nil
	}
	select {
	case c.handshake <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-c.handshake }()
	if conn := c.current(); conn != nil { // opened while this one waited
		return conn, nil
	}

	hctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	conn, err := c.initialize(hctx)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	return conn, nil
}

func (c *MCPClient) current() mcpConn {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil && !c.conn.alive() {
		c.conn.close()
		c.conn = nil
	}
	return c.conn
}

// drop forgets conn, unless another session already replaced it.
func (c *MCPClient) drop(conn mcpConn) {
	c.mu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	c.mu.Unlock()
	conn.close()
}

func (c *MCPClient) dial(ctx context.Context) (mcpConn, error) {
	switch c.config.Transport {
	case "sse":
		return dialSSE(ctx, c.http, c.config.URL, c.config.APIKey, &c.ids)
	case "http":
		return &streamableConn{http: c.http, url: c.config.URL, apiKey: c.config.APIKey, ids: &c.ids}, nil
	default:
		return nil, fmt.Errorf("unknown transport %q (http or sse)", c.config.Transport)
	}
}

type mcpImplementation struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type mcpInitializeParams struct {
	ProtocolVersion string            `json:"protocolVersion"`
	Capabilities    struct{}          `json:"capabilities"` // none: no sampling, roots or elicitation
	ClientInfo      mcpImplementation `json:"clientInfo"`
}

type mcpInitializeResult struct {
	ProtocolVersion string            `json:"protocolVersion"`
	ServerInfo      mcpImplementation `json:"serverInfo"`
}

// initialize opens a session: the initialize request, which negotiates the
// protocol version, then the initialized notification. Nothing else may be
// sent before.
func (c *MCPClient) initialize(ctx context.Context) (mcpConn, error) {
	conn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := conn.call(ctx, "initialize", mcpInitializeParams{
		ProtocolVersion: mcpVersions[0],
		ClientInfo:      mcpImplementation{Name: "temporal-agent", Version: "1"},
	}, mcpMaxHandshake)
	if err != nil {
		conn.close()
		var httpErr *mcpHTTPError
		if c.config.Transport == "http" && errors.As(err, &httpErr) && (httpErr.Status == http.StatusNotFound || httpErr.Status == http.StatusMethodNotAllowed) {
			return nil, fmt.Errorf("initialize: %w (an HTTP+SSE server takes transport: sse)", err)
		}
		return nil, fmt.Errorf("initialize: %w", err)
	}
	var res mcpInitializeResult
	if err := json.Unmarshal(raw, &res); err != nil {
		conn.close()
		return nil, fmt.Errorf("parse initialize result: %w", err)
	}
	if !slices.Contains(mcpVersions, res.ProtocolVersion) {
		conn.close()
		return nil, fmt.Errorf("initialize: unsupported protocol version %q (this client speaks %v)", res.ProtocolVersion, mcpVersions)
	}
	conn.setVersion(res.ProtocolVersion)
	if err := conn.notify(ctx, "notifications/initialized", nil); err != nil {
		conn.close()
		return nil, fmt.Errorf("notifications/initialized: %w", err)
	}
	return conn, nil
}
