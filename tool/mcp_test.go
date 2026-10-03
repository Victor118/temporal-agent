package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// barrier lets the handlers sharing it go on once all of them have arrived.
// A request that never arrives makes the others give up after a while: the
// test fails instead of hanging, srv.Close included.
type barrier struct {
	arrived sync.WaitGroup
	all     chan struct{}
}

func newBarrier(n int) *barrier {
	b := &barrier{all: make(chan struct{})}
	b.arrived.Add(n)
	go func() { b.arrived.Wait(); close(b.all) }()
	return b
}

// arrive waits for the others, and reports whether they all came in time.
func (b *barrier) arrive() bool {
	b.arrived.Done()
	select {
	case <-b.all:
		return true
	case <-time.After(5 * time.Second):
		return false
	}
}

func discover(t *testing.T, c *MCPClient) []*Tool {
	t.Helper()
	tools, err := c.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return tools
}

func call(t *testing.T, tools []*Tool, name, input string) (string, error) {
	t.Helper()
	for _, tl := range tools {
		if tl.Name == name {
			return tl.Execute(context.Background(), json.RawMessage(input))
		}
	}
	t.Fatalf("no tool %s", name)
	return "", nil
}

// The client opens a session before anything else: initialize, then the
// initialized notification. Every request after carries the session ID the
// server gave and the negotiated version; the API key goes with all of them.
func TestMCPClient_Handshake(t *testing.T) {
	f := newFakeMCP(t, func(f *fakeMCP) { f.apiKey = "k" })
	c := NewMCPClient(f.config("srv"))

	tools := discover(t, c)
	if len(tools) != 1 || tools[0].Name != "srv_echo" || tools[0].Kind != ToolKindMCP {
		t.Fatalf("tools = %+v", tools)
	}
	if got, err := call(t, tools, "srv_echo", `{"a":1}`); err != nil || got != `echo {"a":1}` {
		t.Errorf("call = %q, %v", got, err)
	}

	f.mu.Lock()
	reqs := f.requests
	f.mu.Unlock()
	want := []string{"initialize", "notifications/initialized", "tools/list", "tools/call"}
	if got := f.methods(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("requests = %v, want %v", got, want)
	}
	if reqs[0].Session != "" || reqs[0].Version != "" {
		t.Errorf("initialize carried a session or a version: %+v", reqs[0])
	}
	for _, r := range reqs[1:] {
		if r.Session != "session-1" || r.Version != mcpVersions[0] {
			t.Errorf("%s: session %q, version %q", r.Method, r.Session, r.Version)
		}
	}
	for _, r := range reqs {
		if r.Auth != "Bearer k" {
			t.Errorf("%s: Authorization %q", r.Method, r.Auth)
		}
	}
}

// A server may answer a POST with an event stream: the client reads it to
// the response, skipping comments, notifications and an error without an ID
// (on a stream, it may be about any message), and answers a ping the server
// sends first.
func TestMCPClient_EventStreamResponse(t *testing.T) {
	f := newFakeMCP(t, func(f *fakeMCP) { f.sse, f.preface = true, true })
	tools := discover(t, NewMCPClient(f.config("srv")))
	if got, err := call(t, tools, "srv_echo", `{}`); err != nil || got != "echo {}" {
		t.Errorf("call = %q, %v", got, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.replies) == 0 || !strings.Contains(f.replies[0], `"id":"srv-1"`) || !strings.Contains(f.replies[0], `"result":{}`) {
		t.Errorf("replies to the server's ping = %v", f.replies)
	}
}

// A server may hold the stream open after the response: the call returns
// with the response all the same.
func TestMCPClient_StreamHeldOpenAfterTheResponse(t *testing.T) {
	f := newFakeMCP(t, func(f *fakeMCP) { f.sse, f.linger = true, true })
	c := NewMCPClient(f.config("srv"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tools, err := c.Discover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := tools[0].Execute(ctx, json.RawMessage(`{}`)); err != nil || got != "echo {}" {
		t.Errorf("call = %q, %v", got, err)
	}
}

// A server may close the stream before the response, having given its
// events IDs: the client resumes it from the last one.
func TestMCPClient_ResumesAClosedStream(t *testing.T) {
	f := newFakeMCP(t, func(f *fakeMCP) { f.sse, f.early = true, true })
	tools := discover(t, NewMCPClient(f.config("srv")))
	if got, err := call(t, tools, "srv_echo", `{}`); err != nil || got != "echo {}" {
		t.Errorf("call = %q, %v", got, err)
	}
}

// A server that forgot the session answers 404: the client opens a new one
// and sends the request again, once.
func TestMCPClient_NewSessionAfterExpiry(t *testing.T) {
	f := newFakeMCP(t, nil)
	tools := discover(t, NewMCPClient(f.config("srv")))
	f.expire()

	if got, err := call(t, tools, "srv_echo", `{}`); err != nil || got != "echo {}" {
		t.Fatalf("call after expiry = %q, %v", got, err)
	}
	if n := f.count("initialize"); n != 2 {
		t.Errorf("%d handshakes, want 2", n)
	}
	f.mu.Lock()
	last := f.requests[len(f.requests)-1]
	f.mu.Unlock()
	if last.Method != "tools/call" || last.Session != "session-2" {
		t.Errorf("last request %+v, want tools/call in session-2", last)
	}
}

// Many calls at once on a new client open a single session.
func TestMCPClient_ConcurrentCallsShareOneHandshake(t *testing.T) {
	f := newFakeMCP(t, nil)
	c := NewMCPClient(f.config("srv"))
	tools, err := c.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	c.Close() // no session: the calls below race to open one
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := tools[0].Execute(context.Background(), json.RawMessage(`{}`)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := f.count("initialize"); n != 2 {
		t.Errorf("%d handshakes, want 2 (discovery, then one for all the calls)", n)
	}
}

// The client asks for its latest version and accepts an older one it
// speaks, which it then sends in the header; a version it does not speak
// ends the handshake.
func TestMCPClient_NegotiatesTheVersion(t *testing.T) {
	older := newFakeMCP(t, func(f *fakeMCP) { f.version = "2025-03-26" })
	discover(t, NewMCPClient(older.config("srv")))
	older.mu.Lock()
	last := older.requests[len(older.requests)-1]
	older.mu.Unlock()
	if last.Version != "2025-03-26" {
		t.Errorf("version header %q after negotiating 2025-03-26", last.Version)
	}

	unknown := newFakeMCP(t, func(f *fakeMCP) { f.version = "1999-01-01" })
	_, err := NewMCPClient(unknown.config("srv")).Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unsupported protocol version") {
		t.Errorf("err = %v", err)
	}
	if n := unknown.count("tools/list"); n != 0 {
		t.Errorf("listed tools after a failed negotiation")
	}
}

func TestMCPClient_PaginatedList(t *testing.T) {
	f := newFakeMCP(t, func(f *fakeMCP) { f.pages = 2 })
	f.setTools("a", "b", "c", "d", "e")
	if n := len(discover(t, NewMCPClient(f.config("srv")))); n != 5 {
		t.Errorf("%d tools, want 5", n)
	}
	if n := f.count("tools/list"); n != 3 {
		t.Errorf("%d pages asked, want 3", n)
	}
}

// Every way a call can fail comes back as an error that says why.
func TestMCPClient_Errors(t *testing.T) {
	f := newFakeMCP(t, func(f *fakeMCP) { f.sse = true })
	c := NewMCPClient(f.config("srv"))
	f.setTools("echo", "fail")
	tools := discover(t, c)

	if _, err := call(t, tools, "srv_fail", `{}`); err == nil || !strings.Contains(err.Error(), "mcp tool error: it failed") {
		t.Errorf("isError result: %v", err)
	}

	f.mu.Lock()
	f.callError = &jsonRPCError{Code: -32602, Message: "bad arguments"}
	f.mu.Unlock()
	_, err := call(t, tools, "srv_echo", `{}`)
	var rpcErr *jsonRPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32602 || !strings.Contains(err.Error(), "bad arguments") {
		t.Errorf("rpc error: %v", err)
	}
	f.mu.Lock()
	f.callError = nil
	f.mu.Unlock()

	// A call that outlives its deadline ends with it.
	block := make(chan struct{})
	defer close(block)
	f.mu.Lock()
	f.block = block
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := tools[0].Execute(ctx, json.RawMessage(`{}`)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("hung call: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("the deadline was not kept")
	}

	// A wrong key: the status, and what the server said.
	wrongKey := newFakeMCP(t, func(f *fakeMCP) { f.apiKey = "right" })
	cfg := wrongKey.config("srv")
	cfg.APIKey = "wrong"
	_, err = NewMCPClient(cfg).Discover(context.Background())
	var httpErr *mcpHTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != http.StatusUnauthorized || !strings.Contains(err.Error(), "unauthorized") {
		t.Errorf("wrong key: %v", err)
	}

	// A server that is gone.
	gone := httptest.NewServer(http.NotFoundHandler())
	gone.Close()
	if _, err := NewMCPClient(MCPServerConfig{Name: "gone", URL: gone.URL}).Discover(context.Background()); err == nil || !strings.Contains(err.Error(), "mcp gone") {
		t.Errorf("unreachable server: %v", err)
	}
}

// The deprecated HTTP+SSE transport: the stream names the endpoint, the
// handshake comes first there too, and responses arrive on the stream among
// notifications and pings.
func TestMCPClient_LegacySSE(t *testing.T) {
	f := newFakeMCP(t, func(f *fakeMCP) { f.legacy, f.preface, f.apiKey = true, true, "k" })
	c := NewMCPClient(f.config("srv"))
	defer c.Close()
	tools := discover(t, c)
	if got, err := call(t, tools, "srv_echo", `{"x":2}`); err != nil || got != `echo {"x":2}` {
		t.Fatalf("call = %q, %v", got, err)
	}
	want := []string{"initialize", "notifications/initialized", "tools/list"}
	if got := f.methods(); fmt.Sprint(got[:3]) != fmt.Sprint(want) {
		t.Errorf("requests = %v, want %v first", got, want)
	}

	// The session's stream is lost: the next call opens a new one.
	f.expire()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := call(t, tools, "srv_echo", `{}`)
		if err == nil && got == "echo {}" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after a lost stream: %q, %v", got, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := f.count("initialize"); n != 2 {
		t.Errorf("%d handshakes, want 2", n)
	}
}

// The endpoint a legacy server names must be on its own origin: the API key
// goes there with every message.
func TestMCPClient_LegacyEndpointStaysOnTheOrigin(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: endpoint\ndata: http://elsewhere.example/messages\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	_, err := NewMCPClient(MCPServerConfig{Name: "srv", URL: srv.URL, Transport: "sse"}).Discover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "origin") {
		t.Errorf("err = %v", err)
	}
}

func TestSSEReader(t *testing.T) {
	stream := ": comment\r\n" +
		"id: 1\ndata:\n\n" + // priming: an ID, no data
		"event: message\ndata: a\ndata: b\n\n" +
		"data:no space\nretry: 250\nid: 2\n\n" +
		"data: cut by the end"
	r := newSSEReader(strings.NewReader(stream), mcpMaxMessage)
	ev, err := r.next()
	if err != nil || ev.data != "a\nb" || ev.event != "message" || r.lastID != "1" {
		t.Errorf("first: %+v, %v, last ID %q", ev, err, r.lastID)
	}
	ev, err = r.next()
	if err != nil || ev.data != "no space" || r.lastID != "2" || r.retry != 250*time.Millisecond {
		t.Errorf("second: %+v, %v, last ID %q, retry %v", ev, err, r.lastID, r.retry)
	}
	if _, err := r.next(); err == nil {
		t.Error("an event cut by the end was returned")
	}
}

// Servers answering together register all their tools without writing the
// registry from two goroutines (go test -race reports it otherwise, and a
// concurrent map write is a fatal error, not a panic).
func TestMCPServers_DiscoverRegistersEveryServer(t *testing.T) {
	const servers, perServer = 4, 200
	gate := newBarrier(servers)
	var names []string
	for i := range perServer {
		names = append(names, fmt.Sprintf("t%d", i))
	}
	var configs []MCPServerConfig
	for i := range servers {
		f := newFakeMCP(t, func(f *fakeMCP) { f.gate = gate })
		f.setTools(names...)
		configs = append(configs, f.config(fmt.Sprintf("s%d", i)))
	}

	r := NewRegistry()
	NewMCPServers(r, configs, exposeAll).Discover(context.Background())
	if got := len(r.List()); got != servers*perServer {
		t.Errorf("registered %d tools, want %d", got, servers*perServer)
	}
}

// Tools are registered in the configuration's order, whichever server answers
// first: of two servers giving the same name, the earlier keeps it.
func TestMCPServers_DiscoverRegistersInConfigOrder(t *testing.T) {
	gate := newBarrier(2)
	first := newFakeMCP(t, func(f *fakeMCP) { f.gate = gate })
	first.setTools("b_c")
	second := newFakeMCP(t, func(f *fakeMCP) { f.gate = gate })
	second.setTools("c", "d")
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()

	r := NewRegistry()
	NewMCPServers(r, []MCPServerConfig{
		first.config("a"),
		{Name: "off1", URL: down.URL},
		second.config("a_b"),
		{Name: "off2", URL: down.URL},
	}, exposeAll).Discover(context.Background())

	got, ok := r.Get("a_b_c")
	if !ok || !strings.HasPrefix(got.Description, "[MCP:a]") {
		t.Fatalf("a_b_c = %+v, want the earlier server's", got)
	}
	if names(r.All()) != "[a_b_c a_b_d]" {
		t.Errorf("registry = %s", names(r.All()))
	}
}

// schemaOf is an input schema of about n bytes.
func schemaOf(n int) json.RawMessage {
	return json.RawMessage(`{"type":"object","description":"` + strings.Repeat("x", max(n-40, 0)) + `"}`)
}

func toolInfo(name string) mcpToolInfo {
	return mcpToolInfo{Name: name, Description: "tool " + name, InputSchema: json.RawMessage(`{"type":"object"}`)}
}

// A tool whose name has characters the model's API does not allow (MCP
// allows "search.web") is exposed with _ in their place, and called by its
// own name.
func TestMCPClient_ExposedNameReachesTheRealName(t *testing.T) {
	f := newFakeMCP(t, nil)
	f.setToolInfos(toolInfo("search.web"), toolInfo("files/read"), toolInfo("plain-name_1"))
	tools := discover(t, NewMCPClient(f.config("srv")))
	if got := names(tools); got != "[srv_search_web srv_files_read srv_plain-name_1]" {
		t.Fatalf("tools = %s", got)
	}
	if got, err := call(t, tools, "srv_search_web", `{"q":1}`); err != nil || got != `search.web {"q":1}` {
		t.Errorf("call = %q, %v", got, err)
	}
	if got, err := call(t, tools, "srv_files_read", `{}`); err != nil || got != `files/read {}` {
		t.Errorf("call = %q, %v", got, err)
	}
}

// One tool this worker cannot take refuses the whole list, and so does a
// list too big: too many tools, or too many bytes, on one page or across
// pages, as JSON or as an event stream.
func TestMCPClient_RefusesAToolList(t *testing.T) {
	many := func(n int) []mcpToolInfo {
		var tools []mcpToolInfo
		for i := range n {
			tools = append(tools, toolInfo(fmt.Sprintf("t%d", i)))
		}
		return tools
	}
	heavy := func(n int) []mcpToolInfo { // n tools of 60 KiB each
		var tools []mcpToolInfo
		for i := range n {
			tools = append(tools, mcpToolInfo{Name: fmt.Sprintf("t%d", i), InputSchema: schemaOf(60 << 10)})
		}
		return tools
	}
	withSchema := func(schema string) []mcpToolInfo {
		return []mcpToolInfo{{Name: "t", InputSchema: json.RawMessage(schema)}}
	}
	cases := []struct {
		name      string
		server    string // the server's name in the config; "" = srv
		configure func(*fakeMCP)
		tools     []mcpToolInfo
		want      string
	}{
		{name: "no name", tools: []mcpToolInfo{toolInfo("")}, want: "no name"},
		{name: "too long once exposed", tools: []mcpToolInfo{toolInfo(strings.Repeat("x", 61))}, want: "not 1 to 64"},
		{name: "invalid server name", server: "my.srv", tools: []mcpToolInfo{toolInfo("echo")}, want: "not 1 to 64"},
		{name: "two tools exposed as one", tools: []mcpToolInfo{toolInfo("a.b"), toolInfo("a_b")}, want: "both exposed as \"srv_a_b\""},
		{name: "same name twice", tools: []mcpToolInfo{toolInfo("a"), toolInfo("a")}, want: "both exposed"},
		{name: "description too big", tools: []mcpToolInfo{{Name: "t", Description: strings.Repeat("d", mcpMaxDescription+1), InputSchema: schemaOf(10)}}, want: "description of"},
		{name: "schema too big", tools: []mcpToolInfo{{Name: "t", InputSchema: schemaOf(mcpMaxSchema + 100)}}, want: "input schema of"},
		{name: "schema an array", tools: withSchema(`[]`), want: "not a JSON object"},
		{name: "schema a string", tools: withSchema(`"object"`), want: "not a JSON object"},
		{name: "no schema", tools: withSchema(`null`), want: "not a JSON object"},
		{name: "too many tools", tools: many(mcpMaxTools + 1), want: "more than 500 tools"},
		{name: "too many tools across pages", configure: func(f *fakeMCP) { f.pages = 50 }, tools: many(mcpMaxTools + 1), want: "more than 500 tools"},
		{name: "too many bytes", tools: heavy(80), want: "more than 4194304 bytes"},
		{name: "too many bytes in a stream", configure: func(f *fakeMCP) { f.sse = true }, tools: heavy(80), want: "more than 4194304 bytes"},
		{name: "too many bytes across pages", configure: func(f *fakeMCP) { f.pages = 10 }, tools: heavy(80), want: "more than 4194304 bytes"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeMCP(t, tc.configure)
			f.setToolInfos(tc.tools...)
			server := tc.server
			if server == "" {
				server = "srv"
			}
			tools, err := NewMCPClient(f.config(server)).Discover(context.Background())
			if !errors.Is(err, errToolsRefused) || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %.300v, want a refusal saying %q", err, tc.want)
			}
			if tools != nil {
				t.Errorf("tools = %s", names(tools))
			}
		})
	}

	// At the limits, the list is taken.
	f := newFakeMCP(t, func(f *fakeMCP) { f.pages = 100 })
	limits := many(mcpMaxTools)
	limits[0] = mcpToolInfo{Name: strings.Repeat("x", 60), Description: strings.Repeat("d", mcpMaxDescription), InputSchema: schemaOf(mcpMaxSchema)}
	f.setToolInfos(limits...)
	if n := len(discover(t, NewMCPClient(f.config("srv")))); n != mcpMaxTools {
		t.Errorf("%d tools at the limits, want %d", n, mcpMaxTools)
	}
}

// A redirect is not followed, on either transport: the request and its API
// key stay with the configured server, and the status says why it failed.
func TestMCPClient_DoesNotFollowRedirects(t *testing.T) {
	var reached atomic.Int32
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
	}))
	defer elsewhere.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+"/mcp", http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	for _, transport := range []string{"http", "sse"} {
		_, err := NewMCPClient(MCPServerConfig{Name: "srv", URL: redirect.URL, APIKey: "k", Transport: transport}).Discover(context.Background())
		var httpErr *mcpHTTPError
		if !errors.As(err, &httpErr) || httpErr.Status != http.StatusTemporaryRedirect {
			t.Errorf("%s: err = %v, want HTTP 307", transport, err)
		}
	}
	if n := reached.Load(); n != 0 {
		t.Errorf("the redirect was followed %d times", n)
	}
}

// An error without an ID, as the whole answer to a POST, is the answer to
// its request: the server could not read it.
func TestMCPClient_ErrorWithoutIDAnswersAPost(t *testing.T) {
	f := newFakeMCP(t, func(f *fakeMCP) { f.idless = true })
	tools := discover(t, NewMCPClient(f.config("srv")))
	f.mu.Lock()
	f.callError = &jsonRPCError{Code: -32700, Message: "parse error"}
	f.mu.Unlock()
	var rpcErr *jsonRPCError
	if _, err := call(t, tools, "srv_echo", `{}`); !errors.As(err, &rpcErr) || rpcErr.Code != -32700 {
		t.Errorf("err = %v, want the error without an ID", err)
	}
}

// A legacy server may echo request IDs as strings.
func TestMCPClient_LegacyStringIDs(t *testing.T) {
	f := newFakeMCP(t, func(f *fakeMCP) { f.legacy, f.stringIDs = true, true })
	c := NewMCPClient(f.config("srv"))
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tools, err := c.Discover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := tools[0].Execute(ctx, json.RawMessage(`{}`)); err != nil || got != "echo {}" {
		t.Errorf("call = %q, %v", got, err)
	}
}

// A legacy server's requests are answered a few at a time: beyond
// mcpMaxReplies replies in flight, a request goes unanswered rather than
// costing a goroutine and a POST each. The stream is read on meanwhile.
func TestMCPClient_LegacyBoundsRepliesToTheServer(t *testing.T) {
	gate := make(chan struct{})
	f := newFakeMCP(t, func(f *fakeMCP) { f.legacy, f.pings, f.replyGate = true, 50, gate })
	t.Cleanup(func() { close(gate) }) // before the server closes
	c := NewMCPClient(f.config("srv"))
	defer c.Close()

	// The response comes after the 50 pings on the stream: once the
	// discovery returns, the client has dealt with every one of them, its
	// replies held by the server.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Discover(ctx); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the replies reach the server", func() bool { return f.count("(reply)") >= mcpMaxReplies })
	// No other reply is under way: every request was answered or dropped
	// before the response, and only mcpMaxReplies were let through.
	if n := f.count("(reply)"); n != mcpMaxReplies {
		t.Errorf("%d replies, want %d", n, mcpMaxReplies)
	}
}
