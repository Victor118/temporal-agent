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
// the response, skipping comments and notifications, and answers a ping the
// server sends first.
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
	r := newSSEReader(strings.NewReader(stream))
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
func TestRegisterMCPServers_RegistersEveryServer(t *testing.T) {
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
	if errs := RegisterMCPServers(context.Background(), r, configs); len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if got := len(r.List()); got != servers*perServer {
		t.Errorf("registered %d tools, want %d", got, servers*perServer)
	}
}
