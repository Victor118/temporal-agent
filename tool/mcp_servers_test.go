package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func exposeAll(string) bool { return true }

// fakePublisher records what it is asked to publish. fail makes the next
// publish write nothing.
type fakePublisher struct {
	mu    sync.Mutex
	calls []string // "put [..] drop [..]"
	fail  bool
}

func (p *fakePublisher) Publish(_ context.Context, put []*Tool, drop []string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		p.fail = false
		var retry []string
		for _, t := range put {
			retry = append(retry, t.Name)
		}
		return append(retry, drop...)
	}
	p.calls = append(p.calls, fmt.Sprintf("put %s drop %v", names(put), drop))
	return nil
}

func (p *fakePublisher) published() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

// fastServers runs servers with intervals of milliseconds; Run stops with
// the test.
func fastServers(t *testing.T, r *Registry, expose func(string) bool, configs ...MCPServerConfig) *MCPServers {
	t.Helper()
	m := NewMCPServers(r, configs, expose)
	m.Refresh, m.MinRetry, m.MaxRetry = 20*time.Millisecond, 5*time.Millisecond, 20*time.Millisecond
	return m
}

func run(t *testing.T, m *MCPServers, pub MCPPublisher) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx, pub); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

// applied waits until Run has applied a discovery that started after the
// call, attempts counting the discoveries the server saw. A watcher asks
// again only once Run took its last result, and Run takes a result only
// once it applied the one before: the third attempt from now means the
// first one is applied.
func applied(t *testing.T, attempts func() int) {
	t.Helper()
	n := attempts()
	eventually(t, "a discovery is applied", func() bool { return attempts() >= n+3 })
}

// discovered: applied, for a server that answers (one tools/list each).
func discovered(t *testing.T, f *fakeMCP) {
	t.Helper()
	applied(t, func() int { return f.count("tools/list") })
}

// failed: applied, for a server that is down (one request cut each).
func failed(t *testing.T, f *fakeMCP) {
	t.Helper()
	applied(t, func() int { return int(f.refused.Load()) })
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("never: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A server down at startup is asked again in the background; once it
// answers, its tools are registered, filtered by what the worker exposes as
// at startup, and published.
func TestMCPServers_RetriesUntilTheServerComesUp(t *testing.T) {
	f := newFakeMCP(t, nil)
	f.setTools("echo", "secret")
	f.down.Store(true)
	r := NewRegistry()
	m := fastServers(t, r, func(name string) bool { return name != "srv_secret" }, f.config("srv"))
	m.Discover(context.Background())
	if len(r.All()) != 0 {
		t.Fatalf("registered %s from a server that is down", names(r.All()))
	}

	pub := &fakePublisher{}
	run(t, m, pub)
	failed(t, f) // a few failed attempts
	f.down.Store(false)

	eventually(t, "the tools are published", func() bool { return len(pub.published()) > 0 })
	if got := pub.published()[0]; got != "put [srv_echo] drop []" {
		t.Errorf("published %q", got)
	}
	if got := names(r.All()); got != "[srv_echo]" {
		t.Errorf("registry = %s", got)
	}
}

// Discovered again, a server's new tools are registered and published, and
// the ones it no longer gives are removed and withdrawn.
func TestMCPServers_FollowsTheServersTools(t *testing.T) {
	f := newFakeMCP(t, nil)
	f.setTools("a", "b")
	r := NewRegistry()
	m := fastServers(t, r, exposeAll, f.config("srv"))
	m.Discover(context.Background())
	if got := names(r.All()); got != "[srv_a srv_b]" {
		t.Fatalf("at startup: %s", got)
	}

	pub := &fakePublisher{}
	run(t, m, pub)
	discovered(t, f)
	if got := pub.published(); len(got) != 0 {
		t.Errorf("published %v with nothing changed", got)
	}
	f.setTools("b", "c")
	eventually(t, "the change is published", func() bool { return len(pub.published()) > 0 })
	if got := pub.published()[0]; got != "put [srv_c] drop [srv_a]" {
		t.Errorf("published %q", got)
	}
	if got := names(r.All()); got != "[srv_b srv_c]" {
		t.Errorf("registry = %s", got)
	}
}

// A server that goes down keeps its tools: the model's list does not move,
// and a call fails as a tool error until the server is back.
func TestMCPServers_KeepsTheToolsOfAServerThatGoesDown(t *testing.T) {
	f := newFakeMCP(t, nil)
	r := NewRegistry()
	m := fastServers(t, r, exposeAll, f.config("srv"))
	m.Discover(context.Background())
	pub := &fakePublisher{}
	run(t, m, pub)

	f.down.Store(true)
	failed(t, f)
	if got := names(r.All()); got != "[srv_echo]" {
		t.Fatalf("registry = %s", got)
	}
	if got := pub.published(); len(got) != 0 {
		t.Errorf("published %v for a server that is down", got)
	}
	if _, err := r.Execute(context.Background(), "srv_echo", json.RawMessage(`{}`)); err == nil || !strings.Contains(err.Error(), "mcp srv") {
		t.Errorf("call to a server that is down: %v", err)
	}

	f.down.Store(false)
	if got, err := r.Execute(context.Background(), "srv_echo", json.RawMessage(`{}`)); err != nil || got != "echo {}" {
		t.Errorf("call once it is back: %q, %v", got, err)
	}
}

// What a publish could not write is written at the next discovery, though
// nothing changed since.
func TestMCPServers_RetriesAFailedPublish(t *testing.T) {
	f := newFakeMCP(t, nil)
	f.setTools("a")
	r := NewRegistry()
	m := fastServers(t, r, exposeAll, f.config("srv"))
	m.Discover(context.Background())
	pub := &fakePublisher{fail: true}
	f.setTools("a", "b")
	run(t, m, pub)

	eventually(t, "the failed publish is tried again", func() bool { return len(pub.published()) > 0 })
	if got := pub.published()[0]; got != "put [srv_b] drop []" {
		t.Errorf("published %q", got)
	}
}

// A server cannot take a name a built-in tool or another server of the
// worker holds, at startup or later. Once the holder drops the name, the
// holder withdraws it first, and the other server may then have it.
func TestMCPServers_NameCollisionAtRuntime(t *testing.T) {
	a := newFakeMCP(t, nil)
	a.setTools("b_c")
	ab := newFakeMCP(t, nil)
	ab.setTools("x")
	r := NewRegistry()
	r.Register(&Tool{Name: "a_b_builtin", Description: "built in"})
	m := fastServers(t, r, exposeAll, a.config("a"), ab.config("a_b"))
	m.Discover(context.Background())
	pub := &fakePublisher{}
	run(t, m, pub)

	ab.setTools("x", "c", "builtin", "y")
	eventually(t, "a_b's new tool is published", func() bool { return len(pub.published()) > 0 })
	if got := pub.published()[0]; got != "put [a_b_y] drop []" {
		t.Errorf("published %q", got)
	}
	if got, _ := r.Get("a_b_c"); !strings.HasPrefix(got.Description, "[MCP:a]") {
		t.Errorf("a_b_c = %q, want a's", got.Description)
	}
	if got, _ := r.Get("a_b_builtin"); got.Description != "built in" {
		t.Errorf("a_b_builtin = %q, want the built-in tool", got.Description)
	}

	a.setTools()
	eventually(t, "a_b takes a_b_c", func() bool { return len(pub.published()) >= 3 })
	if got := fmt.Sprint(pub.published()[1:3]); got != "[put [] drop [a_b_c] put [a_b_c] drop []]" {
		t.Errorf("published %s", got)
	}
	if got, _ := r.Get("a_b_c"); !strings.HasPrefix(got.Description, "[MCP:a_b]") {
		t.Errorf("a_b_c = %q, want a_b's", got.Description)
	}
}

// Run ends when its context does, discoveries in flight included.
func TestMCPServers_RunStopsOnCancel(t *testing.T) {
	asked := make(chan struct{}, 1)
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body) // the server sees the client leave only once the body is read
		select {
		case asked <- struct{}{}:
		default:
		}
		<-r.Context().Done()
	}))
	defer hang.Close()
	m := fastServers(t, NewRegistry(), exposeAll, MCPServerConfig{Name: "hang", URL: hang.URL})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx, &fakePublisher{}); close(done) }()
	select {
	case <-asked:
	case <-time.After(5 * time.Second):
		t.Fatal("the server was never asked")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

// A list the client refuses changes nothing: the server keeps the tools it
// had, nothing is published, and a list it takes later is applied.
func TestMCPServers_RefusedListKeepsTheTools(t *testing.T) {
	f := newFakeMCP(t, nil)
	f.setTools("a")
	r := NewRegistry()
	m := fastServers(t, r, exposeAll, f.config("srv"))
	m.Discover(context.Background())
	pub := &fakePublisher{}
	run(t, m, pub)

	f.setToolInfos(toolInfo("a"), toolInfo("b.c"), toolInfo("b_c"))
	discovered(t, f)
	if got := names(r.All()); got != "[srv_a]" {
		t.Errorf("registry = %s after a refused list", got)
	}
	if got := pub.published(); len(got) != 0 {
		t.Errorf("published %v from a refused list", got)
	}

	f.setTools("a", "b")
	eventually(t, "the list taken later is published", func() bool { return len(pub.published()) > 0 })
	if got := pub.published()[0]; got != "put [srv_b] drop []" {
		t.Errorf("published %q", got)
	}
}

// A refusal is logged when it starts or its reason changes, not at every
// discovery; so are the other changes of state.
func TestMCPServers_LogsEachChangeOfStateOnce(t *testing.T) {
	var buf strings.Builder
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	m := NewMCPServers(NewRegistry(), []MCPServerConfig{{Name: "srv", URL: "http://unused"}}, exposeAll)
	refused := func(why string) error {
		return fmt.Errorf("mcp srv: discover tools: %w: %s", errToolsRefused, why)
	}
	down := errors.New("mcp srv: connection refused")
	for _, err := range []error{
		refused("x"), refused("x"), refused("x"), // one line
		refused("y"), // the reason changed
		down, down,   // one line
		nil, nil, // up: one line
		refused("y"),                   // refused again
		down,                           // down again
		errors.New("mcp srv: timeout"), // still down
	} {
		m.apply(context.Background(), mcpDiscovery{0, []*Tool{}, err}, nil)
	}

	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if strings.Contains(l, "MCP server srv") {
			lines = append(lines, l)
		}
	}
	want := []string{"refused: x", "refused: y", "unreachable", "reachable: 0 tools", "refused: y", "unreachable"}
	if len(lines) != len(want) {
		t.Fatalf("logged %d lines, want %d:\n%s", len(lines), len(want), strings.Join(lines, "\n"))
	}
	for i, w := range want {
		if !strings.Contains(lines[i], w) {
			t.Errorf("line %d = %q, want %q", i, lines[i], w)
		}
	}
}
