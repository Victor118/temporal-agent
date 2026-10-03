package main

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/session"
	"github.com/victor/temporal-agent/sse"
)

// A server bounds the headers' time and an idle connection, but no whole
// request or response: the SSE streams never end.
func TestNewHTTPServer_Timeouts(t *testing.T) {
	srv := newHTTPServer(":0", http.NotFoundHandler())
	if srv.ReadHeaderTimeout <= 0 || srv.IdleTimeout <= 0 {
		t.Errorf("ReadHeaderTimeout %v, IdleTimeout %v: want both set", srv.ReadHeaderTimeout, srv.IdleTimeout)
	}
	if srv.WriteTimeout != 0 || srv.ReadTimeout != 0 {
		t.Errorf("WriteTimeout %v, ReadTimeout %v: would cut the SSE streams", srv.WriteTimeout, srv.ReadTimeout)
	}
}

// An idle stream sends a comment now and then, which keeps a proxy from
// closing it; events still go through.
func TestRelaySSE_PingsWhileIdle(t *testing.T) {
	hub := sse.NewHub()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		relaySSE(w, r, hub, 10*time.Millisecond, nil, "s1")
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	lines := bufio.NewScanner(resp.Body)

	next := func() string {
		t.Helper()
		if !lines.Scan() {
			t.Fatalf("stream ended: %v", lines.Err())
		}
		return lines.Text()
	}
	if got := next(); got != ": ping" {
		t.Fatalf("first line %q, want a ping", got)
	}
	if got := next(); got != "" {
		t.Fatalf("line %q after the ping, want its blank line", got)
	}

	hub.Publish("s1", activity.SSEEvent{Type: "message", Data: []byte(`"hi"`)})
	for {
		line := next()
		if line == ": ping" || line == "" {
			continue
		}
		if !strings.HasPrefix(line, "id: ") || next() != "event: message" || !strings.HasPrefix(next(), `data: "hi"`) {
			t.Fatalf("event line %q", line)
		}
		return
	}
}

// A stream whose check fails ends at the next keep-alive; one that passes
// goes on. A member_left event makes it check at once, and is not sent to
// the one it ends.
func TestRelaySSE_EndsWhenNoLongerAlive(t *testing.T) {
	hub := sse.NewHub()
	var member atomic.Bool
	member.Store(true)
	serve := func(keepAlive time.Duration) string {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			relaySSE(w, r, hub, keepAlive, member.Load, "s1")
		}))
		t.Cleanup(srv.Close)
		return srv.URL
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lines := openStream(t, ctx, serve(10*time.Millisecond), "")
	for range 3 { // pings, the stream alive
		if !lines.Scan() {
			t.Fatalf("stream ended while alive: %v", lines.Err())
		}
	}
	member.Store(false)
	for lines.Scan() {
		if line := lines.Text(); line != ": ping" && line != "" {
			t.Errorf("line %q after the check failed", line)
		}
	}
	if err := lines.Err(); err != nil {
		t.Fatalf("stream did not end: %v", err)
	}

	// With a keep-alive too far to wait for: the event ends it.
	member.Store(true)
	lines = openStream(t, ctx, serve(time.Hour), "")
	hub.Publish("s1", activity.SSEEvent{Type: session.EventMemberLeft, Data: []byte(`{"user_ids":["u-carol"]}`)})
	if got := nextEvent(t, lines); !strings.HasSuffix(got, " "+session.EventMemberLeft) {
		t.Fatalf("a member still in: %q", got)
	}
	member.Store(false)
	hub.Publish("s1", activity.SSEEvent{Type: session.EventMemberLeft, Data: []byte(`{"user_ids":["u-bob"]}`)})
	for lines.Scan() {
		if line := lines.Text(); line != "" {
			t.Errorf("line %q sent to a member out", line)
		}
	}
	if err := lines.Err(); err != nil {
		t.Fatalf("stream did not end: %v", err)
	}
}

// openStream connects to a stream at url, with a Last-Event-ID header when
// header is not empty, and returns its lines.
func openStream(t *testing.T, ctx context.Context, url, header string) *bufio.Scanner {
	t.Helper()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if header != "" {
		req.Header.Set("Last-Event-ID", header)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return bufio.NewScanner(resp.Body)
}

// nextEvent reads the next event of a stream, pings skipped, as "id type".
func nextEvent(t *testing.T, lines *bufio.Scanner) string {
	t.Helper()
	var id, typ string
	for lines.Scan() {
		switch line := lines.Text(); {
		case strings.HasPrefix(line, "id: "):
			id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			typ = strings.TrimPrefix(line, "event: ")
		case line == "" && typ != "":
			return id + " " + typ
		}
	}
	t.Fatalf("stream ended: %v", lines.Err())
	return ""
}

// A client that reconnects is sent what it missed, from the ID EventSource
// sends or the one in the URL (the htmx extension's reconnection sends no
// header); from an ID the hub cannot place, a reload.
func TestRelaySSE_ReplaysWhatAReconnectionMissed(t *testing.T) {
	hub := sse.NewHub()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		relaySSE(w, r, hub, time.Hour, nil, "s1")
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	from := hub.Position("s1") // the page was rendered here
	hub.Publish("s1", activity.SSEEvent{Type: "message", Data: []byte(`{}`)})
	hub.Publish("s1", activity.SSEEvent{Type: "turn_done", Data: []byte(`{}`)})

	lines := openStream(t, ctx, srv.URL+"?last_event_id="+from, "")
	first, second := nextEvent(t, lines), nextEvent(t, lines)
	if !strings.HasSuffix(first, " message") || !strings.HasSuffix(second, " turn_done") {
		t.Fatalf("replayed %q, %q", first, second)
	}

	// The header is the latest: only what came after it.
	lastID := strings.Fields(first)[0]
	lines = openStream(t, ctx, srv.URL+"?last_event_id="+from, lastID)
	if got := nextEvent(t, lines); got != second {
		t.Errorf("from the header: %q, want %q", got, second)
	}

	// Another epoch (a restart): reload, at the hub's position.
	lines = openStream(t, ctx, srv.URL, "0-1")
	if got := nextEvent(t, lines); got != hub.Position("s1")+" reload" {
		t.Errorf("a stale ID: %q", got)
	}
}
