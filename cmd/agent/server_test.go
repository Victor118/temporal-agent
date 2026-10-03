package main

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/activity"
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
		relaySSE(w, r, hub, "s1", 10*time.Millisecond)
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
		relaySSE(w, r, hub, "s1", time.Hour)
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
