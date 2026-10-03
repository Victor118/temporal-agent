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
		if line != "event: message" || !strings.HasPrefix(next(), `data: "hi"`) {
			t.Fatalf("event line %q", line)
		}
		return
	}
}
