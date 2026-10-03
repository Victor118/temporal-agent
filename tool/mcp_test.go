package tool

import (
	"context"
	"encoding/json"
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

// mcpServer answers tools/list with count tools, described by desc, once
// every server sharing ready has been asked: the answers then arrive
// together, as they would from fast servers at startup.
func mcpServer(t *testing.T, desc string, count int, ready *barrier) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Method != "tools/list" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if !ready.arrive() {
			http.Error(w, "the other servers were never asked", http.StatusGatewayTimeout)
			return
		}
		var list mcpToolListResult
		for i := range count {
			list.Tools = append(list.Tools, mcpToolInfo{Name: fmt.Sprintf("t%d", i), Description: desc})
		}
		result, _ := json.Marshal(list)
		json.NewEncoder(w).Encode(jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: result})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Servers answering together register all their tools without writing the
// registry from two goroutines (go test -race reports it otherwise, and a
// concurrent map write is a fatal error, not a panic).
func TestRegisterMCPServers_RegistersEveryServer(t *testing.T) {
	const servers, perServer = 4, 200
	ready := newBarrier(servers)
	var configs []MCPServerConfig
	for i := range servers {
		srv := mcpServer(t, "", perServer, ready)
		configs = append(configs, MCPServerConfig{Name: fmt.Sprintf("s%d", i), URL: srv.URL})
	}

	r := NewRegistry()
	if errs := RegisterMCPServers(context.Background(), r, configs); len(errs) != 0 {
		t.Fatalf("errors: %v", errs)
	}
	if got := len(r.List()); got != servers*perServer {
		t.Errorf("registered %d tools, want %d", got, servers*perServer)
	}
}

// Tools are registered in the configuration's order, whichever server answers
// first: of two servers giving the same names, the later one wins. Errors
// come in that order too.
func TestRegisterMCPServers_RegistersInConfigOrder(t *testing.T) {
	ready := newBarrier(2)
	first := mcpServer(t, "first", 1, ready)
	second := mcpServer(t, "second", 1, ready)
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()

	r := NewRegistry()
	errs := RegisterMCPServers(context.Background(), r, []MCPServerConfig{
		{Name: "dup", URL: first.URL},
		{Name: "off1", URL: down.URL},
		{Name: "dup", URL: second.URL},
		{Name: "off2", URL: down.URL},
	})

	got, ok := r.Get("dup_t0")
	if !ok {
		t.Fatal("dup_t0 not registered")
	}
	if !strings.HasSuffix(got.Description, "second") {
		t.Errorf("dup_t0 = %q, want the later server's", got.Description)
	}
	if len(errs) != 2 || !strings.Contains(errs[0].Error(), "off1") || !strings.Contains(errs[1].Error(), "off2") {
		t.Errorf("errors = %v, want off1 then off2", errs)
	}
}
