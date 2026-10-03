package tool

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// MCPPublisher writes what became of a server's tools to the tools catalog.
type MCPPublisher interface {
	// Publish writes put, and withdraws drop from the rows this worker's
	// queue holds. It returns the names it could not write, to try again.
	Publish(ctx context.Context, put []*Tool, drop []string) (retry []string)
}

// MCPServers keeps the registry in step with the worker's MCP servers: their
// tools are registered once discovered, kept while a server is down, and
// follow what a server gives when it is discovered again.
type MCPServers struct {
	registry *Registry
	expose   func(name string) bool
	servers  []*mcpServer

	// Refresh is the wait between two discoveries of a server that answers.
	// One that does not is asked again after MinRetry, then twice as long
	// each time, up to MaxRetry. Set before Discover.
	Refresh, MinRetry, MaxRetry time.Duration
}

// mcpServer is a server and what is known of it. Written by one goroutine
// at a time: Discover at startup, then Run's.
type mcpServer struct {
	client  *MCPClient
	tried   bool     // a discovery came back
	up      bool     // the last one succeeded
	hidden  string   // tools not exposed, as last logged
	refused string   // tools whose names are taken, as last logged
	retry   []string // names a publish could not write
}

type mcpDiscovery struct {
	index int
	tools []*Tool
	err   error
}

// NewMCPServers prepares the servers; nothing is asked before Discover.
// expose is the worker's choice of tools: the others are not registered.
func NewMCPServers(registry *Registry, configs []MCPServerConfig, expose func(name string) bool) *MCPServers {
	m := &MCPServers{
		registry: registry,
		expose:   expose,
		Refresh:  30 * time.Second,
		MinRetry: 5 * time.Second,
		MaxRetry: 5 * time.Minute,
	}
	for _, c := range configs {
		m.servers = append(m.servers, &mcpServer{client: NewMCPClient(c)})
	}
	return m
}

// Discover asks every server for its tools, in parallel, then registers them
// in config order whichever answers first: of two servers giving one name,
// the earlier keeps it. It publishes nothing: at startup the worker
// publishes its whole registry once this returns.
func (m *MCPServers) Discover(ctx context.Context) {
	found := make([]mcpDiscovery, len(m.servers))
	var wg sync.WaitGroup
	for i, s := range m.servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tools, err := s.client.Discover(ctx)
			found[i] = mcpDiscovery{i, tools, err}
		}()
	}
	wg.Wait()
	for _, d := range found {
		m.apply(ctx, d, nil)
	}
}

// Run discovers each server again until ctx ends — a server down at its
// last discovery after a growing wait, the others every Refresh — and
// publishes what changes. Discoveries run in parallel; their results are
// applied here, in this goroutine alone, which is the only one to write the
// servers' tools and to publish them after startup.
func (m *MCPServers) Run(ctx context.Context, pub MCPPublisher) {
	if len(m.servers) == 0 {
		return
	}
	results := make(chan mcpDiscovery)
	var wg sync.WaitGroup
	for i, s := range m.servers {
		wg.Add(1)
		go func(up bool) {
			defer wg.Done()
			m.watch(ctx, i, up, results)
		}(s.up)
	}
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case d := <-results:
			m.apply(ctx, d, pub)
		}
	}
}

// watch discovers server i over and over, waiting as Run says.
func (m *MCPServers) watch(ctx context.Context, i int, up bool, out chan<- mcpDiscovery) {
	client := m.servers[i].client
	retry := m.MinRetry
	wait := m.Refresh
	if !up {
		wait, retry = retry, min(2*retry, m.MaxRetry)
	}
	for {
		if sleepCtx(ctx, wait) != nil {
			return
		}
		tools, err := client.Discover(ctx)
		if ctx.Err() != nil {
			return
		}
		select {
		case out <- mcpDiscovery{i, tools, err}:
		case <-ctx.Done():
			return
		}
		if err == nil {
			wait, retry = m.Refresh, m.MinRetry
		} else {
			wait, retry = retry, min(2*retry, m.MaxRetry)
		}
	}
}

// apply registers what a discovery found and publishes the difference (pub
// nil: nothing published). A failed discovery changes nothing: the server's
// tools stay registered, their calls fail as tool errors, and the model's
// tool list does not move. Only the server's state changes are logged.
func (m *MCPServers) apply(ctx context.Context, d mcpDiscovery, pub MCPPublisher) {
	s := m.servers[d.index]
	name := s.client.Name()
	if d.err != nil {
		switch {
		case !s.tried:
			log.Printf("Warning: MCP server %s unreachable, retrying in the background: %v", name, d.err)
		case s.up:
			log.Printf("Warning: MCP server %s unreachable, retrying in the background; its tools stay registered, their calls fail meanwhile: %v", name, d.err)
		}
		s.tried, s.up = true, false
		return
	}

	var exposed []*Tool
	var hidden []string
	for _, t := range d.tools {
		if m.expose(t.Name) {
			exposed = append(exposed, t)
		} else {
			hidden = append(hidden, t.Name)
		}
	}
	if h := fmt.Sprint(hidden); h != s.hidden {
		if len(hidden) > 0 {
			log.Printf("MCP server %s: tools not exposed by this worker: %v", name, hidden)
		}
		s.hidden = h
	}

	change := m.registry.SyncSource(name, exposed)
	if r := fmt.Sprint(change.Refused); r != s.refused {
		if len(change.Refused) > 0 {
			log.Printf("Error: MCP server %s: tools not registered, their names are taken: %v", name, change.Refused)
		}
		s.refused = r
	}
	switch {
	case !s.up:
		log.Printf("MCP server %s reachable: %d tools registered", name, len(exposed)-len(change.Refused))
	case len(change.Changed) > 0 || len(change.Removed) > 0:
		var changed []string
		for _, t := range change.Changed {
			changed = append(changed, t.Name)
		}
		log.Printf("MCP server %s: tools added or changed %v, removed %v", name, changed, change.Removed)
	}
	s.tried, s.up = true, true

	if pub == nil {
		return
	}
	put, drop := m.toPublish(name, change, s.retry)
	if len(put) == 0 && len(drop) == 0 {
		s.retry = nil
		return
	}
	s.retry = pub.Publish(ctx, put, drop)
}

// toPublish is what to write after a sync: the changes, and the names an
// earlier publish could not write, as the registry now has them. A name the
// registry holds is never withdrawn: another source of this worker took it.
func (m *MCPServers) toPublish(source string, change SourceChange, retry []string) (put []*Tool, drop []string) {
	listed := make(map[string]bool)
	for _, t := range change.Changed {
		put = append(put, t)
		listed[t.Name] = true
	}
	for _, n := range change.Removed {
		if !listed[n] && !m.registry.has(n) {
			drop = append(drop, n)
			listed[n] = true
		}
	}
	for _, n := range retry {
		if listed[n] {
			continue
		}
		listed[n] = true
		if t, ok := m.registry.heldBy(n, source); ok {
			put = append(put, t)
		} else if !m.registry.has(n) {
			drop = append(drop, n)
		}
	}
	return put, drop
}
