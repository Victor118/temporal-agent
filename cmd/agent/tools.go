package main

import (
	"context"
	"errors"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/taskqueue"
	"github.com/victor/temporal-agent/tool"
)

// loadWorkerConfig reads the worker config file. Without a file, the worker
// serves everything on the workflow queue: workflows, all tools, and every
// MCP server from MCP_SERVERS.
func loadWorkerConfig(cfg *config.Config) *config.WorkerConfig {
	wc, err := config.LoadWorkerConfig(cfg.WorkerFile)
	if err == nil {
		log.Printf("Worker config %s: queue %q, workflows %v, tools %v, %d MCP servers",
			cfg.WorkerFile, wc.Queue, wc.Workflows, wc.Tools, len(wc.MCP))
		if len(wc.Tools) == 0 {
			log.Printf("Worker config %s: tools: [], this worker publishes no tool (it serves the workflows of its queue)", cfg.WorkerFile)
		}
		return wc
	}
	if !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("Invalid worker config: %v", err)
	}

	log.Printf("No worker config at %s: serving workflows and all tools on queue %q", cfg.WorkerFile, cfg.WorkflowQueue)
	wc = &config.WorkerConfig{Queue: cfg.WorkflowQueue, Workflows: true, Tools: []string{"*"}}
	seen := make(map[string]bool)
	for _, s := range cfg.MCPServers {
		// The tools of a server are registered under its name: without one,
		// or with another's, it would claim names it must not.
		if s.Name == "" || s.URL == "" || seen[s.Name] {
			log.Printf("Warning: MCP_SERVERS entry %q (%s) skipped: it needs a name of its own and a url", s.Name, s.URL)
			continue
		}
		seen[s.Name] = true
		wc.MCP = append(wc.MCP, config.WorkerMCPServer{
			Name:      s.Name,
			URL:       s.URL,
			APIKey:    s.APIKey,
			Transport: s.Transport,
		})
	}
	return wc
}

// mcpStartupWait bounds the discovery of the MCP servers at startup. A
// server slower than that is not waited for: the background discovery
// registers and publishes its tools when it answers.
const mcpStartupWait = 10 * time.Second

// discoverMCPServers registers the tools of the worker's MCP servers that
// answer now, among those the worker exposes. The others are retried in the
// background by Run.
func discoverMCPServers(registry *tool.Registry, wc *config.WorkerConfig) *tool.MCPServers {
	configs := make([]tool.MCPServerConfig, len(wc.MCP))
	for i, s := range wc.MCP {
		configs[i] = tool.MCPServerConfig{
			Name:      s.Name,
			URL:       s.URL,
			APIKey:    s.APIKey,
			Transport: s.Transport,
		}
	}
	servers := tool.NewMCPServers(registry, configs, func(name string) bool { return tool.MatchAny(wc.Tools, name) })
	servers.Refresh = catalogRefresh
	if len(configs) > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), mcpStartupWait)
		defer cancel()
		servers.Discover(ctx)
	}
	return servers
}

// exposeTools keeps only the tools listed in the worker config. Run before
// the MCP servers' tools are registered, which are filtered as they come.
func exposeTools(registry *tool.Registry, wc *config.WorkerConfig) {
	if removed := registry.Retain(wc.Tools); len(removed) > 0 {
		log.Printf("Tools not exposed by this worker: %v", removed)
	}
}

// workerQueues returns the task queues this worker polls: the workflow queue
// if it serves workflows, and its tool queue.
func workerQueues(cfg *config.Config, wc *config.WorkerConfig) []string {
	var queues []string
	if wc.Workflows {
		queues = append(queues, cfg.WorkflowQueue)
	}
	if !slices.Contains(queues, wc.Queue) {
		queues = append(queues, wc.Queue)
	}
	return queues
}

// toolPublisher is the tools table, as a worker publishing to it sees it.
type toolPublisher interface {
	ListTools(ctx context.Context) ([]store.ToolRecord, error)
	UpsertTool(ctx context.Context, tool store.ToolRecord) error
}

// publishTools writes tools to the DB catalog under queue, and returns the
// names it failed to write. A tool already published by another queue that
// Temporal still sees served is skipped with an error log.
func publishTools(ctx context.Context, st toolPublisher, tc taskqueue.Describer, tools []*tool.Tool, queue string) (failed []string) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	existing := make(map[string]store.ToolRecord)
	if records, err := st.ListTools(ctx); err != nil {
		log.Printf("Warning: failed to list published tools: %v", err)
	} else {
		for _, r := range records {
			existing[r.Name] = r
		}
	}

	served := make(map[string]bool) // other queues already checked
	published := 0
	for _, t := range tools {
		rec := store.ToolRecord{
			Name:             t.Name,
			TaskQueue:        queue,
			Description:      t.Description,
			InputSchema:      t.InputSchema,
			Kind:             string(t.Kind),
			WorkflowName:     t.WorkflowName(),
			FireAndForget:    t.FireAndForget,
			Sensitive:        t.Sensitive,
			PrivateInput:     t.PrivateInput,
			NeedsCallContext: t.NeedsCallContext,
			Timeout:          t.Timeout,
			SchemaHash:       t.SchemaHash(),
		}
		if len(rec.InputSchema) == 0 {
			rec.InputSchema = []byte(`{"type":"object","properties":{}}`)
		}

		prev, had := existing[t.Name]
		if had && prev.TaskQueue != queue {
			isServed, checked := served[prev.TaskQueue]
			if !checked {
				isServed = queueServed(ctx, tc, prev.TaskQueue)
				served[prev.TaskQueue] = isServed
			}
			if isServed {
				log.Printf("Error: tool %q is already served on queue %q; not publishing it on %q (tried again later)", t.Name, prev.TaskQueue, queue)
				failed = append(failed, t.Name)
				continue
			}
			log.Printf("Tool %q moves from unserved queue %q to %q", t.Name, prev.TaskQueue, queue)
		}
		if had && prev.TaskQueue == queue && prev.SchemaHash != rec.SchemaHash {
			log.Printf("Warning: tool %q changed schema on queue %q (workers of this queue may disagree)", t.Name, queue)
		}

		if err := st.UpsertTool(ctx, rec); err != nil {
			log.Printf("Error: failed to publish tool %q: %v", t.Name, err)
			failed = append(failed, t.Name)
			continue
		}
		published++
	}
	log.Printf("Published %d tools on queue %q", published, queue)
	return failed
}

// toolCatalog is the tools table, as the background MCP discovery sees it.
type toolCatalog interface {
	toolPublisher
	DeleteTool(ctx context.Context, name, taskQueue string) (bool, error)
}

// catalogPublisher publishes what changes in the worker's MCP servers after
// startup. Other workers see it at their next catalog refresh.
type catalogPublisher struct {
	st    toolCatalog
	tc    taskqueue.Describer
	queue string
}

// Publish writes the new and changed tools, and withdraws the ones their
// server no longer gives — on this queue only: the row of a tool another
// queue serves is not this worker's to remove. Left in the table, a removed
// tool would stay in every agent's list and fail each call as unknown.
func (p catalogPublisher) Publish(ctx context.Context, put []*tool.Tool, drop []string) (retry []string) {
	if len(put) > 0 {
		retry = publishTools(ctx, p.st, p.tc, put, p.queue)
	}
	for _, name := range drop {
		deleted, err := p.st.DeleteTool(ctx, name, p.queue)
		switch {
		case err != nil:
			log.Printf("Error: failed to withdraw tool %q from queue %q: %v", name, p.queue, err)
			retry = append(retry, name)
		case deleted:
			log.Printf("Withdrew tool %q from queue %q: its MCP server no longer gives it", name, p.queue)
		}
	}
	return retry
}

// queueServed reports whether Temporal has seen a recent poller on the queue.
// If Temporal can't be queried, the queue is assumed served (don't take over).
func queueServed(ctx context.Context, tc taskqueue.Describer, queue string) bool {
	status := taskqueue.Describe(ctx, tc, queue)
	if status.Err != nil {
		log.Printf("Warning: failed to describe task queue %q: %v", queue, status.Err)
		return true
	}
	return status.Served()
}

// withdrawUnoffered withdraws from queue the published rows of the tools this
// worker no longer offers (registry): a tool moved to another worker (as
// analyze_repo left the coding containers for the main worker), or dropped
// from worker.yaml. Left in the table, it would stay in the agents' lists,
// and keep its name from being published elsewhere. The tools of the
// worker's MCP servers (mcpPrefixes, "<server>_") are left to their
// discovery, which publishes and withdraws them, a server that is down
// included.
func withdrawUnoffered(ctx context.Context, st toolCatalog, registry *tool.Registry, queue string, mcpPrefixes []string) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	records, err := st.ListTools(ctx)
	if err != nil {
		log.Printf("Warning: failed to list published tools: %v", err)
		return
	}
	for _, r := range records {
		if r.TaskQueue != queue {
			continue
		}
		if _, offered := registry.Get(r.Name); offered {
			continue
		}
		if slices.ContainsFunc(mcpPrefixes, func(p string) bool { return strings.HasPrefix(r.Name, p) }) {
			continue
		}
		if deleted, err := st.DeleteTool(ctx, r.Name, queue); err != nil {
			log.Printf("Error: failed to withdraw tool %q from queue %q: %v", r.Name, queue, err)
		} else if deleted {
			log.Printf("Withdrew tool %q from queue %q: this worker no longer offers it", r.Name, queue)
		}
	}
}

// keepPublishing publishes again, every interval, the tools a publish left
// out (another queue still serving one, the database away), until every one
// is in: a tool moving between workers lands whatever the order they start
// in.
func keepPublishing(ctx context.Context, st toolPublisher, tc taskqueue.Describer, registry *tool.Registry, queue string, left []string, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for len(left) > 0 {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		var tools []*tool.Tool
		for _, name := range left {
			if tl, ok := registry.Get(name); ok {
				tools = append(tools, tl)
			}
		}
		left = publishTools(ctx, st, tc, tools, queue)
	}
}
