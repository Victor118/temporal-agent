package main

import (
	"context"
	"errors"
	"log"
	"os"
	"slices"
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
		return wc
	}
	if !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("Invalid worker config: %v", err)
	}

	log.Printf("No worker config at %s: serving workflows and all tools on queue %q", cfg.WorkerFile, cfg.WorkflowQueue)
	wc = &config.WorkerConfig{Queue: cfg.WorkflowQueue, Workflows: true, Tools: []string{"*"}}
	for _, s := range cfg.MCPServers {
		wc.MCP = append(wc.MCP, config.WorkerMCPServer{
			Name:      s.Name,
			URL:       s.URL,
			APIKey:    s.APIKey,
			Transport: s.Transport,
		})
	}
	return wc
}

// registerMCPServers discovers and registers the tools of the worker's MCP servers.
func registerMCPServers(registry *tool.Registry, servers []config.WorkerMCPServer) {
	if len(servers) == 0 {
		return
	}
	mcpConfigs := make([]tool.MCPServerConfig, len(servers))
	for i, s := range servers {
		mcpConfigs[i] = tool.MCPServerConfig{
			Name:      s.Name,
			URL:       s.URL,
			APIKey:    s.APIKey,
			Transport: s.Transport,
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, err := range tool.RegisterMCPServers(ctx, registry, mcpConfigs) {
		log.Printf("Warning: MCP server error: %v", err)
	}
}

// exposeTools keeps only the tools listed in the worker config.
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

// publishTools writes the registry's tools to the DB catalog under queue.
// A tool already published by another queue that Temporal still sees served
// is skipped with an error log.
func publishTools(st toolPublisher, tc taskqueue.Describer, registry *tool.Registry, queue string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	for _, t := range registry.All() {
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
				log.Printf("Error: tool %q is already served on queue %q; not publishing it on %q", t.Name, prev.TaskQueue, queue)
				continue
			}
			log.Printf("Tool %q moves from unserved queue %q to %q", t.Name, prev.TaskQueue, queue)
		}
		if had && prev.TaskQueue == queue && prev.SchemaHash != rec.SchemaHash {
			log.Printf("Warning: tool %q changed schema on queue %q (workers of this queue may disagree)", t.Name, queue)
		}

		if err := st.UpsertTool(ctx, rec); err != nil {
			log.Printf("Error: failed to publish tool %q: %v", t.Name, err)
			continue
		}
		published++
	}
	log.Printf("Published %d tools on queue %q", published, queue)
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
