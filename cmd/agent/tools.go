package main

import (
	"context"
	"errors"
	"log"
	"os"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// queuePollerFreshness is how recent a poller must be for its queue to count
// as served. Temporal itself drops pollers unseen for about 5 minutes.
const queuePollerFreshness = 5 * time.Minute

// loadWorkerConfig reads the worker config file. Without a file, it falls back
// to the legacy env config: primary task queue, all tools, MCP_SERVERS filtered
// by TASK_QUEUE_MCP.
func loadWorkerConfig(cfg *config.Config) *config.WorkerConfig {
	wc, err := config.LoadWorkerConfig(cfg.WorkerFile)
	if err == nil {
		log.Printf("Worker config %s: queue %q, tools %v, %d MCP servers", cfg.WorkerFile, wc.Queue, wc.Tools, len(wc.MCP))
		return wc
	}
	if !errors.Is(err, os.ErrNotExist) {
		log.Fatalf("Invalid worker config: %v", err)
	}

	log.Printf("No worker config at %s, using env config (queue %q, all tools)", cfg.WorkerFile, cfg.PrimaryTaskQueue())
	wc = &config.WorkerConfig{Queue: cfg.PrimaryTaskQueue(), Tools: []string{"*"}}
	for _, s := range cfg.MCPServersForQueues(cfg.TaskQueues) {
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

// exposeTools keeps only the tools listed in the worker config, makes sure the
// worker polls its queue, and returns the task queues to poll.
func exposeTools(registry *tool.Registry, wc *config.WorkerConfig, queues []string) []string {
	if removed := registry.Retain(wc.Tools); len(removed) > 0 {
		log.Printf("Tools not exposed by this worker: %v", removed)
	}
	if !slices.Contains(queues, wc.Queue) {
		queues = append(queues, wc.Queue)
	}
	return queues
}

// publishTools writes the registry's tools to the DB catalog under queue.
// A tool already published by another queue that Temporal still sees served
// is skipped with an error log.
func publishTools(st store.Store, tc client.Client, registry *tool.Registry, queue string) {
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
			Name:          t.Name,
			TaskQueue:     queue,
			Description:   t.Description,
			InputSchema:   t.InputSchema,
			Kind:          string(t.Kind),
			WorkflowName:  t.WorkflowName(),
			FireAndForget: t.FireAndForget,
			SchemaHash:    t.SchemaHash(),
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
func queueServed(ctx context.Context, tc client.Client, queue string) bool {
	for _, typ := range []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY, enumspb.TASK_QUEUE_TYPE_WORKFLOW} {
		resp, err := tc.DescribeTaskQueue(ctx, queue, typ)
		if err != nil {
			log.Printf("Warning: failed to describe task queue %q: %v", queue, err)
			return true
		}
		for _, p := range resp.GetPollers() {
			if time.Since(p.GetLastAccessTime().AsTime()) < queuePollerFreshness {
				return true
			}
		}
	}
	return false
}
