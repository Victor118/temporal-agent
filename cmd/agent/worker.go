package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
	"github.com/victor/temporal-agent/workflow"
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Start the Temporal worker (no HTTP server)",
	Run:   runWorker,
}

func runWorker(cmd *cobra.Command, args []string) {
	cfg := config.Load()

	// Store
	st, err := store.NewPostgresStore(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to init store: %v", err)
	}
	defer st.Close()

	// LLM provider
	var llmProvider provider.LLMProvider
	switch cfg.LLMProvider {
	case "anthropic":
		llmProvider = provider.NewAnthropicProvider(cfg.LLMAPIKey)
	default:
		log.Fatalf("Unknown LLM provider: %s", cfg.LLMProvider)
	}

	// Tool registry
	registry := tool.NewRegistry()
	tool.RegisterFilesystemTools(registry, cfg.WorkspacePath)
	tool.RegisterGrepTool(registry, cfg.WorkspacePath)
	tool.RegisterGlobTool(registry, cfg.WorkspacePath)
	tool.RegisterExecTool(registry, cfg.WorkspacePath)
	tool.RegisterWebTools(registry)
	tool.RegisterWebSearchTool(registry, cfg.BraveSearchAPIKey)
	tool.RegisterSpawnTool(registry, workflow.AgentWorkflow)
	tool.RegisterAskUserTool(registry, workflow.AskUserWorkflow)

	// MCP servers
	if len(cfg.MCPServers) > 0 {
		mcpConfigs := make([]tool.MCPServerConfig, len(cfg.MCPServers))
		for i, s := range cfg.MCPServers {
			mcpConfigs[i] = tool.MCPServerConfig{
				Name:      s.Name,
				URL:       s.URL,
				APIKey:    s.APIKey,
				Transport: s.Transport,
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		errs := tool.RegisterMCPServers(ctx, registry, mcpConfigs)
		cancel()
		for _, err := range errs {
			log.Printf("Warning: MCP server error: %v", err)
		}
	}

	// Skills — load from git repo if configured, build per-queue prompts
	var skillStore skill.Store
	var prompts map[string]string
	if cfg.SkillsRepo != "" {
		skillStore = &skill.GitStore{
			RepoURL:  cfg.SkillsRepo,
			Branch:   cfg.SkillsBranch,
			CacheDir: filepath.Join(os.TempDir(), "temporal-agent-skills-worker"),
		}
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		skills, err := skillStore.LoadAll(ctx)
		cancel()
		if err != nil {
			log.Printf("Warning: failed to load skills from repo: %v", err)
		} else {
			log.Printf("Loaded %d skills from %s", len(skills), cfg.SkillsRepo)
			prompts = activity.BuildSkillPrompts(skills, cfg.TaskQueueSkills)
			for queue, skillNames := range cfg.TaskQueueSkills {
				log.Printf("Task queue %q: skills %v", queue, skillNames)
			}
		}
	} else {
		log.Println("No skills repo configured (SKILLS_REPO), running without skills")
	}

	skillAct := activity.NewSkillActivities(prompts, nil)

	// Register this worker's queues with the server and fetch the full catalog
	registerAndFetchCatalog(cfg, skillAct)

	// Temporal client
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHost,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		log.Fatalf("Failed to connect to Temporal: %v", err)
	}
	defer temporalClient.Close()

	// Register query_workflow tool (needs temporal client)
	tool.RegisterQueryWorkflowTool(registry, temporalClient)

	// Register schedule tools (needs temporal client + store)
	tool.RegisterScheduleTools(registry, temporalClient, st, workflow.ScheduledAgentWorkflow, cfg.PrimaryTaskQueue())

	// Notification bridge: POST to server's internal endpoint
	notifier := activity.NewHTTPNotifier(cfg.NotifyURL)

	// Create one worker per task queue
	workers := make([]worker.Worker, 0, len(cfg.TaskQueues))
	for _, queue := range cfg.TaskQueues {
		w := worker.New(temporalClient, queue, worker.Options{})

		w.RegisterWorkflow(workflow.SessionWorkflow)
		w.RegisterWorkflow(workflow.AgentWorkflow)
		w.RegisterWorkflow(workflow.AskUserWorkflow)
		w.RegisterWorkflow(workflow.ScheduledAgentWorkflow)

		w.RegisterActivity(&activity.LLMActivities{Provider: llmProvider})
		w.RegisterActivity(&activity.MemoryActivities{Store: st})
		w.RegisterActivity(&activity.ToolActivities{Registry: registry})
		w.RegisterActivity(&activity.NotificationActivities{Hub: notifier})
		w.RegisterActivity(&activity.DeliveryActivities{Hub: notifier})
		w.RegisterActivity(skillAct)

		workers = append(workers, w)
		log.Printf("Worker registered on task queue %q", queue)
	}

	// Poll server for skills version changes + refresh catalog
	ctx, stopPoll := context.WithCancel(context.Background())
	if cfg.SkillsRepo != "" && skillStore != nil {
		go watchSkillsVersion(ctx, cfg.NotifyURL, 30*time.Second, func() {
			reloadCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			skills, err := skillStore.LoadAll(reloadCtx)
			if err != nil {
				log.Printf("Error reloading skills: %v", err)
				return
			}
			activity.SetPrompts(skillAct, activity.BuildSkillPrompts(skills, cfg.TaskQueueSkills))
			refreshCatalog(cfg.NotifyURL, skillAct)
			log.Printf("Worker reloaded %d skills, rebuilt prompts", len(skills))
		})
	}

	// Start all workers
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down workers...")
		stopPoll()
		for _, w := range workers {
			w.Stop()
		}
	}()

	// Run all workers: first N-1 in goroutines, last one blocks
	for i, w := range workers {
		if i < len(workers)-1 {
			go func(w worker.Worker) {
				if err := w.Run(worker.InterruptCh()); err != nil {
					log.Printf("Worker failed: %v", err)
				}
			}(w)
		} else {
			if err := w.Run(worker.InterruptCh()); err != nil {
				log.Fatalf("Worker failed: %v", err)
			}
		}
	}
}

// registerAndFetchCatalog registers this worker's queues+skills with the server,
// then fetches the full agents catalog so all agents know about each other.
func registerAndFetchCatalog(cfg *config.Config, skillAct *activity.SkillActivities) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Register each queue with its skills
	for queue, skillNames := range cfg.TaskQueueSkills {
		body, _ := json.Marshal(map[string]interface{}{
			"task_queue": queue,
			"skills":     skillNames,
		})
		req, err := http.NewRequestWithContext(ctx, "POST", cfg.NotifyURL+"/internal/workers/register", bytes.NewReader(body))
		if err != nil {
			log.Printf("Warning: failed to build register request for queue %q: %v", queue, err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("Warning: failed to register queue %q with server: %v", queue, err)
			continue
		}
		resp.Body.Close()
		log.Printf("Registered queue %q with server", queue)
	}

	// Fetch the full catalog
	refreshCatalog(cfg.NotifyURL, skillAct)
}

// refreshCatalog fetches the full agents catalog from the server.
func refreshCatalog(serverURL string, skillAct *activity.SkillActivities) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", serverURL+"/internal/agents", nil)
	if err != nil {
		log.Printf("Warning: failed to build catalog request: %v", err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Warning: failed to fetch agents catalog: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Warning: agents catalog returned status %d", resp.StatusCode)
		return
	}

	var catalog []activity.AgentCatalogEntry
	if err := json.NewDecoder(resp.Body).Decode(&catalog); err != nil {
		log.Printf("Warning: failed to decode agents catalog: %v", err)
		return
	}

	activity.SetCatalog(skillAct, catalog)
	log.Printf("Fetched agents catalog: %d agents", len(catalog))
}
