package main

import (
	"context"
	"log"
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
	"github.com/victor/temporal-agent/telegram"
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
	tool.RegisterEmailTool(registry, tool.SMTPConfig{
		Host:     cfg.SMTPHost,
		Port:     cfg.SMTPPort,
		Username: cfg.SMTPUsername,
		Password: cfg.SMTPPassword,
		From:     cfg.SMTPFrom,
	})
	tool.RegisterSpawnTool(registry, workflow.AgentWorkflow)
	tool.RegisterAskUserTool(registry, workflow.AskUserWorkflow)
	tool.RegisterMemoryTools(registry, st)

	// MCP servers — only load those assigned to this worker's task queues
	mcpServers := cfg.MCPServersForQueues(cfg.TaskQueues)
	if len(mcpServers) > 0 {
		mcpConfigs := make([]tool.MCPServerConfig, len(mcpServers))
		for i, s := range mcpServers {
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

	// Register this worker's queues in the catalog (DB)
	catalog := registerAndLoadCatalog(st, cfg)
	skillAct := activity.NewSkillActivities(prompts, catalog)

	// Load activity queue mapping from DB and register for workflow SideEffect access
	workerCfg := activity.NewWorkerConfig()
	workerCfg.SetActivityQueues(loadActivityQueuesFromDB(st))
	activity.SetGlobalWorkerConfig(workerCfg)

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

	// Notification bridge: POST to server's internal endpoint (SSE requires HTTP)
	notifier := activity.NewHTTPNotifier(cfg.NotifyURL)

	// Telegram client (optional)
	var tgClient activity.TelegramSender
	if cfg.TelegramBotToken != "" {
		tgClient = telegram.NewClient(cfg.TelegramBotToken)
		log.Println("Telegram bot client configured")
	}

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
		w.RegisterActivity(&activity.NotificationActivities{Hub: notifier, Telegram: tgClient})
		w.RegisterActivity(&activity.DeliveryActivities{Hub: notifier, Store: st})
		w.RegisterActivity(&activity.ScheduleActivities{Client: temporalClient, Store: st})
		w.RegisterActivity(skillAct)

		workers = append(workers, w)
		log.Printf("Worker registered on task queue %q", queue)
	}

	// Poll DB for activity queue mapping changes
	ctx, stopPoll := context.WithCancel(context.Background())
	go pollActivityQueues(ctx, st, workerCfg, 30*time.Second)

	// Poll DB for skills version changes
	if cfg.SkillsRepo != "" && skillStore != nil {
		go watchSkillsVersionDB(ctx, st, 30*time.Second, func() {
			reloadCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			skills, err := skillStore.LoadAll(reloadCtx)
			if err != nil {
				log.Printf("Error reloading skills: %v", err)
				return
			}
			activity.SetPrompts(skillAct, activity.BuildSkillPrompts(skills, cfg.TaskQueueSkills))

			// Refresh catalog from DB
			agents := loadCatalogFromDB(st)
			activity.SetCatalog(skillAct, agents)

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

// registerAndLoadCatalog upserts this worker's queues into the DB catalog
// and returns the full catalog for all agents.
func registerAndLoadCatalog(st store.Store, cfg *config.Config) []activity.AgentCatalogEntry {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for queue, skillNames := range cfg.TaskQueueSkills {
		if err := st.UpsertAgent(ctx, queue, skillNames); err != nil {
			log.Printf("Warning: failed to register queue %q in catalog: %v", queue, err)
			continue
		}
		log.Printf("Registered queue %q in catalog", queue)
	}

	return loadCatalogFromDB(st)
}

// loadCatalogFromDB reads the full agent catalog from PostgreSQL.
func loadCatalogFromDB(st store.Store) []activity.AgentCatalogEntry {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	agents, err := st.ListAgents(ctx)
	if err != nil {
		log.Printf("Warning: failed to load agents catalog from DB: %v", err)
		return nil
	}

	catalog := make([]activity.AgentCatalogEntry, len(agents))
	for i, a := range agents {
		catalog[i] = activity.AgentCatalogEntry{TaskQueue: a.TaskQueue, Skills: a.Skills}
	}
	log.Printf("Loaded agents catalog from DB: %d agents", len(catalog))
	return catalog
}

// loadActivityQueuesFromDB reads the activity → task queue mapping from PostgreSQL.
func loadActivityQueuesFromDB(st store.Store) map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m, err := st.GetActivityQueueMap(ctx)
	if err != nil {
		log.Printf("Warning: failed to load activity queue mapping from DB: %v", err)
		return nil
	}
	if len(m) > 0 {
		log.Printf("Loaded activity queue mapping from DB: %v", m)
	}
	return m
}

// pollActivityQueues periodically refreshes the activity → task queue mapping from DB.
func pollActivityQueues(ctx context.Context, st store.Store, cfg *activity.WorkerConfig, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cfg.SetActivityQueues(loadActivityQueuesFromDB(st))
		}
	}
}

// watchSkillsVersionDB polls PostgreSQL for skills version changes.
func watchSkillsVersionDB(ctx context.Context, st store.Store, interval time.Duration, onReload func()) {
	var currentVersion int64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			v, err := st.GetSkillsVersion(ctx)
			if err != nil {
				log.Printf("Warning: failed to poll skills version from DB: %v", err)
				continue
			}
			if v > currentVersion {
				currentVersion = v
				log.Printf("Skills version changed to %d, reloading...", v)
				onReload()
			}
		}
	}
}
