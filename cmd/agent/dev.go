package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/telegram"
	"github.com/victor/temporal-agent/tool"
	"github.com/victor/temporal-agent/web"
	"github.com/victor/temporal-agent/workflow"
)

var devCmd = &cobra.Command{
	Use:   "dev",
	Short: "Start both server and worker in a single process (for development)",
	Run:   runDev,
}

func runDev(cmd *cobra.Command, args []string) {
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

	// Skills — dev mode loads from local filesystem
	skillStore := &skill.FileStore{Dir: "./skills"}
	skills, err := skillStore.LoadAll(context.Background())
	if err != nil {
		log.Printf("Warning: failed to load skills: %v", err)
	}
	if len(skills) > 0 {
		log.Printf("Loaded %d skills from ./skills", len(skills))
	} else {
		log.Println("No skills found in ./skills")
	}
	prompts := activity.BuildSkillPrompts(skills, cfg.TaskQueueSkills)

	// In dev mode, build the catalog locally (no server to register with)
	var catalog []activity.AgentCatalogEntry
	for queue, skillNames := range cfg.TaskQueueSkills {
		log.Printf("Task queue %q: skills %v", queue, skillNames)
		catalog = append(catalog, activity.AgentCatalogEntry{TaskQueue: queue, Skills: skillNames})
	}
	skillAct := activity.NewSkillActivities(prompts, catalog)

	// Load activity queue mapping from DB and register for workflow SideEffect access
	workerCfg := activity.NewWorkerConfig()
	workerCfg.SetActivityQueues(loadActivityQueuesFromDB(st))
	activity.SetGlobalWorkerConfig(workerCfg)

	// SSE hub (in-memory, shared between server and worker)
	hub := sse.NewHub()

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

	// Telegram client (optional)
	var tgClient activity.TelegramSender
	if cfg.TelegramBotToken != "" {
		tgClient = telegram.NewClient(cfg.TelegramBotToken)
		log.Println("Telegram bot client configured")
	}

	// Workers — one per task queue
	var workers []worker.Worker
	for _, queue := range cfg.TaskQueues {
		w := worker.New(temporalClient, queue, worker.Options{})

		w.RegisterWorkflow(workflow.SessionWorkflow)
		w.RegisterWorkflow(workflow.AgentWorkflow)
		w.RegisterWorkflow(workflow.AskUserWorkflow)
		w.RegisterWorkflow(workflow.ScheduledAgentWorkflow)

		w.RegisterActivity(&activity.LLMActivities{Provider: llmProvider})
		w.RegisterActivity(&activity.MemoryActivities{Store: st})
		w.RegisterActivity(&activity.ToolActivities{Registry: registry})
		w.RegisterActivity(&activity.NotificationActivities{Hub: hub, Telegram: tgClient})
		w.RegisterActivity(&activity.DeliveryActivities{Hub: hub, Store: st})
		w.RegisterActivity(&activity.ScheduleActivities{Client: temporalClient, Store: st})
		w.RegisterActivity(skillAct)

		workers = append(workers, w)
		log.Printf("Worker registered on task queue %q", queue)
	}

	// Poll DB for activity queue mapping changes
	go pollActivityQueues(context.Background(), st, workerCfg, 30*time.Second)

	// Start all workers in background
	for _, w := range workers {
		go func(w worker.Worker) {
			if err := w.Run(worker.InterruptCh()); err != nil {
				log.Printf("Worker failed: %v", err)
			}
		}(w)
	}

	// HTTP server
	h := &handler{
		temporalClient: temporalClient,
		hub:            hub,
		cfg:            cfg,
		registry:       registry,
		store:          st,
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(corsMiddleware)

	// Unauthenticated routes
	r.Get("/", web.HandleIndex)
	r.Post("/auth/login", h.login)
	r.Post("/auth/logout", h.logout)
	r.Post("/webhooks/telegram", h.handleTelegramWebhook)

	// Authenticated routes
	r.Group(func(g chi.Router) {
		g.Use(authMiddleware(cfg))

		g.Get("/auth/check", h.checkAuth)
		g.Post("/sessions", h.createSession)
		g.Get("/users/{userID}/sessions", h.listSessions)
		g.Get("/users/{userID}/notifications", h.getNotifications)
		g.Get("/users/{userID}/notifications/stream", h.streamNotifications)
		g.Delete("/users/{userID}/notifications/{notifID}", h.deleteNotification)
		g.Delete("/users/{userID}/notifications", h.deleteAllNotifications)
		g.Post("/sessions/{id}/messages", h.sendMessage)
		g.Post("/sessions/{id}/cancel", h.cancelAgent)
		g.Delete("/sessions/{id}", h.deleteSession)
		g.Get("/sessions/{id}/state", h.getState)
		g.Get("/sessions/{id}/history", h.getHistory)
		g.Get("/sessions/{id}/stream", h.stream)
		g.Post("/sessions/{id}/answer", h.answerQuestion)

		// Admin routes
		g.Get("/admin/queues", h.listKnownQueues)
		g.Get("/admin/activity-queues", h.listActivityQueues)
		g.Put("/admin/activity-queues", h.setActivityQueue)
		g.Delete("/admin/activity-queues/{activityName}", h.deleteActivityQueue)
	})

	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: r}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		for _, w := range workers {
			w.Stop()
		}
	}()

	log.Printf("Dev mode: API on %s, workers on queues %v", cfg.HTTPAddr, cfg.TaskQueues)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
}
