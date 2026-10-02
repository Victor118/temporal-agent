package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/telegram"
	"github.com/victor/temporal-agent/tool"
	"github.com/victor/temporal-agent/web/admin"
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
		llmProvider = provider.NewAnthropicProvider(cfg.LLMAPIKey, cfg.LLMModel)
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
	tool.RegisterAskUserTool(registry, workflow.AskUserWorkflow)
	tool.RegisterMemoryTools(registry, st)

	// Worker config — queue served by this worker and the tools it exposes there
	workerConf := loadWorkerConfig(cfg)
	registerMCPServers(registry, workerConf.MCP)

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

	// Agents — dev mode seeds the DB like the server, then reads the catalog
	// (agents + tools) from it
	if err := seedAgents(st, cfg.AgentsFile); err != nil {
		log.Fatalf("Failed to seed agents: %v", err)
	}
	catalog := initCatalog(st)
	skillAct := activity.NewSkillActivities(skills, catalog)

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
	tool.RegisterScheduleTools(registry, temporalClient.ScheduleClient(), st, workflow.ScheduledAgentWorkflow, cfg.WorkflowQueue)

	// Expose only the configured tools and publish them to the DB catalog
	exposeTools(registry, workerConf)
	queues := workerQueues(cfg, workerConf)
	publishTools(st, temporalClient, registry, workerConf.Queue)
	refreshCatalog(st, catalog) // include the tools just published

	// Telegram client (optional)
	// Notifiers, one per channel a session can reach its user on
	notifiers := map[string]activity.Notifier{activity.ChannelWeb: activity.HubNotifier{Hub: hub}}
	if cfg.TelegramBotToken != "" {
		notifiers[telegram.Channel] = &telegram.Notifier{Client: telegram.NewClient(cfg.TelegramBotToken)}
		log.Println("Telegram bot client configured")
	}

	// Workers — one per task queue
	var workers []worker.Worker
	for _, queue := range queues {
		// Sessions pin stateful tool calls to one worker of the tool queue
		w := worker.New(temporalClient, queue, worker.Options{
			EnableSessionWorker: queue == workerConf.Queue,
		})

		w.RegisterWorkflow(workflow.SessionWorkflow)
		w.RegisterWorkflow(workflow.AgentWorkflow)
		w.RegisterWorkflow(workflow.AskUserWorkflow)
		w.RegisterWorkflow(workflow.ScheduledAgentWorkflow)
		w.RegisterWorkflow(workflow.ForkSessionWorkflow)

		w.RegisterActivity(&activity.LLMActivities{Provider: llmProvider})
		w.RegisterActivity(&activity.ForkActivities{Store: st, LLM: llmProvider})
		w.RegisterActivity(&activity.MemoryActivities{Store: st})
		w.RegisterActivity(&activity.ToolActivities{Registry: registry, Catalog: catalog})
		w.RegisterActivity(&activity.NotificationActivities{Notifiers: notifiers})
		w.RegisterActivity(&activity.DeliveryActivities{Hub: hub, Store: st})
		w.RegisterActivity(&activity.ScheduleActivities{Client: temporalClient.ScheduleClient(), Store: st})
		w.RegisterActivity(skillAct)

		workers = append(workers, w)
		log.Printf("Worker registered on task queue %q", queue)
	}

	// Poll DB for activity queue mapping and agents catalog changes
	go pollActivityQueues(context.Background(), st, workerCfg, 30*time.Second)
	go pollCatalog(context.Background(), st, catalog, 30*time.Second)

	// Start all workers in background
	for _, w := range workers {
		go func(w worker.Worker) {
			if err := w.Run(worker.InterruptCh()); err != nil {
				log.Printf("Worker failed: %v", err)
			}
		}(w)
	}

	// Back-office — shows the skills dev mode loaded from ./skills
	authSvc := &auth.Service{Store: st, Limits: auth.DefaultLoginLimits()}
	adminUI := admin.New(admin.Config{
		Auth:           authSvc,
		Store:          st,
		Temporal:       temporalClient,
		Skills:         func() []skill.Skill { return skills },
		SkillsSource:   skillStore.Dir,
		DefaultAgentID: cfg.DefaultAgentID,
		WorkflowQueue:  cfg.WorkflowQueue,
	})

	// HTTP server
	warnClosedWebhooks(cfg)
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: newServer(cfg, st, temporalClient, hub, authSvc, adminUI.Routes()).routes()}

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

	log.Printf("Dev mode: API on %s, workers on queues %v", cfg.HTTPAddr, queues)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
}
