package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/claudecode"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/subproc"
	"github.com/victor/temporal-agent/telegram"
	"github.com/victor/temporal-agent/tool"
	"github.com/victor/temporal-agent/web/admin"
	"github.com/victor/temporal-agent/workflow"
)

// catalogRefresh is how often a worker reloads its catalog and routes.
const catalogRefresh = 30 * time.Second

// workerOptions is where `agent worker` and `agent dev` differ.
type workerOptions struct {
	// web is the web channel's notifier: the server's in-process hub in dev
	// mode, the server's internal API through an HTTPNotifier otherwise.
	web activity.Notifier
	// skills is where the skills come from; nil = none.
	skills skill.Store
	// watchSkills reloads them when skills_version moves (a git repository).
	watchSkills bool
}

// workerRuntime is what a process needs to serve Temporal: the tools it runs,
// the catalog of agents and tools, the skills, and one worker per queue.
type workerRuntime struct {
	workers []worker.Worker
	queues  []string
	// workflows: the worker serves the workflow queue (worker.yaml).
	workflows bool
	skills    []skill.Skill      // as loaded at startup
	stop      context.CancelFunc // ends the polling
}

// newWorkerRuntime builds the workers of a process, the same way for `agent
// worker` and `agent dev`: they register the same workflows and activities,
// so what a queue serves does not depend on the mode that serves it.
func newWorkerRuntime(cfg *config.Config, st store.Store, tc client.Client, opts workerOptions) (*workerRuntime, error) {
	llmProvider, err := provider.New(cfg.LLMProvider, cfg.LLMAPIKey, cfg.LLMModel)
	if err != nil {
		return nil, err
	}

	// Fail before polling a queue this worker could not serve to the end.
	if err := checkGitKey(cfg.ClaudeCodeSSHKey); err != nil {
		return nil, fmt.Errorf("git identity: %w", err)
	}

	// A cap the operator set and mistyped must not become no cap at all.
	budget, err := parseBudget(cfg.ClaudeCodeMaxBudgetUSD)
	if err != nil {
		return nil, fmt.Errorf("CLAUDE_CODE_MAX_BUDGET_USD: %w", err)
	}

	maxContext, err := parseContextBytes(cfg.LLMMaxContextBytes)
	if err != nil {
		return nil, fmt.Errorf("LLM_MAX_CONTEXT_BYTES: %w", err)
	}

	// Who pays for a coding run, the API or a subscription, is settled before
	// any run, where the CLI is installed: never left to the CLI picking
	// whichever credential it finds.
	var auth claudecode.Auth
	if (&claudecode.Runner{}).Available() {
		if auth, err = claudecode.ResolveAuth(cfg.ClaudeCodeAuth, os.Environ()); err != nil {
			return nil, err
		}
		log.Printf("Coding runs %s", auth.Describe(os.Environ()))
	}

	runAs, err := subproc.ParseIdentity(cfg.RunAsUID, cfg.RunAsGID)
	if err != nil {
		return nil, err
	}
	if err := prepareRunAs(cfg, runAs); err != nil {
		return nil, fmt.Errorf("RUN_AS_UID: %w", err)
	}

	// One count of the commands run as runAs for the whole process: exec and
	// the coding runs share it, or one would end the other's processes.
	runs := subproc.NewRuns(runAs)

	workerConf := loadWorkerConfig(cfg)
	registry := buildRegistry(cfg, st, tc, runAs, runs, auth)

	skills := loadSkills(opts.skills)
	catalog := initCatalog(st)
	skillAct := activity.NewSkillActivities(skills, catalog)

	// Load activity queue mapping from DB and register for workflow SideEffect access
	workerCfg := activity.NewWorkerConfig()
	workerCfg.SetActivityQueues(loadActivityQueuesFromDB(st))
	activity.SetGlobalWorkerConfig(workerCfg)

	// Expose only the configured tools, add the MCP servers' that answer,
	// and publish them to the DB catalog
	exposeTools(registry, workerConf)
	mcpServers := discoverMCPServers(registry, workerConf)
	queues := workerQueues(cfg, workerConf)
	publishTools(context.Background(), st, tc, registry.All(), workerConf.Queue)
	refreshCatalog(st, catalog) // include the tools just published

	// Notifiers, one per channel a session can reach its user on
	notifiers := map[string]activity.Notifier{activity.ChannelWeb: opts.web}
	if cfg.TelegramBotToken != "" {
		notifiers[telegram.Channel] = &telegram.Notifier{Client: telegram.NewClient(cfg.TelegramBotToken)}
		log.Println("Telegram bot client configured")
	}

	rt := &workerRuntime{queues: queues, workflows: workerConf.Workflows, skills: skills}
	for _, queue := range queues {
		// Sessions pin stateful tool calls to one worker of the tool queue
		w := worker.New(tc, queue, worker.Options{
			EnableSessionWorker: queue == workerConf.Queue,
			// A worker that stops polling for good takes the process with
			// it, so that whatever runs it starts a new one.
			OnFatalError: func(err error) { log.Fatalf("Worker on %q failed: %v", queue, err) },
		})

		w.RegisterWorkflow(workflow.SessionWorkflow)
		w.RegisterWorkflow(workflow.AgentWorkflow)
		w.RegisterWorkflow(workflow.AskUserWorkflow)
		w.RegisterWorkflow(workflow.AnalyzeRepoWorkflow)
		w.RegisterWorkflow(workflow.ImplementFeatureWorkflow)
		w.RegisterWorkflow(workflow.ScheduledAgentWorkflow)
		w.RegisterWorkflow(workflow.ForkSessionWorkflow)
		w.RegisterWorkflow(workflow.ReportToParentWorkflow)

		w.RegisterActivity(&activity.LLMActivities{Provider: llmProvider, Store: st, Catalog: catalog, Prompts: skillAct.Prompts, MaxContextBytes: maxContext})
		w.RegisterActivity(&activity.ForkActivities{Store: st, LLM: llmProvider, Private: catalog})
		w.RegisterActivity(&activity.MemoryActivities{Store: st})
		w.RegisterActivity(&activity.ForkPostActivities{Store: st})
		w.RegisterActivity(&activity.ClaudeCodeActivities{Root: cfg.ClaudeCodeWorkspace, SSHKeyPath: cfg.ClaudeCodeSSHKey, AllowedRepos: cfg.ClaudeCodeRepos, RunAs: runAs, Runs: runs, ClaudeConfigDir: cfg.ClaudeConfigDir, Model: cfg.ClaudeCodeModel, MaxBudgetUSD: budget, Auth: auth})
		w.RegisterActivity(&activity.ToolActivities{Registry: registry, Catalog: catalog})
		w.RegisterActivity(&activity.NotificationActivities{Notifiers: notifiers})
		w.RegisterActivity(&activity.DeliveryActivities{Web: opts.web, Store: st})
		w.RegisterActivity(&activity.ScheduleActivities{Client: tc.ScheduleClient(), Store: st})
		w.RegisterActivity(skillAct)

		rt.workers = append(rt.workers, w)
		log.Printf("Worker registered on task queue %q", queue)
	}

	// Poll DB for activity queue mapping, agents catalog and skills changes
	ctx, stop := context.WithCancel(context.Background())
	rt.stop = stop
	go pollActivityQueues(ctx, st, workerCfg, catalogRefresh)
	go pollCatalog(ctx, st, catalog, catalogRefresh)
	// Started after the startup publish: from here on, this goroutine alone
	// changes and publishes the MCP servers' tools.
	go mcpServers.Run(ctx, catalogPublisher{st: st, tc: tc, queue: workerConf.Queue})
	if opts.watchSkills && opts.skills != nil {
		go watchSkillsVersionDB(ctx, st, catalogRefresh, func() {
			if skills, ok := reloadSkills(opts.skills); ok {
				activity.SetSkills(skillAct, skills)
				log.Printf("Worker reloaded %d skills", len(skills))
			}
		})
	}
	return rt, nil
}

// buildRegistry registers the built-in tools this process can run. Which of
// them it exposes is the worker config's decision (exposeTools); the MCP
// servers' come after (discoverMCPServers).
func buildRegistry(cfg *config.Config, st store.Store, tc client.Client, runAs *subproc.Identity, runs *subproc.Runs, auth claudecode.Auth) *tool.Registry {
	registry := tool.NewRegistry()
	tool.RegisterFilesystemTools(registry, cfg.WorkspacePath, runAs)
	tool.RegisterGrepTool(registry, cfg.WorkspacePath)
	tool.RegisterGlobTool(registry, cfg.WorkspacePath)
	tool.RegisterExecTool(registry, cfg.WorkspacePath, runAs, runs)
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
	tool.RegisterQueryWorkflowTool(registry, tc)
	tool.RegisterScheduleTools(registry, tc.ScheduleClient(), st, workflow.ScheduledAgentWorkflow, cfg.WorkflowQueue)

	// The coding tools only where the CLI is installed: a worker that cannot
	// run a coding session has none to offer.
	if (&claudecode.Runner{}).Available() {
		tool.RegisterClaudeCodeTools(registry, workflow.AnalyzeRepoWorkflow, workflow.ImplementFeatureWorkflow, auth.CostNote())
		if cfg.ClaudeCodeSSHKey != "" {
			log.Printf("Coding runs use the git identity at %s", cfg.ClaudeCodeSSHKey)
		}
		model := cfg.ClaudeCodeModel
		if model == "" {
			model = "the CLI's default"
		}
		if cfg.ClaudeCodeMaxBudgetUSD == "" {
			log.Printf("Coding runs use %s, with no budget cap (CLAUDE_CODE_MAX_BUDGET_USD)", model)
		} else {
			log.Printf("Coding runs use %s, and stop at $%s each", model, cfg.ClaudeCodeMaxBudgetUSD)
		}
		if len(cfg.ClaudeCodeRepos) == 0 {
			log.Println("CLAUDE_CODE_REPOS is empty: coding runs on this worker refuse every repository")
		} else {
			log.Printf("Coding runs may use the repositories %v", cfg.ClaudeCodeRepos)
		}
	}

	return registry
}

// parseBudget reads a budget cap in dollars; empty = none (0).
func parseBudget(raw string) (float64, error) {
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || !(v > 0) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("%q is not a positive amount of dollars", raw)
	}
	return v, nil
}

// parseContextBytes reads LLM_MAX_CONTEXT_BYTES: empty is the default (0), a
// value that is no positive number stops the worker rather than lift the
// bound.
func parseContextBytes(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%q is not a positive number of bytes", raw)
	}
	return v, nil
}

// prepareRunAs gets ready what the user that commands chosen by a model run
// as must reach — its home, the workspace exec and the file tools share — or
// says in the log why those commands will be refused.
func prepareRunAs(cfg *config.Config, runAs *subproc.Identity) error {
	if err := subproc.CheckRunAs(runAs); err != nil {
		log.Printf("ERROR: exec and coding runs are refused on this worker: %v", err)
		return nil
	}
	if runAs == nil {
		return nil
	}
	if err := runAs.PrepareHome(); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.WorkspacePath, 0o755); err != nil {
		return err
	}
	if err := runAs.Give(cfg.WorkspacePath); err != nil {
		return err
	}
	// The CLI's configuration is the worker's: each run works on a copy of
	// its own (claudecode.SeedConfigDir). One an earlier version of the
	// worker gave away is taken back.
	if cfg.ClaudeConfigDir != "" {
		if _, err := os.Lstat(cfg.ClaudeConfigDir); err == nil {
			if err := subproc.ReclaimTree(cfg.ClaudeConfigDir); err != nil {
				return err
			}
		}
	}
	log.Printf("exec and coding runs run as uid %d, gid %d", runAs.UID, runAs.GID)
	return nil
}

// loadSkills loads the skills once at startup. A failure leaves the worker
// without skills rather than down.
func loadSkills(s skill.Store) []skill.Skill {
	if s == nil {
		log.Println("No skills source configured, running without skills")
		return nil
	}
	skills, ok := reloadSkills(s)
	if !ok {
		return nil
	}
	log.Printf("Loaded %d skills", len(skills))
	return skills
}

func reloadSkills(s skill.Store) ([]skill.Skill, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	skills, err := s.LoadAll(ctx)
	if err != nil {
		log.Printf("Warning: failed to load skills: %v", err)
		return nil, false
	}
	return skills, true
}

// start runs every worker in the background.
func (rt *workerRuntime) start() error {
	for _, w := range rt.workers {
		if err := w.Start(); err != nil {
			rt.shutdown()
			return err
		}
	}
	return nil
}

// shutdown stops the polling and the workers.
func (rt *workerRuntime) shutdown() {
	rt.stop()
	for _, w := range rt.workers {
		w.Stop()
	}
}

// httpOptions is where `agent server` and `agent dev` differ on the HTTP
// side: which skills the back-office shows.
type httpOptions struct {
	skills       func() []skill.Skill
	skillsSource string
	// skillsReloadable: the skills come from a repo that the server and the
	// workers reload when skills_version moves.
	skillsReloadable bool
}

// newHTTPHandler builds the public HTTP side: the back-office and the
// adapters over the session service.
func newHTTPHandler(cfg *config.Config, st store.Store, tc client.Client, hub *sse.Hub, opts httpOptions) http.Handler {
	clients, err := auth.ParseClientAddrs(cfg.TrustedProxies)
	if err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}
	if !clients.Known() {
		log.Println("TRUSTED_PROXIES is not set: failed logins are limited per account only, not per client address")
	}
	authSvc := &auth.Service{Store: st, Limits: auth.DefaultLoginLimits(clients), Clients: clients}
	adminUI := admin.New(admin.Config{
		Auth:             authSvc,
		Store:            st,
		Temporal:         tc,
		Skills:           opts.skills,
		SkillsSource:     opts.skillsSource,
		SkillsReloadable: opts.skillsReloadable,
		DefaultAgentID:   cfg.DefaultAgentID,
		WorkflowQueue:    cfg.WorkflowQueue,
	})
	warnClosedWebhooks(cfg)
	return newServer(cfg, st, tc, hub, authSvc, adminUI.Routes()).routes()
}

// dialTemporal connects to Temporal or ends the process.
func dialTemporal(cfg *config.Config) client.Client {
	tlsCfg, err := temporalTLS(cfg)
	if err != nil {
		log.Fatalf("Temporal TLS: %v", err)
	}
	tc, err := client.Dial(client.Options{
		HostPort:          cfg.TemporalHost,
		Namespace:         cfg.TemporalNamespace,
		ConnectionOptions: client.ConnectionOptions{TLS: tlsCfg},
	})
	if err != nil {
		log.Fatalf("Failed to connect to Temporal: %v", err)
	}
	return tc
}

// openStore opens the database or ends the process.
func openStore(cfg *config.Config) *store.PostgresStore {
	st, err := store.NewPostgresStore(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to init store: %v", err)
	}
	return st
}

// waitForSignal blocks until SIGINT or SIGTERM.
func waitForSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
