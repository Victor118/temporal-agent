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

// workerStopTimeout is how long a stopping worker waits for the tasks under
// way, and for their answers to go out, before it cancels them and returns
// (worker.Options.WorkerStopTimeout; zero would not wait at all). A coding
// run ends at once on a stop (activity.RunStop): its CLI is dead
// within claudecode's kill grace (10s), its output drained within as much
// again, and the bound leaves room for the answer. A session's creation
// waits it out whole (its session never ends on its own): a stop takes 30
// to 50s. Whatever runs the process must give it that long before killing
// it (the README says 60s).
const workerStopTimeout = 30 * time.Second

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
	// endRuns ends the coding runs under way, before the workers stop
	// (activity.RunStop).
	endRuns func()
	// releaseRuns gives up this process's claim on the coding runs' root
	// (claimRunsRoot).
	releaseRuns func()
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
	maxFile, err := parseFileBytes(cfg.FilesMaxBytes)
	if err != nil {
		return nil, fmt.Errorf("FILES_MAX_BYTES: %w", err)
	}

	maxRuns, err := parseMaxRuns(cfg.ClaudeCodeMaxConcurrentRuns)
	if err != nil {
		return nil, fmt.Errorf("CLAUDE_CODE_MAX_CONCURRENT_RUNS: %w", err)
	}
	queueWait, err := parseQueueWait(cfg.ClaudeCodeQueueWait)
	if err != nil {
		return nil, fmt.Errorf("CLAUDE_CODE_QUEUE_WAIT: %w", err)
	}
	stallTimeout, err := parseStallTimeout(cfg.ClaudeCodeStallTimeout)
	if err != nil {
		return nil, fmt.Errorf("CLAUDE_CODE_STALL_TIMEOUT: %w", err)
	}

	// Who pays for a coding run, the API or a subscription, is settled before
	// any run, where the CLI is installed: never left to the CLI picking
	// whichever credential it finds.
	coding := (&claudecode.Runner{}).Available()
	var auth claudecode.Auth
	if coding {
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

	stopRuns := &activity.RunStop{}
	codeAct := &activity.ClaudeCodeActivities{Root: cfg.ClaudeCodeWorkspace, SSHKeyPath: cfg.ClaudeCodeSSHKey, AllowedRepos: cfg.ClaudeCodeRepos, RunAs: runAs, Runs: runs, Runner: &claudecode.Runner{StallTimeout: stallTimeout}, ClaudeConfigDir: cfg.ClaudeConfigDir, Model: cfg.ClaudeCodeModel, MaxBudgetUSD: budget, Auth: auth, QueueWait: queueWait, Stopper: stopRuns}
	// Before this worker offers a run: what a run left on this machine's
	// disk when its worker died is reachable from here alone.
	releaseRuns := func() {}
	if coding {
		if releaseRuns, err = claimRunsRoot(codeAct.Root, codeAct.Runs, rootClaimWait); err != nil {
			return nil, err
		}
	}

	workerConf := loadWorkerConfig(cfg)
	registry := buildRegistry(cfg, st, tc, runAs, runs, auth, queueWait, &tool.Publisher{Store: st, MaxBytes: maxFile})

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

	// A coding run is a Temporal session on its tool's queue: all of its
	// steps on the worker that took it.
	if coding {
		log.Printf("Coding runs: at most %d at a time on this worker (CLAUDE_CODE_MAX_CONCURRENT_RUNS); "+
			"a run waits up to %s for a worker with one to spare (CLAUDE_CODE_QUEUE_WAIT)", maxRuns, queueWait)
		if stallTimeout < 0 {
			log.Printf("Coding runs: a CLI that writes nothing is never ended for it (CLAUDE_CODE_STALL_TIMEOUT=0)")
		} else {
			log.Printf("Coding runs: a CLI that writes nothing for %s is ended as stuck (CLAUDE_CODE_STALL_TIMEOUT)", stallTimeout)
		}
	}
	endRuns := func() {}
	if coding {
		endRuns = stopRuns.Stop
	}
	acts := workerActivities(activityDeps{
		llm: llmProvider, store: st, catalog: catalog, skills: skillAct, maxContext: maxContext,
		code: codeAct, registry: registry, notifiers: notifiers, web: opts.web, schedules: tc.ScheduleClient(), relay: tc,
	})
	rt := &workerRuntime{queues: queues, workflows: workerConf.Workflows, skills: skills, endRuns: endRuns, releaseRuns: releaseRuns}
	for _, queue := range queues {
		wopts := worker.Options{
			// A worker that stops polling for good takes the process with
			// it, so that whatever runs it starts a new one.
			OnFatalError:      func(err error) { log.Fatalf("Worker on %q failed: %v", queue, err) },
			WorkerStopTimeout: workerStopTimeout,
		}
		withCodingSessions(&wopts, queue == workerConf.Queue && coding, maxRuns)
		w := worker.New(tc, queue, wopts)

		for _, wf := range workerWorkflows() {
			w.RegisterWorkflow(wf)
		}

		for _, act := range acts {
			w.RegisterActivity(act)
		}

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

// workerWorkflows are the workflows every worker registers, on each of its
// queues. The tests register this very list (TestWorkerWorkflows_Register).
func workerWorkflows() []any {
	return []any{
		workflow.ParticipantWorkflow,
		workflow.AgentWorkflow,
		workflow.AskUserWorkflow,
		workflow.AnalyzeRepoWorkflow,
		workflow.ImplementFeatureWorkflow,
		workflow.ScheduledAgentWorkflow,
		workflow.ForkSessionWorkflow,
		workflow.ReportToParentWorkflow,
	}
}

// activityDeps is what a worker's activities are built from.
type activityDeps struct {
	llm        provider.LLMProvider
	store      store.Store
	catalog    *activity.Catalog
	skills     *activity.SkillActivities
	maxContext int
	code       *activity.ClaudeCodeActivities
	registry   *tool.Registry
	notifiers  map[string]activity.Notifier
	web        activity.Notifier
	schedules  activity.ScheduleHandles
	relay      activity.SignalStarter
}

// workerActivities are the activity structs every worker registers, on each
// of its queues. RegisterActivity makes an activity of every exported method
// of each, and panics on one that returns neither a result nor an error:
// what is not an activity is no exported method of these structs. The tests
// register this very list (TestWorkerActivities_Register) and pin the
// activities it holds (TestWorkerActivities_AreTheActivities).
func workerActivities(d activityDeps) []any {
	return []any{
		&activity.LLMActivities{Provider: d.llm, Store: d.store, Catalog: d.catalog, Prompts: d.skills.Prompts, MaxContextBytes: d.maxContext},
		&activity.ForkActivities{Store: d.store, LLM: d.llm, Private: d.catalog},
		&activity.MemoryActivities{Store: d.store},
		&activity.TurnActivities{Store: d.store},
		&activity.RelayActivities{Client: d.relay},
		&activity.ForkPostActivities{Store: d.store},
		d.code,
		&activity.ToolActivities{Registry: d.registry, Catalog: d.catalog},
		&activity.NotificationActivities{Notifiers: d.notifiers},
		&activity.DeliveryActivities{Web: d.web, Store: d.store},
		&activity.ScheduleActivities{Client: d.schedules, Store: d.store},
		d.skills,
	}
}

// withCodingSessions sets wopts for the coding runs' sessions, which pin a
// run's steps to one worker of the tool queue: only where runs can happen
// (runs: the tool queue, on a worker with the CLI). A worker without the
// CLI takes no session, and answers the probe that it cannot
// (ProbeRunWorker). How many at once is the machine's limit.
func withCodingSessions(wopts *worker.Options, runs bool, maxRuns int) {
	if !runs {
		return
	}
	wopts.EnableSessionWorker = true
	wopts.MaxConcurrentSessionExecutionSize = maxRuns
}

// buildRegistry registers the built-in tools this process can run. Which of
// them it exposes is the worker config's decision (exposeTools); the MCP
// servers' come after (discoverMCPServers).
func buildRegistry(cfg *config.Config, st store.Store, tc client.Client, runAs *subproc.Identity, runs *subproc.Runs, auth claudecode.Auth, queueWait time.Duration, pub *tool.Publisher) *tool.Registry {
	registry := tool.NewRegistry()
	tool.RegisterFilesystemTools(registry, cfg.WorkspacePath, runAs)
	tool.RegisterGrepTool(registry, cfg.WorkspacePath)
	tool.RegisterGlobTool(registry, cfg.WorkspacePath)
	tool.RegisterExecTool(registry, cfg.WorkspacePath, runAs, runs, pub)
	tool.RegisterPublishFileTool(registry, pub)
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

	// The document tools only where pandoc and typst are installed. Each
	// call renders in a directory of its own under the temporary directory,
	// never the workspace.
	if tool.DocumentToolsAvailable() {
		tool.RegisterDocumentTools(registry, &tool.Documents{Dir: os.TempDir(), Packages: cfg.TypstPackages, RunAs: runAs, Runs: runs, Pub: pub, Files: st})
		log.Println("Documents: pandoc and typst are installed, render_pdf and make_slides offered")
	}

	// The coding tools only where the CLI is installed: a worker that cannot
	// run a coding session has none to offer.
	if (&claudecode.Runner{}).Available() {
		tool.RegisterClaudeCodeTools(registry, workflow.AnalyzeRepoWorkflow, workflow.ImplementFeatureWorkflow, auth.CostNote(), queueWait)
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

// defaultMaxRuns is how many coding runs a worker takes at a time when
// CLAUDE_CODE_MAX_CONCURRENT_RUNS is empty.
const defaultMaxRuns = 1

// maxRunsPerUID is how many coding runs may share one RunAs identity: one.
// Runs under a single uid reach each other's clone and credentials, and a
// process one leaves behind outlives it, since subproc.Runs only kills strays
// once no command of that uid is running. Running several needs one uid per
// run slot; until then, a higher limit is refused rather than weaken the
// isolation between runs.
const maxRunsPerUID = 1

// parseMaxRuns reads CLAUDE_CODE_MAX_CONCURRENT_RUNS: empty is the default, a
// value that is no positive number stops the worker rather than lift the
// limit (the SDK reads 0 as 1000), and so does one past maxRunsPerUID.
func parseMaxRuns(raw string) (int, error) {
	if raw == "" {
		return defaultMaxRuns, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%q is not a positive number of runs", raw)
	}
	if v > maxRunsPerUID {
		return 0, fmt.Errorf("%d runs at a time is not supported yet: runs sharing the one RUN_AS_UID could reach "+
			"each other's clone and credentials, and a process one left behind would outlive it; "+
			"that takes one uid per run slot first (set %d)", v, maxRunsPerUID)
	}
	return v, nil
}

// parseQueueWait reads CLAUDE_CODE_QUEUE_WAIT, a Go duration: empty is
// activity.DefaultRunQueueWait, and what is no positive duration stops the
// worker rather than leave runs waiting for ever (the SDK reads zero as no
// bound).
func parseQueueWait(raw string) (time.Duration, error) {
	if raw == "" {
		return activity.DefaultRunQueueWait, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%q is not a positive duration (e.g. 30m)", raw)
	}
	return d, nil
}

// parseStallTimeout reads CLAUDE_CODE_STALL_TIMEOUT, a Go duration: empty is
// claudecode.DefaultStallTimeout, zero turns the check off (a negative
// Runner.StallTimeout), and anything else stops the worker rather than end
// runs at a limit nobody chose.
func parseStallTimeout(raw string) (time.Duration, error) {
	if raw == "" {
		return claudecode.DefaultStallTimeout, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("%q is not a duration (e.g. 12m; 0 = never)", raw)
	}
	if d == 0 {
		return -1, nil
	}
	return d, nil
}

// rootClaimWait bounds how long a starting worker waits for another worker
// process sweeping the coding runs' root (activity.RootClaim).
const rootClaimWait = 2 * time.Minute

// claimRunsRoot claims the coding runs' root for this process, then deletes
// what runs left there when their worker stopped mid-run, and returns the
// release of the claim, to call when this process stops
// (activity.RootClaim). Failing to claim the root is an error: a worker
// serving runs without a claim could see its clones deleted by the next one
// to start. A failed deletion is only logged.
func claimRunsRoot(root string, runs activity.RunCounter, wait time.Duration) (release func(), err error) {
	claim, err := activity.ClaimRoot(root, wait)
	if err != nil {
		return nil, fmt.Errorf("claim the coding runs' workspace %q (CLAUDE_CODE_WORKSPACE): %w", root, err)
	}
	removed, err := claim.Sweep(workflow.RunWorkspaceLifetime, runs)
	if len(removed) > 0 {
		log.Printf("Removed %d leftovers of coding runs that are over from %s", len(removed), root)
	}
	if err != nil {
		log.Printf("Warning: sweeping the coding runs' workspaces: %v", err)
	}
	if err := claim.Share(wait); err != nil {
		claim.Release()
		return nil, fmt.Errorf("share the claim on the coding runs' workspace %q: %w", root, err)
	}
	return claim.Release, nil
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

// parseFileBytes reads FILES_MAX_BYTES: empty is tool.DefaultMaxFileBytes,
// and a value that is no positive number stops the worker rather than lift
// the bound.
func parseFileBytes(raw string) (int64, error) {
	if raw == "" {
		return tool.DefaultMaxFileBytes, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
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
		log.Printf("ERROR: exec, documents and coding runs are refused on this worker: %v", err)
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
	log.Printf("exec, documents and coding runs run as uid %d, gid %d", runAs.UID, runAs.GID)
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

// shutdown stops the polling and the workers. The coding runs end first:
// each answers while its worker still waits for it (workerStopTimeout), so
// that Temporal records that the worker is gone before the process ends.
// The SDK stops the workflow poller first: the next worker of the queue
// (another replica, or this one restarted) reads that answer, not this one.
func (rt *workerRuntime) shutdown() {
	rt.stop()
	rt.endRuns()
	for _, w := range rt.workers {
		w.Stop()
	}
	rt.releaseRuns()
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
