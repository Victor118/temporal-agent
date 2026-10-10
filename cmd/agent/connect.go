package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/victor/temporal-agent/claudecode"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/machine/connect"
	"github.com/victor/temporal-agent/provider"
)

// version is the binary's version, which a machine announces; set at build
// time (-ldflags "-X main.version=…").
var version = "dev"

// connectCmd is `agent connect`: this machine, its owner's, runs the
// directives the server sends it. It opens neither the database nor
// Temporal: it holds a machine token, and a WebSocket to the server.
var connectCmd = &cobra.Command{
	Use:   "connect",
	Short: "Connect this machine to a server, to run what its owner's agents send it (no database, no Temporal)",
	Long: `Connects this machine to a server's gateway and runs the directives it sends.

The first time, enroll it: agent connect --join https://agent.example.com
shows a code to type in the server's "Mes machines" page. From a script,
--token-stdin reads an enrollment token (created on that page) from the
standard input instead. The machine's token is then kept in its directory
(0600), and agent connect reconnects by itself.`,
	Run: runConnect,
}

func init() {
	f := connectCmd.Flags()
	f.String("join", "", "enroll this machine on the server at this URL (https://…)")
	f.String("name", "", "the machine's name, at enrollment (default: the host name)")
	f.Bool("token-stdin", false, "with --join: read an enrollment token from the standard input instead of showing a code")
	f.String("dir", "", "the machine's directory (default: $XDG_CONFIG_HOME/agent/machine)")
	f.Int("max-directives", 1, "how many directives this machine runs at once")
	f.String("repos", os.Getenv("AGENT_CONNECT_REPOS"), "repositories a run may clone (and push to, with --allow-push), comma-separated globs (* stops at /); empty = every one refused (env AGENT_CONNECT_REPOS)")
	f.Bool("allow-push", os.Getenv("AGENT_CONNECT_ALLOW_PUSH") == "true", "let implement_feature run here and push its branch (agent/…) with your git identity, to a repository of --repos; off = this machine runs no implementation (env AGENT_CONNECT_ALLOW_PUSH=true)")
	f.Float64("max-budget-usd", 0, "what one Claude Code run may spend at most, in dollars; 0 = no cap (env AGENT_CONNECT_MAX_BUDGET_USD)")
	f.String("claude-auth", os.Getenv("CLAUDE_CODE_AUTH"), "who pays the runs: api (ANTHROPIC_API_KEY) or subscription (your Claude login); empty = the one credential present (env CLAUDE_CODE_AUTH)")
	f.String("claude-model", os.Getenv("CLAUDE_CODE_MODEL"), "model of the runs; empty = the CLI's default (env CLAUDE_CODE_MODEL)")
	f.String("work-dir", "", "where the runs' clones go, deleted after each (default: the user's cache, agent/runs/<machine>)")
	f.String("llm-provider", os.Getenv("AGENT_CONNECT_LLM_PROVIDER"), "run the model of your turns here, for the agents that allow it, with this provider (anthropic) and your key in AGENT_CONNECT_LLM_API_KEY; empty = no model here (env AGENT_CONNECT_LLM_PROVIDER)")
	f.String("llm-model", os.Getenv("AGENT_CONNECT_LLM_MODEL"), "the model of your turns, whatever the server asks (env AGENT_CONNECT_LLM_MODEL)")
	f.Int("llm-max-concurrent", machine.DefaultMaxLLM, "how many calls to the model this machine makes at once, apart from its directives")
}

func runConnect(cmd *cobra.Command, args []string) {
	join, _ := cmd.Flags().GetString("join")
	name, _ := cmd.Flags().GetString("name")
	tokenStdin, _ := cmd.Flags().GetBool("token-stdin")
	dir, _ := cmd.Flags().GetString("dir")
	maxDirectives, _ := cmd.Flags().GetInt("max-directives")
	if maxDirectives < 1 || maxDirectives > machine.MaxDirectives {
		log.Fatalf("--max-directives: between 1 and %d", machine.MaxDirectives)
	}
	if dir == "" {
		var err error
		if dir, err = connect.DefaultDir(); err != nil {
			log.Fatalf("The machine's directory: %v", err)
		}
	}
	state := connect.State{Dir: dir}
	if err := state.Init(); err != nil {
		log.Fatalf("The machine's directory %s: %v", dir, err)
	}
	unlock, err := state.Lock()
	if errors.Is(err, connect.ErrLocked) {
		log.Fatalf("%v: one agent connect per machine (or give another --dir)", err)
	}
	if err != nil {
		log.Fatalf("Lock the machine's directory %s: %v", dir, err)
	}
	defer unlock()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	executors := map[string]connect.Executor{machine.KindEcho: connect.Echo}
	coder, err := newCoder(cmd)
	if err != nil {
		log.Fatal(err)
	}
	modeler, maxLLM, err := newModeler(cmd)
	if err != nil {
		log.Fatal(err)
	}
	if modeler != nil {
		executors[machine.KindLLM] = modeler.Call
	}
	if coder != nil {
		executors[machine.KindAnalyzeRepo] = coder.Analyze
		executors[machine.KindImplementFeature] = coder.Implement
	}
	status := func() connect.Status {
		s := connect.Status{Capabilities: []string{machine.KindEcho}, ClaudeCode: string(claudecode.LoginAbsent)}
		if coder != nil {
			s.ClaudeCode = string(coder.Login())
			if s.ClaudeCode == string(claudecode.LoginOK) {
				s.Capabilities = append(s.Capabilities, machine.CapClaudeCode)
				if coder.PluginDir {
					s.Capabilities = append(s.Capabilities, machine.CapRunSkills)
				}
			}
			if coder.AllowPush {
				s.Capabilities = append(s.Capabilities, machine.CapGitPush)
			}
		}
		if modeler != nil {
			s.LLM = modeler.State()
			if modeler.Offered() {
				s.Capabilities = append(s.Capabilities, machine.CapLLM)
			}
		}
		return s
	}
	if tokenStdin && join == "" {
		log.Fatal("--token-stdin goes with --join")
	}
	if join != "" {
		if cfg, err := state.Load(); err == nil {
			log.Fatalf("This machine is already enrolled as %q on %s. To enroll it again, revoke it in « Mes machines » and remove %s",
				cfg.Name, cfg.Server, dir)
		}
		if err := enroll(ctx, state, join, name, tokenStdin, status().Capabilities, maxDirectives); err != nil {
			log.Fatal(err)
		}
	}

	c := &connect.Client{State: state, Executors: executors, Status: status, MaxDirectives: maxDirectives,
		OS: runtime.GOOS + "/" + runtime.GOARCH, Version: version}
	if modeler != nil {
		c.MaxLLM, c.LLMProvider, c.LLMModel = maxLLM, modeler.ProviderName, modeler.Model
		modeler.OnRefused = c.Refresh
		log.Printf("connect: the model of your turns runs here for the agents that allow it: %s %s, %d calls at a time, "+
			"billed to your key (AGENT_CONNECT_LLM_API_KEY). The machine gets the agent's prompt, its tools and the session's conversation, "+
			"and decides which tools the turn calls", modeler.ProviderName, modeler.Model, maxLLM)
	}
	cfg, err := state.Load()
	if err != nil {
		log.Fatal(err)
	}
	if coder != nil {
		coder.OnLoginRefused = c.Refresh
		coder.Upload = c.Upload
		c.OnConnect = coder.Retry
		if coder.WorkDir == "" {
			cache, err := os.UserCacheDir()
			if err != nil {
				log.Fatalf("--work-dir: %v", err)
			}
			coder.WorkDir = filepath.Join(cache, "agent", "runs", cfg.MachineID)
		}
		if err := coder.Sweep(); err != nil {
			log.Printf("connect: clean %s: %v", coder.WorkDir, err)
		}
		describeCoder(coder)
	}
	log.Printf("connect: machine %q of %s, running %v, %d at a time (%s)", cfg.Name, cfg.Server, c.Capabilities(), maxDirectives, dir)
	if err := c.Run(ctx); err != nil {
		if errors.Is(err, connect.ErrRefused) {
			log.Fatalf("Stopped: %v", err)
		}
		log.Fatal(err)
	}
	log.Println("connect: stopped")
}

// enroll enrolls the machine, by a code its owner types in the front, or by
// an enrollment token read from the standard input (never an argument: it
// would show in the process list and the shell's history).
func enroll(ctx context.Context, state connect.State, join, name string, tokenStdin bool, capabilities []string, maxDirectives int) error {
	server, err := connect.CheckServer(join)
	if err != nil {
		return err
	}
	if name == "" {
		if name, err = os.Hostname(); err != nil {
			name = "machine"
		}
	}
	en := &connect.Enroller{Server: server, Out: os.Stdout}
	req := machine.EnrollRequest{Name: name, OS: runtime.GOOS + "/" + runtime.GOARCH, MaxDirectives: maxDirectives, AgentVersion: version,
		Capabilities: capabilities}
	var grant machine.TokenGrant
	if tokenStdin {
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil && line == "" {
			return fmt.Errorf("read the enrollment token from the standard input: %w", err)
		}
		req.EnrollmentToken = strings.TrimSpace(line)
		grant, err = en.ByToken(ctx, req)
		if err != nil {
			return err
		}
	} else if grant, err = en.ByCode(ctx, req); err != nil {
		return err
	}
	if err := state.Save(connect.Config{Server: server, MachineID: grant.MachineID, Name: grant.Name, Token: grant.Token}); err != nil {
		return fmt.Errorf("keep the machine's token: %w", err)
	}
	fmt.Printf("Machine « %s » inscrite sur %s.\n", grant.Name, server)
	return nil
}

// newCoder is the machine's analyze_repo and implement_feature, when the
// claude CLI is installed (nil otherwise): who pays is settled here, as on a
// worker (claudecode.ResolveAuth): both credentials in the environment and
// no --claude-auth stops agent connect.
func newCoder(cmd *cobra.Command) (*connect.Coder, error) {
	runner := claudecode.Runner{}
	if !runner.Available() {
		log.Println("connect: no claude CLI in PATH: this machine runs no Claude Code (install it, then restart agent connect)")
		return nil, nil
	}
	f := cmd.Flags()
	mode, _ := f.GetString("claude-auth")
	auth, err := claudecode.ResolveAuth(mode, os.Environ())
	if err != nil {
		return nil, fmt.Errorf("Claude Code: %w (--claude-auth api or subscription)", err)
	}
	budget, _ := f.GetFloat64("max-budget-usd")
	if !f.Changed("max-budget-usd") {
		if raw := os.Getenv("AGENT_CONNECT_MAX_BUDGET_USD"); raw != "" {
			if budget, err = strconv.ParseFloat(raw, 64); err != nil || budget < 0 {
				return nil, fmt.Errorf("AGENT_CONNECT_MAX_BUDGET_USD=%q: not an amount of dollars", raw)
			}
		}
	}
	if budget < 0 {
		return nil, fmt.Errorf("--max-budget-usd %g: not an amount of dollars", budget)
	}
	reposFlag, _ := f.GetString("repos")
	var repos []string
	for _, r := range strings.Split(reposFlag, ",") {
		if r = strings.TrimSpace(r); r != "" {
			repos = append(repos, r)
		}
	}
	model, _ := f.GetString("claude-model")
	workDir, _ := f.GetString("work-dir")
	allowPush, _ := f.GetBool("allow-push")
	home, _ := os.UserHomeDir()
	// Whether the CLI loads a run's skills: read once, from its --help.
	pluginDir := runner.SupportsFlag(context.Background(), claudecode.PluginDirFlag)
	return &connect.Coder{Runner: runner, Auth: auth, Repos: repos, AllowPush: allowPush, MaxBudgetUSD: budget, Model: model,
		WorkDir: workDir, Environ: os.Environ(), Home: home, PluginDir: pluginDir}, nil
}

// newModeler is the machine's model, when --llm-provider is given (nil
// otherwise): the provider of the binary's registry (provider.New), the
// owner's key from AGENT_CONNECT_LLM_API_KEY only (never ANTHROPIC_API_KEY,
// which Claude Code's runs read or drop by their own rule), and a model.
func newModeler(cmd *cobra.Command) (*connect.Modeler, int, error) {
	f := cmd.Flags()
	name, _ := f.GetString("llm-provider")
	model, _ := f.GetString("llm-model")
	maxLLM, _ := f.GetInt("llm-max-concurrent")
	if name == "" {
		if model != "" {
			return nil, 0, fmt.Errorf("--llm-model goes with --llm-provider")
		}
		return nil, 0, nil
	}
	key := os.Getenv("AGENT_CONNECT_LLM_API_KEY")
	switch {
	case key == "":
		return nil, 0, fmt.Errorf("--llm-provider %s: put your API key in AGENT_CONNECT_LLM_API_KEY", name)
	case model == "":
		return nil, 0, fmt.Errorf("--llm-provider %s: name the model with --llm-model (e.g. claude-sonnet-5)", name)
	case len(model) > machine.MaxLLMModelBytes:
		return nil, 0, fmt.Errorf("--llm-model: at most %d bytes", machine.MaxLLMModelBytes)
	case maxLLM < 1 || maxLLM > machine.MaxLLM:
		return nil, 0, fmt.Errorf("--llm-max-concurrent: between 1 and %d", machine.MaxLLM)
	}
	p, err := provider.New(name, key, model)
	if err != nil {
		return nil, 0, fmt.Errorf("--llm-provider: %w", err)
	}
	return &connect.Modeler{Provider: p, ProviderName: name, Model: model}, maxLLM, nil
}

// describeCoder says at startup who pays the runs, and what keeps them in
// bounds: the owner sees it before any run.
func describeCoder(a *connect.Coder) {
	switch a.Auth {
	case claudecode.AuthAPI:
		log.Println("connect: Claude Code runs are billed to the Anthropic API (ANTHROPIC_API_KEY)")
	case claudecode.AuthSubscription:
		log.Println("connect: Claude Code runs use your Claude subscription (the CLI's login, or CLAUDE_CODE_OAUTH_TOKEN); " +
			"an ANTHROPIC_API_KEY in your shell is not passed to them")
	}
	switch a.Login() {
	case claudecode.LoginOK:
		log.Println("connect: Claude Code: logged in")
	case claudecode.LoginNone:
		log.Println("connect: Claude Code: no login found: run claude then /login (or claude setup-token); " +
			"agent connect announces it within 30 s once there")
	}
	if len(a.Repos) == 0 {
		log.Println("connect: --repos is empty: every run is refused (give the repositories you allow, e.g. --repos 'git@github.com:me/*')")
	} else {
		log.Printf("connect: runs may clone %v", a.Repos)
	}
	if a.AllowPush {
		log.Println("connect: --allow-push: implementations run here, and push their branch (agent/…) to those repositories with your git identity")
	} else {
		log.Println("connect: no --allow-push: implementations run elsewhere (the installation's fallback), never here")
	}
	switch {
	case a.MaxBudgetUSD > 0:
		log.Printf("connect: each run stops at $%g", a.MaxBudgetUSD)
	case a.Auth == claudecode.AuthSubscription:
		log.Println("connect: no spending cap per run (--max-budget-usd): with a subscription, a cap also keeps one run " +
			"from eating your usage limit, which your own Claude sessions share")
	default:
		log.Println("connect: no spending cap per run (--max-budget-usd)")
	}
	if a.PluginDir {
		log.Println("connect: the runs take along the skills their agent marks for them (runs: true), as a plugin of the run alone; " +
			"each run's are named here when it starts")
	} else {
		log.Printf("connect: the claude CLI has no %s: a run that takes its agent's skills is refused here (update the CLI)", claudecode.PluginDirFlag)
	}
	log.Printf("connect: clones go to %s, deleted after each run", a.WorkDir)
}
