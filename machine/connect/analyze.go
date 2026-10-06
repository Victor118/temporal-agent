//go:build unix

package connect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/victor/temporal-agent/claudecode"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/subproc"
)

// Analyzer runs analyze_repo directives with the machine's own claude CLI:
// its owner's login, subscription or key, under its owner's user. What keeps
// a run within bounds is set here, never by the server (design §8): the
// repositories it may clone, what a run may spend, that an analysis is read
// only (machine.AnalyzePermissionMode and AnalyzeSystemPrompt, the same as on
// the installation's workers).
type Analyzer struct {
	// Runner runs the CLI (Binary, StallTimeout); its Auth, OnBeat and
	// heartbeat are set per run.
	Runner claudecode.Runner
	// Auth is who pays (claudecode.ResolveAuth, read on this machine): the
	// other mode's credential never reaches the CLI.
	Auth claudecode.Auth
	// Repos are the repositories it may clone (globs, path.Match: * stops
	// at a /); empty = every one refused.
	Repos []string
	// MaxBudgetUSD caps what one run spends; 0 = no cap.
	MaxBudgetUSD float64
	// Model is the runs' model; empty = the CLI's default.
	Model string
	// WorkDir holds the clones, one per run, deleted after it (under the
	// user's cache, its own: Sweep empties it at start).
	WorkDir string
	// Environ and Home are where the login is looked for (FindLogin).
	Environ []string
	Home    string
	// ProgressEvery is how often a run's progress is sent; zero = 2 s.
	ProgressEvery time.Duration
	// OnLoginRefused is told when a run's credentials were refused: the
	// machine announces Claude Code no more (Client.Refresh).
	OnLoginRefused func()
	// RetryAfter is how long a refused login stays withdrawn when nothing
	// says it changed (a token in the environment, the macOS keychain);
	// zero = DefaultLoginRetry.
	RetryAfter time.Duration

	mu sync.Mutex
	// refused: a run was refused for its login, at refusedAt, whose file
	// then dated from refusedStamp. Claude Code is announced again once the
	// file changes, after RetryAfter, or at the next connection (Retry).
	refused      bool
	refusedAt    time.Time
	refusedStamp time.Time
	// The last Login, for a second: it is asked at every status check and
	// every directive.
	cached   claudecode.LoginStatus
	cachedAt time.Time
}

// DefaultLoginRetry is how long a refused login stays withdrawn when no
// login file tells that it changed.
const DefaultLoginRetry = 10 * time.Minute

// loginCacheFor is how long Login's answer is kept.
const loginCacheFor = time.Second

// Login is the state of the machine's CLI, found without a paid call:
// absent, logged out (no login found, or one a run saw refused and that did
// not change since), or ok.
func (a *Analyzer) Login() claudecode.LoginStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.cachedAt.IsZero() && time.Since(a.cachedAt) < loginCacheFor {
		return a.cached
	}
	a.cached, a.cachedAt = a.login(), time.Now()
	return a.cached
}

func (a *Analyzer) login() claudecode.LoginStatus {
	if !a.Runner.Available() {
		return claudecode.LoginAbsent
	}
	if _, ok := claudecode.FindLogin(a.Auth, a.Environ, a.Home); !ok {
		return claudecode.LoginNone
	}
	if a.refused {
		retry := a.RetryAfter
		if retry <= 0 {
			retry = DefaultLoginRetry
		}
		if !claudecode.LoginStamp(a.Environ, a.Home).After(a.refusedStamp) && time.Since(a.refusedAt) < retry {
			return claudecode.LoginNone
		}
		a.refused = false // a new login, or time to try again
	}
	return claudecode.LoginOK
}

// Retry lifts a login refusal: a new connection tries again.
func (a *Analyzer) Retry() {
	a.mu.Lock()
	a.refused, a.cachedAt = false, time.Time{}
	a.mu.Unlock()
}

func (a *Analyzer) loginRefused() {
	a.mu.Lock()
	a.refused, a.refusedAt, a.refusedStamp = true, time.Now(), claudecode.LoginStamp(a.Environ, a.Home)
	a.cachedAt = time.Time{}
	a.mu.Unlock()
	if a.OnLoginRefused != nil {
		a.OnLoginRefused()
	}
}

// AllowsRepo reports a repository the machine may clone.
func (a *Analyzer) AllowsRepo(repo string) bool {
	for _, g := range a.Repos {
		if ok, _ := path.Match(g, repo); ok {
			return true
		}
	}
	return false
}

// Sweep removes what runs left in WorkDir when agent connect died: one
// instance per machine (State.Lock), so none of it is in use.
func (a *Analyzer) Sweep() error {
	entries, err := os.ReadDir(a.WorkDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "run-") {
			if err := os.RemoveAll(filepath.Join(a.WorkDir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Run is the analyze_repo executor: clone, run the CLI read-only, report,
// delete the clone. The output goes along with an error too (a partial
// report, how far it got).
func (a *Analyzer) Run(ctx context.Context, input json.RawMessage, progress func(string)) (json.RawMessage, error) {
	var in machine.AnalyzeInput
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, fmt.Errorf("analyze_repo input: %w", err)
	}
	if err := in.Check(); err != nil {
		return nil, err
	}
	// Refused before anything runs: the workflow takes it elsewhere.
	if !a.AllowsRepo(in.Repo) {
		return nil, Refuse("repository %q is not one this machine may use (agent connect --repos)", in.Repo)
	}
	if a.Login() != claudecode.LoginOK {
		return nil, Refuse("Claude Code is not logged in on this machine")
	}
	if err := os.MkdirAll(a.WorkDir, 0o700); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(a.WorkDir, "run-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	clone := filepath.Join(dir, "repo")

	progress(machine.CloneProgress)
	commit, err := a.clone(ctx, in, clone)
	if err != nil {
		return nil, err
	}
	out := machine.CodingOutput{Commit: commit}

	runner := a.Runner
	runner.Auth = a.Auth
	// Nothing the CLI starts may wait on the owner's terminal.
	runner.NewSession = true
	runner.HeartbeatEvery = a.ProgressEvery
	if runner.HeartbeatEvery <= 0 {
		runner.HeartbeatEvery = 2 * time.Second
	}
	runner.OnBeat = func(_ context.Context, p claudecode.Progress) {
		progress(machine.CodingProgress(p.ToolCalls, p.LastTool))
	}
	res, err := runner.Run(ctx, claudecode.Params{
		Cwd:                clone,
		Task:               in.Task,
		Model:              a.Model,
		PermissionMode:     machine.AnalyzePermissionMode,
		AppendSystemPrompt: machine.AnalyzeSystemPrompt,
		MaxBudgetUSD:       a.MaxBudgetUSD,
		// The owner's settings and MCP servers, not the clone's: a branch
		// can carry .claude/settings.json (hooks run even in plan mode)
		// and .mcp.json.
		SettingSources:  []string{"user"},
		StrictMCPConfig: true,
		// The clone is deleted at the end: a transcript would outlive it.
		NoSessionPersistence: true,
	})
	if line := claudecode.AuthFailure(res, err); line != "" {
		log.Printf("connect: Claude Code's login was refused (%q): Claude Code withdrawn", line)
		a.loginRefused()
		if res.Progress.ToolCalls == 0 {
			// Nothing done yet: the workflow takes it elsewhere.
			return nil, Refuse("Claude Code's login was refused on this machine (expired or revoked)")
		}
		out.Error = "Claude Code's login was refused on this machine (expired or revoked): its owner must log in again (claude, then /login)"
		if err == nil {
			err = errors.New(out.Error)
		}
	}
	out.Report, out.IsError, out.Subtype = res.Report, res.IsError, res.Subtype
	out.NumTurns, out.DurationMS, out.CostUSD, out.ToolUses = res.NumTurns, res.DurationMS, res.CostUSD, res.ToolUses
	res.Auth = a.Auth
	if paid, perr := res.Payer(); perr == nil {
		out.PaidBy = string(paid)
	}
	if err != nil {
		out.Interrupted = res.Subtype == ""
		out.ToolCalls, out.LastTool, out.Events = res.Progress.ToolCalls, res.Progress.LastTool, res.Progress.Events
		if out.Error == "" {
			out.Error = machine.Cut(err.Error(), machine.MaxErrorBytes)
		}
	}
	raw, merr := json.Marshal(out)
	if merr != nil {
		return nil, merr
	}
	return raw, err
}

// gitEnvNames are what the clone keeps of the owner's environment beyond
// subproc.Env's base (HOME, so git and ssh find their configuration): its
// ssh agent, and how it told git to use ssh.
var gitEnvNames = []string{"SSH_AUTH_SOCK", "GIT_SSH_COMMAND", "XDG_CONFIG_HOME"}

// gitEnv is the clone's environment: the owner's git configuration (their
// credential helpers, their ssh setup), never a prompt (GIT_TERMINAL_PROMPT
// off, ssh in batch mode unless they set GIT_SSH_COMMAND themselves).
func gitEnv() []string {
	env := subproc.GitEnvUser(os.Environ(), gitEnvNames...)
	if os.Getenv("GIT_SSH_COMMAND") == "" {
		env = append(env, "GIT_SSH_COMMAND=ssh -o BatchMode=yes")
	}
	return env
}

// gitHint says, after a clone that failed, what the owner can do about it.
func gitHint(out string) string {
	switch lower := strings.ToLower(out); {
	case strings.Contains(lower, "terminal prompts disabled") || strings.Contains(lower, "could not read username"):
		return " (this repository asks for credentials: git found none without asking — set a credential helper in your git configuration, or give its ssh URL)"
	case strings.Contains(lower, "host key verification failed"):
		return " (the host's ssh key is not in your known_hosts: connect to it once by hand, e.g. ssh -T git@github.com)"
	case strings.Contains(lower, "permission denied (publickey)"):
		return " (your ssh key was refused or is not loaded in your ssh agent)"
	case strings.Contains(lower, "not allowed"):
		return " (only ssh, https and local paths are allowed)"
	}
	return ""
}

// clone clones in.Repo into dir with the owner's git identity, at in.Ref
// (detached), and returns the commit. As on the workers: "--" before the
// repository, no hooks, no filesystem monitor, no hard links to a local
// source; and only ssh, https and local paths (GitProtocolArgs). Git runs in
// a session of its own: nothing can prompt on the owner's terminal.
func (a *Analyzer) clone(ctx context.Context, in machine.AnalyzeInput, dir string) (string, error) {
	env := gitEnv()
	git := func(cwd string, args ...string) (string, error) {
		argv := append(append(append([]string(nil), subproc.GitSafeArgs...), subproc.GitProtocolArgs...), args...)
		cmd := exec.CommandContext(ctx, "git", argv...)
		cmd.Dir = cwd
		cmd.Env = env
		subproc.KillGroupOnCancel(cmd, syscall.SIGTERM, 5*time.Second)
		subproc.NewSession(cmd)
		out, err := cmd.CombinedOutput()
		subproc.KillGroup(cmd)
		return strings.TrimSpace(string(out)), err
	}
	if out, err := git("", "clone", "--quiet", "--no-hardlinks", "--", in.Repo, dir); err != nil {
		return "", fmt.Errorf("clone %s: %v: %s%s", in.Repo, err, machine.Cut(out, 2048), gitHint(out))
	}
	if in.Ref != "" {
		sha := ""
		for _, candidate := range []string{in.Ref, "origin/" + in.Ref} {
			if out, err := git(dir, "rev-parse", "--quiet", "--verify", "--end-of-options", candidate+"^{commit}"); err == nil {
				sha = out
				break
			}
		}
		if sha == "" {
			return "", fmt.Errorf("unknown ref %q", in.Ref)
		}
		if out, err := git(dir, "checkout", "--quiet", "--detach", sha); err != nil {
			return "", fmt.Errorf("checkout %s: %v: %s", in.Ref, err, out)
		}
	}
	commit, err := git(dir, "rev-parse", "HEAD")
	if err != nil {
		return "", fmt.Errorf("resolve HEAD: %v: %s", err, commit)
	}
	return commit, nil
}
