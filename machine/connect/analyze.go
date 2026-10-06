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

// Coder runs the coding directives, analyze_repo and implement_feature, with
// the machine's own claude CLI: its owner's login, subscription or key,
// under its owner's user, and their git identity. What keeps a run within
// bounds is set here, never by the server (design §8): the repositories it
// may clone, whether it may push, what a run may spend, what an analysis
// and an implementation may do (machine.AnalyzePermissionMode,
// ImplementPermissionMode and their prompts, the same as on the
// installation's workers).
type Coder struct {
	// Runner runs the CLI (Binary, StallTimeout); its Auth, OnBeat and
	// heartbeat are set per run.
	Runner claudecode.Runner
	// Auth is who pays (claudecode.ResolveAuth, read on this machine): the
	// other mode's credential never reaches the CLI.
	Auth claudecode.Auth
	// Repos are the repositories it may clone (globs, path.Match: * stops
	// at a /); empty = every one refused.
	Repos []string
	// AllowPush lets an implementation push its branch, with the owner's
	// git identity, to a repository of Repos (agent connect --allow-push).
	// Off: the machine announces no git-push, and refuses an
	// implementation.
	AllowPush bool
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
	// Upload publishes a file of the directive ctx runs (Client.Upload):
	// what a run leaves in its outputs. Nil: they are not published.
	Upload func(ctx context.Context, name string, content []byte) (machine.FileRef, error)
	// MaxFileBytes bounds a file of the outputs; zero = DefaultMaxFileBytes
	// (the server has its own bound, FILES_MAX_BYTES).
	MaxFileBytes int64

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
func (a *Coder) Login() claudecode.LoginStatus {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.cachedAt.IsZero() && time.Since(a.cachedAt) < loginCacheFor {
		return a.cached
	}
	a.cached, a.cachedAt = a.login(), time.Now()
	return a.cached
}

func (a *Coder) login() claudecode.LoginStatus {
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
func (a *Coder) Retry() {
	a.mu.Lock()
	a.refused, a.cachedAt = false, time.Time{}
	a.mu.Unlock()
}

func (a *Coder) loginRefused() {
	a.mu.Lock()
	a.refused, a.refusedAt, a.refusedStamp = true, time.Now(), claudecode.LoginStamp(a.Environ, a.Home)
	a.cachedAt = time.Time{}
	a.mu.Unlock()
	if a.OnLoginRefused != nil {
		a.OnLoginRefused()
	}
}

// AllowsRepo reports a repository the machine may clone.
func (a *Coder) AllowsRepo(repo string) bool {
	for _, g := range a.Repos {
		if ok, _ := path.Match(g, repo); ok {
			return true
		}
	}
	return false
}

// Sweep removes what runs left in WorkDir when agent connect died: one
// instance per machine (State.Lock), so none of it is in use.
func (a *Coder) Sweep() error {
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

// Analyze is the analyze_repo executor: clone, run the CLI read-only,
// publish what it left in its outputs, report, delete the clone. The output
// goes along with an error too (a partial report, how far it got).
func (a *Coder) Analyze(ctx context.Context, input json.RawMessage, progress func(string)) (json.RawMessage, error) {
	var in machine.AnalyzeInput
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, fmt.Errorf("analyze_repo input: %w", err)
	}
	if err := in.Check(); err != nil {
		return nil, err
	}
	// Refused before anything runs: the workflow takes it elsewhere.
	if err := a.refuse(in.Repo); err != nil {
		return nil, err
	}
	r, err := a.newRun()
	if err != nil {
		return nil, err
	}
	defer r.remove()

	progress(machine.CloneProgress)
	commit, err := a.clone(ctx, in.Repo, in.Ref, r.clone)
	if err != nil {
		return nil, err
	}
	out := machine.CodingOutput{Commit: commit}
	err = a.runCLI(ctx, claudecode.Params{
		Cwd:                r.clone,
		Task:               in.Task,
		PermissionMode:     machine.AnalyzePermissionMode,
		AppendSystemPrompt: machine.AnalyzeSystemPrompt + "\n" + machine.OutputsPrompt(r.outputs),
		// Plan mode allows no edit: but this one, in the outputs.
		AllowedTools: []string{machine.OutputsRule(r.outputs)},
		AddDirs:      []string{r.outputs},
	}, &out, progress)
	var refusal *Refusal
	if errors.As(err, &refusal) {
		return nil, err
	}
	out.Unpublished = a.publishOutputs(ctx, r.outputs)
	raw, merr := json.Marshal(out)
	if merr != nil {
		return nil, merr
	}
	return raw, err
}

// refuse turns a run down before anything runs (a Refusal: the workflow
// takes it elsewhere): a repository the owner does not allow, Claude Code
// not logged in.
func (a *Coder) refuse(repo string) error {
	if !a.AllowsRepo(repo) {
		return Refuse("repository %q is not one this machine may use (agent connect --repos)", repo)
	}
	if a.Login() != claudecode.LoginOK {
		return Refuse("Claude Code is not logged in on this machine")
	}
	return nil
}

// codingRun is one run's directory under WorkDir: the clone, and next to it
// the outputs the CLI may leave and the copy of the clone's git
// configuration, all deleted with it.
type codingRun struct {
	dir, clone, outputs, gitConfig string
}

func (a *Coder) newRun() (*codingRun, error) {
	if err := os.MkdirAll(a.WorkDir, 0o700); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(a.WorkDir, "run-")
	if err != nil {
		return nil, err
	}
	r := &codingRun{dir: dir, clone: filepath.Join(dir, "repo"), outputs: filepath.Join(dir, machine.OutputsDir),
		gitConfig: filepath.Join(dir, "gitconfig")}
	if err := os.Mkdir(r.outputs, 0o700); err != nil {
		os.RemoveAll(dir)
		return nil, err
	}
	return r, nil
}

func (r *codingRun) remove() { os.RemoveAll(r.dir) }

// runCLI runs the CLI with p (the machine's model, cap and session settings
// added) and fills out with what it says. A login refused before any tool
// ran is a Refusal: nothing was done, the workflow takes the run elsewhere.
func (a *Coder) runCLI(ctx context.Context, p claudecode.Params, out *machine.CodingOutput, progress func(string)) error {
	runner := a.Runner
	runner.Auth = a.Auth
	// Nothing the CLI starts may wait on the owner's terminal.
	runner.NewSession = true
	runner.HeartbeatEvery = a.ProgressEvery
	if runner.HeartbeatEvery <= 0 {
		runner.HeartbeatEvery = 2 * time.Second
	}
	runner.OnBeat = func(_ context.Context, pr claudecode.Progress) {
		progress(machine.CodingProgress(pr.ToolCalls, pr.LastTool))
	}
	p.Model = a.Model
	p.MaxBudgetUSD = lowerCap(a.MaxBudgetUSD, p.MaxBudgetUSD)
	// The owner's settings and MCP servers, not the clone's: a branch can
	// carry .claude/settings.json (hooks run even in plan mode) and
	// .mcp.json.
	p.SettingSources = []string{"user"}
	p.StrictMCPConfig = true
	// The clone is deleted at the end: a transcript would outlive it.
	p.NoSessionPersistence = true
	res, err := runner.Run(ctx, p)
	if line := claudecode.AuthFailure(res, err); line != "" {
		log.Printf("connect: Claude Code's login was refused (%q): Claude Code withdrawn", line)
		a.loginRefused()
		if res.Progress.ToolCalls == 0 {
			// Nothing done yet: the workflow takes it elsewhere.
			return Refuse("Claude Code's login was refused on this machine (expired or revoked)")
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
	return err
}

// lowerCap is the smaller of two budget caps, where zero means none: what a
// directive asks can only lower the machine's.
func lowerCap(a, b float64) float64 {
	if a <= 0 || (b > 0 && b < a) {
		return max(b, 0)
	}
	return a
}

// gitEnvNames are what the clone keeps of the owner's environment beyond
// subproc.Env's base (HOME, so git and ssh find their configuration): its
// ssh agent, and how it told git to use ssh.
var gitEnvNames = []string{"SSH_AUTH_SOCK", "GIT_SSH_COMMAND", "XDG_CONFIG_HOME"}

// gitEnv is the clone's environment: the owner's git configuration (their
// credential helpers, their core.sshCommand), never a prompt
// (GIT_TERMINAL_PROMPT off; ssh, with no terminal in its session, cannot
// ask either).
func gitEnv() []string {
	return subproc.GitEnvUser(os.Environ(), gitEnvNames...)
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

// git runs one git command for a run, as its owner (their git
// configuration and credentials, gitEnv), never letting the tree it works in
// run a program (GitSafeArgs), on the transports a run may use
// (GitProtocolArgs), in a session of its own: nothing can prompt on the
// owner's terminal, and what it leaves behind is ended with it.
func git(ctx context.Context, cwd string, args ...string) (string, error) {
	argv := append(append(append([]string(nil), subproc.GitSafeArgs...), subproc.GitProtocolArgs...), args...)
	cmd := exec.CommandContext(ctx, "git", argv...)
	cmd.Dir = cwd
	cmd.Env = gitEnv()
	subproc.KillGroupOnCancel(cmd, syscall.SIGTERM, 5*time.Second)
	subproc.NewSession(cmd)
	out, err := cmd.CombinedOutput()
	subproc.KillGroup(cmd)
	return strings.TrimSpace(string(out)), err
}

// clone clones repo into dir with the owner's git identity, at ref
// (detached), and returns the commit. As on the workers: "--" before the
// repository, no hooks, no filesystem monitor, no hard links to a local
// source; and only ssh, https and local paths (GitProtocolArgs).
func (a *Coder) clone(ctx context.Context, repo, ref, dir string) (string, error) {
	if out, err := git(ctx, "", "clone", "--quiet", "--no-hardlinks", "--", repo, dir); err != nil {
		return "", fmt.Errorf("clone %s: %v: %s%s", repo, err, machine.Cut(out, 2048), gitHint(out))
	}
	if ref != "" {
		sha := ""
		for _, candidate := range []string{ref, "origin/" + ref} {
			if out, err := git(ctx, dir, "rev-parse", "--quiet", "--verify", "--end-of-options", candidate+"^{commit}"); err == nil {
				sha = out
				break
			}
		}
		if sha == "" {
			return "", fmt.Errorf("unknown ref %q", ref)
		}
		if out, err := git(ctx, dir, "checkout", "--quiet", "--detach", sha); err != nil {
			return "", fmt.Errorf("checkout %s: %v: %s", ref, err, out)
		}
	}
	commit, err := git(ctx, dir, "rev-parse", "HEAD")
	if err != nil {
		return "", fmt.Errorf("resolve HEAD: %v: %s", err, commit)
	}
	return commit, nil
}
