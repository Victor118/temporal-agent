package activity

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/claudecode"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/machine/outputs"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/subproc"
	"github.com/victor/temporal-agent/tool"
)

// gitHeartbeat is how often a long git operation reports it is still alive.
// Without it a clone that hangs is only noticed at the StartToClose timeout,
// by which point the worker is still holding a live git process.
const gitHeartbeat = 10 * time.Second

// ClaudeCodeActivities runs coding sessions in throwaway workspaces. The
// workflow decides what happens around a run — what to clone, what the CLI is
// allowed to do, whether anything is kept — and these activities only carry
// the steps out.
type ClaudeCodeActivities struct {
	Runner *claudecode.Runner
	// Root holds one directory per run. Nothing outside it is ever deleted.
	Root string
	// SSHKeyPath is the identity used to push, configured on the worker that
	// mounts it. It is deliberately not a workflow input: the key a push uses
	// is a property of the machine that holds it, and a path in a workflow
	// input would be recorded in the execution history for good.
	SSHKeyPath string
	// RunAs is the user the CLI runs as, and who owns a workspace while it
	// does: not the worker's, whose credentials the run's shell would reach.
	// nil runs it as the worker's user, refused when that is root.
	RunAs *subproc.Identity
	// Runs counts the commands the worker runs as RunAs (subproc.Runs, the
	// worker's one, shared with exec): required with RunAs.
	Runs RunCounter
	// ClaudeConfigDir is the operator's CLI configuration (CLAUDE_CONFIG_DIR):
	// each run starts from a copy of its own, next to its workspace, and only
	// a renewed login comes back (claudecode.SeedConfigDir, KeepCredentials).
	// Empty: each run starts from an empty one with RunAs, and without, uses
	// the CLI's default one (claudecode.OwnConfig).
	ClaudeConfigDir string
	// AllowedRepos are the repositories this worker clones and pushes to, as
	// globs (path.Match: * stops at a slash). Empty refuses them all. The
	// repository is the model's choice, and a push goes out with this
	// worker's identity: what it may reach is the operator's decision.
	AllowedRepos []string
	// Model is the model of every run on this worker; empty = the CLI's
	// default. MaxBudgetUSD caps what one run spends; zero = no cap, and a
	// workflow may only lower it. Both the operator's, like AllowedRepos.
	Model        string
	MaxBudgetUSD float64
	// Auth is how runs authenticate: the API or a subscription
	// (claudecode.ResolveAuth, CLAUDE_CODE_AUTH). Zero: as the CLI finds.
	Auth claudecode.Auth
	// QueueWait is how long a run waits for a worker of this queue with a
	// run to spare (CLAUDE_CODE_QUEUE_WAIT); zero = DefaultRunQueueWait.
	// The workflow learns it from ProbeRunWorker.
	QueueWait time.Duration
	// Stopper ends the runs under way when the worker stops (RunStop.Stop);
	// nil = never.
	Stopper *RunStop
	// RunEndWait bounds the wait of a step after a run for the run to be
	// gone (awaitRunEnd); zero = DefaultRunEndWait.
	RunEndWait time.Duration
	// Publisher stores what a run leaves in its outputs (PublishOutputs);
	// nil: they are not published, and the run is told so.
	Publisher *tool.Publisher
	// Skills reads the skills a run takes along, this worker's own
	// (PrepareWorkspace); nil = none.
	Skills RunSkillReader

	live cliRuns
}

// DefaultRunQueueWait is how long a run waits for a worker with a run to
// spare when CLAUDE_CODE_QUEUE_WAIT is empty: about a short run's length.
const DefaultRunQueueWait = 30 * time.Minute

// ProbeRunWorkerOutput is what a worker of a coding queue says to a run
// about to ask it for a slot.
type ProbeRunWorkerOutput struct {
	// QueueWaitSeconds is how long the run may wait for a slot (QueueWait),
	// in whole seconds, rounded up: the history shows it as people read it,
	// where a time.Duration would be nanoseconds.
	QueueWaitSeconds int64 `json:"queue_wait_seconds"`
}

// QueueWait is how long the run may wait for a slot: never zero, which the
// SDK reads as no bound (DefaultRunQueueWait instead).
func (o ProbeRunWorkerOutput) QueueWait() time.Duration {
	if o.QueueWaitSeconds <= 0 {
		return DefaultRunQueueWait
	}
	return time.Duration(o.QueueWaitSeconds) * time.Second
}

// ErrNoClaudeCLI is the type of the probe's error on a worker of a coding
// queue without the CLI: the run fails at once, rather than in its first
// step on that worker.
const ErrNoClaudeCLI = "NoClaudeCLI"

// ProbeRunWorker answers a run before it asks for a slot: some worker polls
// its queue, and can run it. It runs on the tool's queue, which a worker
// polls whether or not it has a run to spare; a full one stops polling for
// sessions, so the session's wait alone cannot tell a busy queue from an
// empty one. It also tells how long the run may wait for a slot: this
// worker's setting, which the workflow has no other way to read, and which
// its history then keeps. A worker without the CLI on the queue is a
// misconfiguration (a coding queue's workers all have it): it says so, and
// the run is not retried.
func (a *ClaudeCodeActivities) ProbeRunWorker(ctx context.Context) (ProbeRunWorkerOutput, error) {
	runner := a.Runner
	if runner == nil {
		runner = &claudecode.Runner{}
	}
	if !runner.Available() {
		return ProbeRunWorkerOutput{}, temporal.NewNonRetryableApplicationError(
			"this worker of the coding runs' queue has no claude CLI: every worker of that queue must have it",
			ErrNoClaudeCLI, nil)
	}
	wait := a.QueueWait
	if wait <= 0 {
		wait = DefaultRunQueueWait
	}
	return ProbeRunWorkerOutput{QueueWaitSeconds: int64((wait + time.Second - 1) / time.Second)}, nil
}

// ErrWorkerStopping is the type of the error of a run its worker ended
// because it was stopping (RunStop): the workflow reads it as a lost worker.
const ErrWorkerStopping = "WorkerStopping"

// ErrRunStalled is the type of the error of a run whose CLI wrote nothing
// for too long (claudecode.StallError), and ErrRunFailed of any other run
// that ended without the CLI's result. Both carry, as their details, how far
// the run got (claudecode.Progress), as ErrWorkerStopping does.
const (
	ErrRunStalled = "RunStalled"
	ErrRunFailed  = "RunFailed"
)

// RunStop ends the coding runs under way when their worker stops: each
// kills its CLI and answers ErrWorkerStopping, while the worker still polls
// the session's queue and waits for the answer to go out (worker.Options
// WorkerStopTimeout). Waiting for the SDK to cancel them instead would come
// last: it stops the session's creation first, which waits that whole
// timeout for a session that does not end on its own.
//
// It is a type of its own, held by ClaudeCodeActivities, and not a method
// of it: every exported method of a struct given to RegisterActivity is an
// activity, and the SDK refuses (panics on) one without a result or error.
type RunStop struct {
	init, closed sync.Once
	stopping     chan struct{}
}

// Stop ends the runs under way; later calls do nothing.
func (s *RunStop) Stop() {
	ch := s.channel()
	s.closed.Do(func() { close(ch) })
}

// channel is closed once Stop is called; nil (never closed) for a nil s.
func (s *RunStop) channel() chan struct{} {
	if s == nil {
		return nil
	}
	s.init.Do(func() { s.stopping = make(chan struct{}) })
	return s.stopping
}

// endOnStop is ctx, cancelled once the worker stops (Stopper), and whether
// it was.
func (a *ClaudeCodeActivities) endOnStop(ctx context.Context) (context.Context, func() bool, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	var stopped atomic.Bool
	stopping := a.Stopper.channel()
	go func() {
		select {
		case <-stopping:
			stopped.Store(true)
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, stopped.Load, cancel
}

// RunCounter is what the coding activities need of subproc.Runs: a run
// counted while the CLI works, and the processes a run left behind ended
// before a clone is taken back, when no run is under way.
type RunCounter interface {
	Hold() (release func())
	KillStrays()
}

// DefaultRunEndWait bounds how long a step after a run waits for the run's
// CLI to be gone (awaitRunEnd), when ClaudeCodeActivities.RunEndWait is zero.
// A run the workflow gave up on (its heartbeat timed out) still runs on its
// worker until the worker learns it, in the answer to its next heartbeat
// (one a minute at most, the SDK's throttle), then ends the CLI within the
// runner's grace (10s); the rest is margin.
const DefaultRunEndWait = 150 * time.Second

// ErrRunStillActive is the type of the error of a step that found the run's
// CLI still running on its worker past the wait (awaitRunEnd): the clone is
// not the worker's to read, push or delete.
const ErrRunStillActive = "RunStillActive"

// cliRuns counts the CLIs running, per workspace: RunClaudeCode holds its
// workspace's while the runner runs, and the steps after it wait for none
// (awaitRunEnd). A count of its own, not subproc.Runs: that one counts every
// command run as RunAs, exec's included, which have nothing to do with a
// clone. The zero value is ready.
type cliRuns struct {
	mu sync.Mutex
	n  map[string]int
	// changed is closed, and dropped, at every release: a waiter then reads
	// the count again, under mu.
	changed chan struct{}
}

// hold counts a CLI running in dir until release is called.
func (c *cliRuns) hold(dir string) (release func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[dir]++
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.n[dir]--; c.n[dir] == 0 {
				delete(c.n, dir)
			}
			if c.changed != nil {
				close(c.changed)
				c.changed = nil
			}
		})
	}
}

// idle waits until no CLI runs in dir, or until ctx is done, and tells
// which: true when none runs.
func (c *cliRuns) idle(ctx context.Context, dir string) bool {
	for {
		c.mu.Lock()
		if c.n[dir] == 0 {
			c.mu.Unlock()
			return true
		}
		if c.changed == nil {
			c.changed = make(chan struct{})
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

// awaitRunEnd waits, heartbeating, until the CLI of the run in dir is gone
// from this worker (cliRuns): the CLI of a run the workflow gave up on, which
// its worker has yet to end, must not be writing the clone as it is
// inspected, pushed or deleted. A run that ended normally is gone already: no
// wait. Past RunEndWait, an ErrRunStillActive, never retried. The step
// cancelled meanwhile: a cancellation, which the workflow reads as such.
func (a *ClaudeCodeActivities) awaitRunEnd(ctx context.Context, dir string) error {
	limit := a.RunEndWait
	if limit <= 0 {
		limit = DefaultRunEndWait
	}
	wait, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	defer heartbeatWhile(ctx, "waiting for the run to end")()
	if a.live.idle(wait, dir) {
		return nil
	}
	if err := ctx.Err(); errors.Is(err, context.Canceled) {
		return temporal.NewCanceledError()
	} else if err != nil {
		return err
	}
	return temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("the run's CLI still runs on this worker after %s", limit), ErrRunStillActive, nil)
}

// stepError is err, prefixed with what failed, unless the SDK reads it by its
// kind: an application error (wrapped, it would reach the workflow as a
// retryable error of no type, its details lost) or a cancellation, which go
// as they are.
func stepError(what string, err error) error {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		return appErr
	}
	var canceled *temporal.CanceledError
	if errors.As(err, &canceled) {
		return canceled
	}
	return fmt.Errorf("%s: %w", what, err)
}

// hasType tells whether err is an application error of type typ.
func hasType(err error, typ string) bool {
	var appErr *temporal.ApplicationError
	return errors.As(err, &appErr) && appErr.Type() == typ
}

// heartbeatWhile heartbeats every gitHeartbeat, inside an activity, until
// stop is called.
func heartbeatWhile(ctx context.Context, details any) (stop func()) {
	if !activity.IsActivity(ctx) {
		return func() {}
	}
	done := make(chan struct{})
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		ticker := time.NewTicker(gitHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, details)
			}
		}
	}()
	return func() {
		close(done)
		<-ended
	}
}

type PrepareWorkspaceInput struct {
	// Name is the workspace directory, one path element, chosen by the
	// workflow so a retry lands on the same place.
	Name string `json:"name"`
	Repo string `json:"repo"`
	Ref  string `json:"ref,omitempty"` // branch, tag or commit; empty = default branch
	// Branch, when set, is created at Ref and checked out. A run meant to
	// produce commits starts on the branch that will carry them, so nothing
	// can land on the base by accident.
	Branch string `json:"branch,omitempty"`
	// Skills names the calling agent's skills for the run
	// (tool.CallContext.RunSkills), read from this worker's own skills and
	// written as the run's plugin (pluginDir).
	Skills []string `json:"skills,omitempty"`
}

type PrepareWorkspaceOutput struct {
	Dir string `json:"dir"`
	// Commit is where the workspace started: the base a later inspection
	// measures the run's commits against.
	Commit string `json:"commit"`
	Branch string `json:"branch,omitempty"`
	// Skills are those written in the run's plugin, for RunClaudeCode;
	// SkillsMissing, those named that were not found ("name: reason");
	// SkillsVersion, what they were loaded from.
	Skills        []string `json:"skills,omitempty"`
	SkillsMissing []string `json:"skills_missing,omitempty"`
	SkillsVersion string   `json:"skills_version,omitempty"`
}

// PrepareWorkspace clones repo into a fresh directory under Root. It clones
// with whatever credentials the worker has and no more: nothing here arranges
// write access, because a read-only run has no use for it. A run that will
// commit (Branch) has its push tried on the fresh clone (checkPush): one that
// could not be published does not start.
func (a *ClaudeCodeActivities) PrepareWorkspace(ctx context.Context, in PrepareWorkspaceInput) (PrepareWorkspaceOutput, error) {
	dir, err := a.workspacePath(in.Name)
	if err != nil {
		return PrepareWorkspaceOutput{}, err
	}
	if err := a.checkRepo(in.Repo); err != nil {
		return PrepareWorkspaceOutput{}, stepError("prepare workspace", err)
	}
	if strings.HasPrefix(in.Ref, "-") || strings.HasPrefix(in.Branch, "-") {
		return PrepareWorkspaceOutput{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("prepare workspace: invalid ref %q or branch %q", in.Ref, in.Branch), "InvalidInput", nil)
	}
	// A retried attempt finds the previous one's half-written clone. Start over
	// rather than trying to repair it.
	if err := removeWorkspace(dir); err != nil {
		return PrepareWorkspaceOutput{}, stepError("prepare workspace", err)
	}
	// The run's skills, read and checked before anything is cloned: skills
	// it cannot take end the step at once, leaving nothing behind.
	skills, err := a.runSkills(in.Skills)
	if err != nil {
		return PrepareWorkspaceOutput{}, err
	}
	if err := os.MkdirAll(a.Root, 0o755); err != nil {
		return PrepareWorkspaceOutput{}, stepError("prepare workspace", err)
	}

	// The clone is the only step that reaches the network, and a private
	// repository needs the worker's identity to answer at all. A worker with
	// no identity clones what is public or local, and nothing else.
	//
	// --no-hardlinks matters only when Repo is a path on this filesystem: git
	// would otherwise link the clone's objects to the source's rather than
	// copy them, and the run has a shell. Git never rewrites an object in
	// place, but `echo x > .git/objects/ab/cdef…` does, and through a hard
	// link that lands in the source repository. Ignored for a remote URL,
	// where there is nothing to link.
	//
	// "--" ends the options: a repo starting with a dash is refused above, and
	// this keeps it so.
	if out, err := a.gitEnv(ctx, "", a.sshEnv(), "clone", "--quiet", "--no-hardlinks", "--", in.Repo, dir); err != nil {
		return PrepareWorkspaceOutput{}, fmt.Errorf("clone %s: %w: %s", in.Repo, err, out)
	}
	if in.Ref != "" {
		sha, err := a.resolveRef(ctx, dir, in.Ref)
		if err != nil {
			return PrepareWorkspaceOutput{}, err
		}
		// Detached: an analysis run reads a state, it does not continue a branch.
		if out, err := a.git(ctx, dir, "checkout", "--quiet", "--detach", sha); err != nil {
			return PrepareWorkspaceOutput{}, fmt.Errorf("checkout %s: %w: %s", in.Ref, err, out)
		}
	}

	commit, err := a.git(ctx, dir, "rev-parse", "HEAD")
	if err != nil {
		return PrepareWorkspaceOutput{}, fmt.Errorf("resolve HEAD: %w: %s", err, commit)
	}
	if in.Branch != "" {
		if out, err := a.git(ctx, dir, "checkout", "--quiet", "-b", in.Branch); err != nil {
			return PrepareWorkspaceOutput{}, fmt.Errorf("create branch %s: %w: %s", in.Branch, err, out)
		}
		if err := a.checkPush(ctx, dir, in.Repo, strings.TrimSpace(commit), in.Branch); err != nil {
			// No run will have this clone: the workflow cleans up after a
			// workspace it got, not this one.
			if rerr := removeWorkspace(dir); rerr != nil {
				log.Printf("Warning: claude code: %s not deleted after its push check: %v", dir, rerr)
			}
			return PrepareWorkspaceOutput{}, err
		}
	}
	if err := keepGitConfig(dir); err != nil {
		return PrepareWorkspaceOutput{}, stepError("prepare workspace", err)
	}
	if err := os.Mkdir(outputsDir(dir), 0o700); err != nil {
		return PrepareWorkspaceOutput{}, stepError("prepare workspace", err)
	}
	// The run works, and commits, as RunAs: the clone and its outputs are
	// its own. Root and the configuration's copy stay the worker's.
	if err := a.RunAs.Give(dir); err != nil {
		return PrepareWorkspaceOutput{}, stepError("prepare workspace", err)
	}
	if err := a.RunAs.Give(outputsDir(dir)); err != nil {
		return PrepareWorkspaceOutput{}, stepError("prepare workspace", err)
	}
	out := PrepareWorkspaceOutput{Dir: dir, Commit: strings.TrimSpace(commit), Branch: in.Branch}
	if err := writePlugin(dir, skills, &out); err != nil {
		return PrepareWorkspaceOutput{}, err
	}
	return out, nil
}

// ErrPushCheckFailed is the type of PrepareWorkspace's error when the branch
// of a run that will commit could not be pushed (checkPush), said as it is:
// for good when the remote refused the worker's identity, retried with the
// preparation otherwise (the network, a remote that did not answer).
const ErrPushCheckFailed = "PushCheckFailed"

// checkPush tries the push of a run's branch before the run: git push
// --dry-run of the base to it, as PushBranch pushes (this worker's identity,
// its options), within machine.PushCheckTimeout. It reaches the remote and
// authenticates as a push does; it runs no hook of the server's and checks
// no branch protection. A failure is an ErrPushCheckFailed: the run's
// commits would be published nowhere, so it does not start, and nothing is
// paid; never retried when git's words are a refusal (pushRefused).
func (a *ClaudeCodeActivities) checkPush(ctx context.Context, dir, repo, base, branch string) error {
	tryCtx, cancel := context.WithTimeout(ctx, machine.PushCheckTimeout)
	defer cancel()
	out, err := a.gitEnv(tryCtx, dir, a.sshEnv(), "push", "--dry-run", "--", repo, base+":refs/heads/"+branch)
	switch {
	case err == nil:
		return nil
	case ctx.Err() != nil:
		return stepError("prepare workspace", ctx.Err())
	case tryCtx.Err() != nil:
		return temporal.NewApplicationError(fmt.Sprintf(
			"could not check that this worker may push to %s: git push --dry-run did not answer within %s; nothing was run",
			repo, machine.PushCheckTimeout), ErrPushCheckFailed)
	}
	out = strings.TrimSpace(out)
	hint := ""
	if a.SSHKeyPath == "" && isSSHRemote(repo) {
		hint = " (this worker has no git identity of its own: its operator sets one with CLAUDE_CODE_SSH_KEY)"
	}
	msg := fmt.Sprintf("this worker may not push to %s (git push --dry-run, before the run): %v: %s%s; nothing was run",
		repo, err, machine.Cut(out, 1024), hint)
	if pushRefused(out) {
		return temporal.NewNonRetryableApplicationError(msg, ErrPushCheckFailed, nil)
	}
	return temporal.NewApplicationError(msg, ErrPushCheckFailed)
}

// pushRefusals are what git, ssh or a forge say when the remote turns the
// identity down: the same push would be refused again. Anything else (a
// connection reset, a name that did not resolve, a remote that broke
// mid-answer) may pass at the next try.
var pushRefusals = []string{
	"permission denied", "authentication failed", "could not read username", "could not read password",
	"terminal prompts disabled", "returned error: 401", "returned error: 403", "permission to ",
	"host key verification failed", "not allowed to push", "repository not found",
	// A deploy key without write access (GitHub), Bitbucket's, Azure
	// DevOps' (TF401019: no such repository, or no right to it).
	"read only", "read-only", "access denied", "not have access", "tf401019",
}

// pushRefused tells a push's refusal, from git's words, from a failure
// worth trying again.
func pushRefused(out string) bool {
	lower := strings.ToLower(out)
	for _, r := range pushRefusals {
		if strings.Contains(lower, r) {
			return true
		}
	}
	return false
}

// isSSHRemote tells a repository git reaches over ssh: ssh://… or the scp
// form, host:path (a colon before any slash), as git reads it.
func isSSHRemote(repo string) bool {
	if strings.HasPrefix(repo, "ssh://") || strings.HasPrefix(repo, "git+ssh://") || strings.HasPrefix(repo, "ssh+git://") {
		return true
	}
	if strings.Contains(repo, "://") {
		return false
	}
	i := strings.Index(repo, ":")
	return i > 0 && !strings.Contains(repo[:i], "/")
}

// runSkills reads the skills named for a run from this worker's own. Skills
// a run cannot take (past its bounds, a name no plugin can carry) are an
// error for good: the same skills would fail again.
func (a *ClaudeCodeActivities) runSkills(names []string) (RunSkillSet, error) {
	var set RunSkillSet
	if len(names) == 0 {
		return set, nil
	}
	if a.Skills != nil {
		set = a.Skills.RunSkills(names)
	} else {
		set.Missing = names
	}
	if err := machine.CheckRunSkills(set.Skills); err != nil {
		return set, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("prepare workspace: the agent's skills cannot go with the run: %v", err), "InvalidInput", nil)
	}
	return set, nil
}

// writePlugin writes set as the plugin of the run in dir (machine.WritePlugin,
// pluginDir), and says in out what it wrote and what was not found. The
// plugin stays the worker's: the run's user reads it, and cannot change it.
func writePlugin(dir string, set RunSkillSet, out *PrepareWorkspaceOutput) error {
	for _, name := range set.Missing {
		out.SkillsMissing = append(out.SkillsMissing, name+": "+machine.SkillNotFound)
	}
	if len(set.Skills) == 0 {
		return nil
	}
	written, err := machine.WritePlugin(pluginDir(dir), set.Skills)
	if err != nil {
		return stepError("prepare workspace: the run's skills", err)
	}
	out.Skills, out.SkillsVersion = written, set.Version
	return nil
}

// gitConfigCopy is where the clone's .git/config is kept while the run works:
// next to the workspace, not in it. The run's edits are only accepted in its
// own directory, and nothing it runs may write in Root.
func gitConfigCopy(dir string) string { return dir + ".gitconfig" }

// keepGitConfig keeps a copy of the clone's git configuration, which the
// worker's own git commands use after the run, in place of whatever the run
// left in .git/config. It also drops the sample hooks: nothing in the
// workspace's .git is the worker's business once the run has had it.
func keepGitConfig(dir string) error {
	config, err := os.ReadFile(filepath.Join(dir, ".git", "config"))
	if err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(dir, ".git", "hooks")); err != nil {
		return err
	}
	return os.WriteFile(gitConfigCopy(dir), config, 0o644)
}

// restoreGitConfig puts the clone's configuration back in .git/config before
// the worker runs git in a tree the run had write access to, and reports
// whether the run had changed it.
//
// The run edits files, and .git/config is one: git reads it on every
// command, and core.fsmonitor, diff.external, credential.helper or
// gpg.program in it are commands the worker would run with its own
// credentials, while url.<x>.pushInsteadOf would send the push to another
// repository than the one checkRepo allowed. A linked-worktree layout
// (commondir, config.worktree) would make git read its configuration from
// elsewhere: it is removed as well. A .git that is no longer a directory is
// refused outright.
func (a *ClaudeCodeActivities) restoreGitConfig(dir string) (changed bool, err error) {
	if err := a.reclaim(dir); err != nil {
		return false, err
	}
	return restoreGitConfig(dir)
}

// reclaim takes the workspace and all of its .git back from RunAs before the
// worker runs git there. A process the run left behind still runs as RunAs:
// once they are the worker's, it can no longer swap .git, nor change what is
// in it.
//
// Such a process is ended first, unless another command of this worker runs
// as RunAs (Runs.KillStrays): a concurrent run's processes are not this one's
// to end, and the ownership taken back below does not rely on it.
func (a *ClaudeCodeActivities) reclaim(dir string) error {
	if a.RunAs == nil {
		return nil
	}
	if a.Runs != nil {
		a.Runs.KillStrays()
	}
	if err := subproc.Reclaim(dir); err != nil {
		return err
	}
	gitDir := filepath.Join(dir, ".git")
	if err := subproc.Reclaim(gitDir); err != nil {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s is no longer the clone's git directory", gitDir), "WorkspaceTampered", err)
	}
	// All of .git, not only its names: the run's user could otherwise still
	// rewrite a file in it — refs, objects, info/ — between the moment the
	// worker checks it and the moment its git reads it. The working tree
	// stays the run's: git only reads it.
	//
	// A clone made with --no-hardlinks has no file in .git that another path
	// shares: one is a ref or an object the run linked to elsewhere — its
	// working tree, say — to go on rewriting it once .git is the worker's.
	if err := subproc.ReclaimTree(gitDir); err != nil {
		if errors.Is(err, subproc.ErrLinkedFile) {
			return temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("the run left a file in %s that another path shares", gitDir), "WorkspaceTampered", err)
		}
		return err
	}
	return nil
}

func restoreGitConfig(dir string) (changed bool, err error) {
	gitDir := filepath.Join(dir, ".git")
	if fi, err := os.Lstat(gitDir); err != nil || !fi.IsDir() {
		return false, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%s is no longer the clone's git directory", gitDir), "WorkspaceTampered", err)
	}
	saved, err := os.ReadFile(gitConfigCopy(dir))
	if err != nil {
		return false, fmt.Errorf("the clone's git configuration was not kept: %w", err)
	}

	for _, name := range []string{"commondir", "config.worktree"} {
		path := filepath.Join(gitDir, name)
		if _, err := os.Lstat(path); err == nil {
			changed = true
			if err := os.RemoveAll(path); err != nil {
				return changed, err
			}
		}
	}

	path := filepath.Join(gitDir, "config")
	fi, err := os.Lstat(path)
	switch {
	case err != nil:
		changed = true
	case !fi.Mode().IsRegular():
		changed = true
	default:
		current, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(current, saved) {
			changed = true
		}
	}
	// Rewritten even when unchanged: the file the run's user had, and may
	// still hold open for writing, is replaced by a new one of the worker's.
	// A new file, never written through whatever the run left at that path:
	// O_EXCL does not follow a symbolic link. Whatever that is goes, a
	// directory with its contents included: RemoveAll does not follow links
	// either, and with RunAs, all of .git is the worker's by now (reclaim).
	if err := os.RemoveAll(path); err != nil {
		return changed, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return changed, err
	}
	if _, err := f.Write(saved); err != nil {
		f.Close()
		return changed, err
	}
	return changed, f.Close()
}

// cliConfigDir is the CLI's configuration for the run in dir: next to the
// workspace, in Root, and thrown away with it.
func cliConfigDir(dir string) string { return dir + ".claude" }

// pluginDir is the run's plugin, its skills (machine.WritePlugin): next to
// the workspace, in Root, the worker's, and deleted with it.
func pluginDir(dir string) string { return dir + ".plugin" }

// outputsDir is where the run in dir may leave files for the user
// (machine.OutputsPrompt): next to the workspace, in Root, the run's own,
// published after it (PublishOutputs) and deleted with it.
func outputsDir(dir string) string { return dir + ".outputs" }

// bundlePath is where the bundle of a run's commits that could not be pushed
// is written (BundleBranch): next to the workspace, in Root, the worker's,
// out of the run's reach and of its outputs, and deleted with it.
func bundlePath(dir string) string { return dir + ".bundle" }

// removeWorkspace deletes a run's directory, the copy of its git
// configuration, its CLI configuration, its outputs, its plugin, its bundle
// and its git (subproc.GitShimDir).
func removeWorkspace(dir string) error {
	for _, path := range []string{dir, cliConfigDir(dir), outputsDir(dir), pluginDir(dir), subproc.GitShimDir(dir)} {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	for _, path := range []string{gitConfigCopy(dir), bundlePath(dir)} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// checkRepo refuses a repository this worker must not clone or push to: one
// git would read as an option (--upload-pack=…, --config=…), and anything
// AllowedRepos does not name.
func (a *ClaudeCodeActivities) checkRepo(repo string) error {
	switch {
	case repo == "":
		return temporal.NewNonRetryableApplicationError("repo is required", "InvalidInput", nil)
	case strings.HasPrefix(repo, "-") || strings.ContainsAny(repo, "\x00\n\r"):
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("invalid repository %q", repo), "InvalidInput", nil)
	case !tool.MatchAny(a.AllowedRepos, repo):
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("repository %q is not one this worker may use (CLAUDE_CODE_REPOS)", repo), "RepoNotAllowed", nil)
	}
	return nil
}

// resolveRef turns a branch, tag or commit into a commit of the clone. A
// branch other than the default one only exists as origin/<name> in a fresh
// clone. --end-of-options keeps a ref from being read as an option.
func (a *ClaudeCodeActivities) resolveRef(ctx context.Context, dir, ref string) (string, error) {
	for _, candidate := range []string{ref, "origin/" + ref} {
		out, err := a.git(ctx, dir, "rev-parse", "--quiet", "--verify", "--end-of-options", candidate+"^{commit}")
		if err == nil {
			return strings.TrimSpace(out), nil
		}
	}
	return "", temporal.NewNonRetryableApplicationError(fmt.Sprintf("unknown ref %q", ref), "UnknownRef", nil)
}

type CleanupWorkspaceInput struct {
	Dir string `json:"dir"`
}

// CleanupWorkspace deletes a run's workspace. It refuses any path that is not
// a directory directly under Root: this runs with a disconnected context on
// every path out of the workflow, including the failures, so a wrong Dir would
// be deleted precisely when nobody is watching.
func (a *ClaudeCodeActivities) CleanupWorkspace(ctx context.Context, in CleanupWorkspaceInput) error {
	if in.Dir == "" {
		return nil
	}
	want, err := a.workspacePath(filepath.Base(in.Dir))
	if err != nil {
		return err
	}
	if filepath.Clean(in.Dir) != want {
		return fmt.Errorf("cleanup: refusing to delete %q, which is not a workspace under %q", in.Dir, a.Root)
	}
	if err := a.awaitRunEnd(ctx, want); err != nil {
		if hasType(err, ErrRunStillActive) {
			log.Printf("Warning: claude code: %s is not deleted, a command of its run still runs: "+
				"it goes at this worker's next start (RootClaim.Sweep)", want)
		}
		return stepError("cleanup", err)
	}
	return removeWorkspace(want)
}

// workspaceDir checks that dir is a run's directory under Root, as the
// workflow got it from PrepareWorkspace.
func (a *ClaudeCodeActivities) workspaceDir(dir string) (string, error) {
	want, err := a.workspacePath(filepath.Base(dir))
	if err != nil {
		return "", err
	}
	if filepath.Clean(dir) != want {
		return "", temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("%q is not a workspace under %q", dir, a.Root), "InvalidInput", nil)
	}
	return want, nil
}

type RunClaudeCodeInput struct {
	Dir  string `json:"dir"`
	Task string `json:"task"`

	// The rest is set by the workflow, never by the calling agent: it is what
	// keeps a read-only run read-only. The model and the budget cap are the
	// worker's (ClaudeCodeActivities); MaxBudgetUSD can only lower the cap.
	PermissionMode     string   `json:"permission_mode,omitempty"`
	AllowedTools       []string `json:"allowed_tools,omitempty"`
	DisallowedTools    []string `json:"disallowed_tools,omitempty"`
	AppendSystemPrompt string   `json:"append_system_prompt,omitempty"`
	MaxBudgetUSD       float64  `json:"max_budget_usd,omitempty"`
	SessionID          string   `json:"session_id,omitempty"`
	// Outputs lets the run leave files for the user in its outputs
	// (outputsDir, a working directory of the run's), and tells it where
	// (machine.OutputsPrompt): the directory is the worker's to name. Not
	// for an analysis: plan mode refuses every write.
	Outputs bool `json:"outputs,omitempty"`
	// Skills are those PrepareWorkspace wrote in the run's plugin
	// (pluginDir): the CLI loads it, and its system prompt names them
	// (machine.SkillsPrompt).
	Skills []string `json:"skills,omitempty"`
}

// RunClaudeCode runs one coding session and heartbeats while it does. A run the
// CLI reports as failed comes back as a Result, not an error: the workflow
// decides what a failed run means, and the report explains it better than an
// activity failure would. A run that ends without the CLI's result is an
// error (ErrRunStalled, ErrRunFailed, ErrWorkerStopping) whose details say
// how far it got: no result reaches the workflow along with an error.
func (a *ClaudeCodeActivities) RunClaudeCode(ctx context.Context, in RunClaudeCodeInput) (claudecode.Result, error) {
	if err := subproc.CheckRunAs(a.RunAs); err != nil {
		return claudecode.Result{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("claude code: %v", err), "RunAsRoot", nil)
	}
	dir, err := a.workspaceDir(in.Dir)
	if err != nil {
		return claudecode.Result{}, stepError("claude code", err)
	}
	// A configuration of the run's own, made afresh on every attempt: what an
	// earlier run, or attempt, wrote in one is never read by another. Without
	// an operator's configuration nor RunAs (a worker on a development
	// machine), the CLI keeps the worker's user's own (claudecode.OwnConfig).
	var configDir string
	if claudecode.OwnConfig(a.ClaudeConfigDir, a.RunAs) {
		configDir = cliConfigDir(dir)
		if err := claudecode.SeedConfigDir(configDir, a.ClaudeConfigDir, a.RunAs); err != nil {
			return claudecode.Result{}, stepError("claude code: configuration", err)
		}
		defer func() {
			// The CLI's environment is the worker's, filtered: whether it has
			// an API key is the worker's.
			if err := claudecode.KeepCredentials(configDir, a.ClaudeConfigDir, a.Auth.Filter(os.Environ())); err != nil {
				log.Printf("Warning: claude code: the login the run renewed was not kept: %v", err)
			}
		}()
	}

	runner := claudecode.Runner{}
	if a.Runner != nil {
		runner = *a.Runner
	}
	runner.RunAs = a.RunAs
	runner.Auth = a.Auth
	runner.OnBeat = func(ctx context.Context, p claudecode.Progress) { activity.RecordHeartbeat(ctx, p) }
	if a.Runs != nil {
		runner.Runs = a.Runs
	}
	prompt := in.AppendSystemPrompt
	var addDirs []string
	if in.Outputs {
		// A working directory of the run's: acceptEdits writes there.
		out := outputsDir(dir)
		prompt = strings.TrimSpace(prompt + "\n" + machine.OutputsPrompt(out))
		addDirs = []string{out}
	}
	var plugins []string
	if len(in.Skills) > 0 {
		plugins = []string{pluginDir(dir)}
		prompt = strings.TrimSpace(prompt + "\n" + machine.SkillsPrompt(in.Skills))
	}
	// The run's git: the worker's, in Root, not the run's to change. No
	// workspace: the runner says so.
	env := machine.RunGitEnv()
	if _, err := os.Stat(dir); err == nil {
		shim, err := subproc.WriteGitShim(dir)
		if err != nil {
			return claudecode.Result{}, stepError("claude code: the run's git", err)
		}
		env = append(env, subproc.WithPath(os.Environ(), shim))
	}
	// Until the CLI is gone, the steps after the run wait (awaitRunEnd).
	defer a.live.hold(dir)()
	ctx, stopped, cancel := a.endOnStop(ctx)
	defer cancel()
	res, err := runner.Run(ctx, claudecode.Params{
		ConfigDir:          configDir,
		Cwd:                in.Dir,
		Task:               in.Task,
		Model:              a.Model,
		PermissionMode:     in.PermissionMode,
		AllowedTools:       in.AllowedTools,
		DisallowedTools:    in.DisallowedTools,
		AppendSystemPrompt: prompt,
		AddDirs:            addDirs,
		PluginDirs:         plugins,
		MaxBudgetUSD:       lowerCap(a.MaxBudgetUSD, in.MaxBudgetUSD),
		SessionID:          in.SessionID,
		// The workspace is deleted at the end of the run, so a transcript on
		// disk would only outlive the tree it talks about.
		NoSessionPersistence: true,
		// The clone's own settings (hooks, even in plan mode) and MCP
		// servers (.mcp.json) are the repository's, not the operator's.
		SettingSources:  []string{"user"},
		StrictMCPConfig: true,
		// Whatever the clone's configuration says, the git the CLI starts
		// runs no program of its (hooks, fsmonitor, editor, signing), and
		// reads no file outside the clone for a commit (the shim first in
		// its PATH).
		Env: env,
	})
	// Stopped is WorkerStopping whatever the CLI returned, a run that ended
	// at the very instant of Stop included: its result is dropped. On
	// purpose: a CLI killed by the stop may exit cleanly with a partial
	// result, which "stopped() && err != nil" would take for a whole run.
	if stopped() {
		return res, temporal.NewNonRetryableApplicationError(
			"claude code: the worker stopped during the run", ErrWorkerStopping, err, res.Progress)
	}
	if err != nil {
		typ, msg := ErrRunFailed, "claude code: the run ended without the CLI's result"
		var stall *claudecode.StallError
		if errors.As(err, &stall) {
			typ, msg = ErrRunStalled, "claude code: the run was ended as stuck"
		}
		// The operator's managed settings forbid the run's plugin: said as
		// such, the CLI's line with it.
		if line := claudecode.PluginRefusal(res, err); line != "" && len(plugins) > 0 && res.Progress.ToolCalls == 0 {
			err = errors.New("the CLI refused the run's skills: this worker's managed settings forbid --plugin-dir: " + line)
		}
		return res, temporal.NewNonRetryableApplicationError(msg, typ, err, res.Progress)
	}
	// What paid the run is the worker's choice, checked against the CLI's
	// word: the summary the agent reads names a payer only when both agree.
	res.Auth = a.Auth
	paid, perr := res.Payer()
	if perr != nil {
		log.Printf("Warning: claude code: %v", perr)
	}
	res.PaidBy = paid
	return res, nil
}

type PublishOutputsInput struct {
	Dir string `json:"dir"`
	// Call is the run's call: the session turn and tool call its files go
	// to, the agent that made it (the last of its chain), the user.
	Call tool.CallContext `json:"call"`
	// Branch is the run's: the name of its bundle (machine.BundleName) is
	// not the outputs' to take.
	Branch string `json:"branch,omitempty"`
}

type PublishOutputsOutput struct {
	Files []tool.FileRef `json:"files,omitempty"`
	// Unpublished are the outputs not published, and why ("path: reason").
	Unpublished []string `json:"unpublished,omitempty"`
}

// PublishOutputs publishes what the run in Dir left in its outputs, as a
// machine does (outputs.Publish: regular files of their own, never through
// a link, within bounds), attached to the call's session turn through the
// worker's file store; once the run's CLI is gone (awaitRunEnd). Retried,
// it stores nothing twice (the same name and content is the file stored
// first).
func (a *ClaudeCodeActivities) PublishOutputs(ctx context.Context, in PublishOutputsInput) (PublishOutputsOutput, error) {
	dir, err := a.workspaceDir(in.Dir)
	if err != nil {
		return PublishOutputsOutput{}, stepError("publish outputs", err)
	}
	if err := a.awaitRunEnd(ctx, dir); err != nil {
		return PublishOutputsOutput{}, stepError("publish outputs", err)
	}
	defer heartbeatWhile(ctx, "publishing the outputs")()
	var out PublishOutputsOutput
	var upload outputs.Upload
	max := int64(tool.DefaultMaxFileBytes)
	if a.Publisher != nil {
		max = a.Publisher.MaxFileBytes()
		ctx = publishingFor(ctx, in.Call)
		upload = func(ctx context.Context, name string, content []byte) error {
			f, err := a.Publisher.Publish(ctx, name, content)
			if err == nil {
				out.Files = append(out.Files, f)
			}
			return err
		}
	}
	var reserved []string
	if in.Branch != "" {
		reserved = append(reserved, machine.BundleName(in.Branch))
	}
	out.Unpublished = outputs.Publish(ctx, outputsDir(dir), max, upload, reserved...)
	return out, nil
}

// publishingFor is ctx as the Publisher reads a call's: its session turn
// and ID, its user, the agent that made it (the last of its chain).
func publishingFor(ctx context.Context, call tool.CallContext) context.Context {
	ctx = tool.WithCall(ctx, call)
	ctx = tool.WithUserID(ctx, call.UserID)
	if n := len(call.AgentChain); n > 0 {
		ctx = tool.WithAgentID(ctx, call.AgentChain[n-1])
	}
	return ctx
}

// lowerCap is the smaller of two budget caps, where zero means none.
func lowerCap(a, b float64) float64 {
	if a <= 0 || (b > 0 && b < a) {
		return max(b, 0)
	}
	return a
}

// workspacePath resolves a run's directory and refuses anything that would
// land outside Root.
func (a *ClaudeCodeActivities) workspacePath(name string) (string, error) {
	return rootPath(a.Root, name)
}

// rootPath is the entry name directly under root.
func rootPath(root, name string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("claude code: workspace root is not configured")
	}
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("claude code: %q is not a valid workspace name", name)
	}
	return filepath.Join(filepath.Clean(root), name), nil
}

// git runs one git command, heartbeating so a clone that hangs is noticed in
// seconds rather than at the activity's timeout.
func (a *ClaudeCodeActivities) git(ctx context.Context, dir string, args ...string) (string, error) {
	return a.gitEnv(ctx, dir, nil, args...)
}

// sshEnv is the environment a git command needs to authenticate to a remote,
// or nil when this worker holds no identity.
//
// IdentitiesOnly stops ssh from offering every other key it can find, so a
// command can only reach what this one key opens.
func (a *ClaudeCodeActivities) sshEnv() []string {
	if a.SSHKeyPath == "" {
		return nil
	}
	return []string{fmt.Sprintf(
		"GIT_SSH_COMMAND=ssh -i %s -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new",
		a.SSHKeyPath)}
}

// gitSafeArgs come before every git command of the worker. Its git commands
// run in a tree the run had write access to: whatever the run left there must
// not make git run a program. The configuration is restored before
// (restoreGitConfig); these hold even if something was missed — no hooks, no
// filesystem monitor.
var gitSafeArgs = subproc.GitSafeArgs

// gitEnv is git with extra environment entries for this command only. Anything
// secret belongs here and never in the worker's own environment.
//
// The worker's environment is not passed on (subproc.Env): git and the ssh it
// starts need none of the platform's credentials. Nor is the system's or the
// user's git configuration read: what git does here is the code's decision.
//
// Its words are git's own, in English (LC_ALL=C): pushRefused reads them. It
// runs in a session of its own, ended with it: an ssh it started does not
// hold its output open past the step's end.
func (a *ClaudeCodeActivities) gitEnv(ctx context.Context, dir string, env []string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append(append([]string(nil), gitSafeArgs...), args...)...)
	cmd.Dir = dir
	cmd.Env = subproc.GitEnv(os.Environ())
	cmd.Env = append(cmd.Env, env...)
	cmd.Env = append(cmd.Env, "LC_ALL=C")
	subproc.KillGroupOnCancel(cmd, syscall.SIGTERM, 5*time.Second)
	subproc.NewSession(cmd)

	defer heartbeatWhile(ctx, strings.Join(args, " "))()

	out, err := cmd.CombinedOutput()
	subproc.KillGroup(cmd)
	return string(out), err
}

type InspectWorkspaceInput struct {
	Dir string `json:"dir"`
	// Base is the commit the workspace started at; commits are counted from
	// there rather than from a branch name the run could have moved.
	Base   string `json:"base"`
	Branch string `json:"branch,omitempty"`
}

type CommitInfo struct {
	SHA     string `json:"sha"`
	Subject string `json:"subject"`
}

type InspectWorkspaceOutput struct {
	// GitConfigChanged reports a run that changed the clone's git
	// configuration, put back before inspecting. A run has no reason to; one
	// that does may be steering the push, which must not happen.
	GitConfigChanged bool         `json:"git_config_changed,omitempty"`
	Commits          []CommitInfo `json:"commits,omitempty"`
	// Branch is where HEAD actually is, which is not necessarily where the
	// preparation left it.
	Branch string `json:"branch"`
	// Dirty reports changes the run left uncommitted. They die with the
	// workspace, so the caller has to be told rather than left to assume the
	// commits hold everything.
	Dirty bool `json:"dirty"`
}

// InspectWorkspace reports what the run actually produced. It reads the tree
// rather than the run's account of itself: a report claiming a commit and a
// branch with no commit on it are both things that happen.
func (a *ClaudeCodeActivities) InspectWorkspace(ctx context.Context, in InspectWorkspaceInput) (InspectWorkspaceOutput, error) {
	if in.Dir == "" || in.Base == "" {
		return InspectWorkspaceOutput{}, fmt.Errorf("inspect workspace: dir and base are required")
	}
	dir, err := a.workspaceDir(in.Dir)
	if err != nil {
		return InspectWorkspaceOutput{}, stepError("inspect workspace", err)
	}
	if err := a.awaitRunEnd(ctx, dir); err != nil {
		return InspectWorkspaceOutput{}, stepError("inspect workspace", err)
	}
	var out InspectWorkspaceOutput
	changed, err := a.restoreGitConfig(in.Dir)
	if err != nil {
		return out, stepError("inspect workspace", err)
	}
	out.GitConfigChanged = changed

	branch, err := a.git(ctx, in.Dir, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return out, fmt.Errorf("read current branch: %w: %s", err, branch)
	}
	out.Branch = strings.TrimSpace(branch)

	// Not into submodules: git status runs a git of its own in each, under
	// that repository's configuration, which the run wrote and nothing
	// restored — a clean filter there is a command it runs.
	status, err := a.git(ctx, in.Dir, "status", "--porcelain", "--ignore-submodules=all")
	if err != nil {
		return out, fmt.Errorf("read status: %w: %s", err, status)
	}
	out.Dirty = strings.TrimSpace(status) != ""

	// %H %s, one commit per line, oldest last. An unknown base is an error
	// worth surfacing: it means the history was rewritten under us.
	log, err := a.git(ctx, in.Dir, "log", "--format=%H %s", in.Base+"..HEAD")
	if err != nil {
		return out, fmt.Errorf("list commits since %s: %w: %s", in.Base, err, log)
	}
	for _, line := range strings.Split(strings.TrimSpace(log), "\n") {
		sha, subject, found := strings.Cut(strings.TrimSpace(line), " ")
		if !found && sha == "" {
			continue
		}
		out.Commits = append(out.Commits, CommitInfo{SHA: sha, Subject: subject})
	}
	return out, nil
}

type PushBranchInput struct {
	Dir string `json:"dir"`
	// Remote is the URL to push to, passed in by the workflow rather than read
	// from .git/config — which the run had write access to.
	Remote string `json:"remote"`
	Branch string `json:"branch"`
	// Commit is what Branch is set to on the remote: the newest commit the
	// inspection listed (InspectWorkspaceOutput.Commits), a full SHA. Not
	// the branch's ref in the clone, which something the run left running
	// may have moved since.
	Commit string `json:"commit"`
}

// PushBranch publishes the run's branch. This is the one step where a secret
// meets a working tree that the run had write access to, so it takes nothing
// from that tree: not the remote URL, not the git configuration (which could
// rewrite that URL), not the hooks git would otherwise run on the way out,
// and not the commit to publish, which is the one the inspection reported.
func (a *ClaudeCodeActivities) PushBranch(ctx context.Context, in PushBranchInput) error {
	if in.Dir == "" || in.Remote == "" || in.Branch == "" || in.Commit == "" {
		return temporal.NewNonRetryableApplicationError(
			"push: dir, remote, branch and commit are required", "InvalidInput", nil)
	}
	if err := a.checkRepo(in.Remote); err != nil {
		return stepError("push", err)
	}
	if strings.HasPrefix(in.Branch, "-") {
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("push: invalid branch %q", in.Branch), "InvalidInput", nil)
	}
	if !isFullSHA(in.Commit) {
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("push: %q is not a full commit SHA", in.Commit), "InvalidInput", nil)
	}
	dir, err := a.workspaceDir(in.Dir)
	if err != nil {
		return stepError("push", err)
	}
	if err := a.awaitRunEnd(ctx, dir); err != nil {
		return stepError("push", err)
	}
	// InspectWorkspace restored the configuration already: a change now was
	// made after it, by something the run left running.
	if changed, err := a.restoreGitConfig(in.Dir); err != nil {
		return stepError("push", err)
	} else if changed {
		return temporal.NewNonRetryableApplicationError(
			"push: the clone's git configuration changed since the inspection", "WorkspaceTampered", nil)
	}

	// An explicit refspec, from the inspected commit to the branch: what is
	// published is what the inspection reported, whatever the clone's refs
	// say now, and a tag the run happened to name like the branch is not.
	out, err := a.gitEnv(ctx, in.Dir, a.sshEnv(),
		"push", "--", in.Remote, in.Commit+":refs/heads/"+in.Branch)
	if err != nil {
		// Typed, and retried as any failure: the workflow keeps the commits
		// of a push that failed for good (BundleBranch).
		return temporal.NewApplicationError(fmt.Sprintf("git push of %s to %s: %v: %s",
			in.Branch, in.Remote, err, machine.Cut(strings.TrimSpace(out), 2048)), ErrPushFailed)
	}
	return nil
}

// ErrPushFailed is the type of PushBranch's error when git push itself
// failed (the remote refused it, a hook of its, the credentials): what
// BundleBranch is for. Not a workspace tampered with, nor a run still
// active: those keep nothing.
const ErrPushFailed = "PushFailed"

type BundleBranchInput struct {
	Dir string `json:"dir"`
	// Base is where the workspace started (PrepareWorkspaceOutput.Commit),
	// Commit what the push tried to publish (PushBranchInput.Commit), both
	// full SHAs: the bundle holds the commits between them, as Branch.
	Base   string `json:"base"`
	Branch string `json:"branch"`
	Commit string `json:"commit"`
	// Call is the run's call: the session turn and tool call the bundle
	// goes to, as PublishOutputsInput's.
	Call tool.CallContext `json:"call"`
}

// ErrBundleFailed is the type of BundleBranch's error when the bundle could
// not be made or is too large to publish: never retried.
const ErrBundleFailed = "BundleFailed"

// BundleBranch keeps the commits of a push that failed (ErrPushFailed): a git
// bundle of the branch at Commit, from Base (machine.WriteBundle), made by
// the worker's git once the run is gone and the clone's configuration
// restored (a changed one keeps nothing: WorkspaceTampered); written in Root
// (bundlePath), never where the run's user or its outputs are; published as a
// file of the call for its user to fetch and push themselves. Within the
// Publisher's bound, which a bundle past it is said to exceed. Retried, it
// stores nothing twice: the same bytes are the file stored first
// (machine.WriteBundle packs on one thread), and other bytes under the name,
// which the outputs may not take, are an earlier attempt's bundle, returned.
func (a *ClaudeCodeActivities) BundleBranch(ctx context.Context, in BundleBranchInput) (tool.FileRef, error) {
	if !isFullSHA(in.Base) || !isFullSHA(in.Commit) || in.Branch == "" || strings.HasPrefix(in.Branch, "-") {
		return tool.FileRef{}, temporal.NewNonRetryableApplicationError(
			"bundle: a full base and commit SHA and a branch are required", "InvalidInput", nil)
	}
	dir, err := a.workspaceDir(in.Dir)
	if err != nil {
		return tool.FileRef{}, stepError("bundle", err)
	}
	if a.Publisher == nil {
		return tool.FileRef{}, temporal.NewNonRetryableApplicationError("this worker publishes no file", ErrBundleFailed, nil)
	}
	if err := a.awaitRunEnd(ctx, dir); err != nil {
		return tool.FileRef{}, stepError("bundle", err)
	}
	if changed, err := a.restoreGitConfig(dir); err != nil {
		return tool.FileRef{}, stepError("bundle", err)
	} else if changed {
		return tool.FileRef{}, temporal.NewNonRetryableApplicationError(
			"the clone's git configuration changed since the inspection", "WorkspaceTampered", nil)
	}
	path := bundlePath(dir)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return tool.FileRef{}, stepError("bundle", err)
	}
	// The worker's git, as for the inspection: nothing goes to the network.
	run := func(ctx context.Context, args ...string) (string, error) { return a.git(ctx, dir, args...) }
	if err := machine.WriteBundle(ctx, run, in.Branch, in.Base, in.Commit, path); err != nil {
		if ctx.Err() != nil {
			return tool.FileRef{}, stepError("bundle", ctx.Err())
		}
		return tool.FileRef{}, temporal.NewNonRetryableApplicationError(err.Error(), ErrBundleFailed, nil)
	}
	content, err := machine.ReadBundle(path, a.Publisher.MaxFileBytes())
	if err != nil {
		return tool.FileRef{}, temporal.NewNonRetryableApplicationError(err.Error(), ErrBundleFailed, nil)
	}
	ctx = publishingFor(ctx, in.Call)
	name := machine.BundleName(in.Branch)
	f, err := a.Publisher.Publish(ctx, name, content)
	if errors.Is(err, store.ErrFileExists) {
		// Published by an attempt before this one: the outputs may not take
		// the name (PublishOutputsInput.Branch), so the file is the bundle.
		if stored, found, serr := a.Publisher.Stored(ctx, name); serr == nil && found {
			return stored, nil
		}
	}
	if err != nil {
		return tool.FileRef{}, stepError("bundle: publish", err)
	}
	return f, nil
}

// isFullSHA tells whether s is a full object name, SHA-1 or SHA-256: what
// git log prints with %H, and nothing git could read as a ref or an option.
func isFullSHA(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
