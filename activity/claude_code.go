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
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/claudecode"
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
}

// RunCounter is what the coding activities need of subproc.Runs: a run
// counted while the CLI works, and the processes a run left behind ended
// before a clone is taken back, when no run is under way.
type RunCounter interface {
	Hold() (release func())
	KillStrays()
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
}

type PrepareWorkspaceOutput struct {
	Dir string `json:"dir"`
	// Commit is where the workspace started: the base a later inspection
	// measures the run's commits against.
	Commit string `json:"commit"`
	Branch string `json:"branch,omitempty"`
}

// PrepareWorkspace clones repo into a fresh directory under Root. It clones
// with whatever credentials the worker has and no more: nothing here arranges
// write access, because a read-only run has no use for it.
func (a *ClaudeCodeActivities) PrepareWorkspace(ctx context.Context, in PrepareWorkspaceInput) (PrepareWorkspaceOutput, error) {
	dir, err := a.workspacePath(in.Name)
	if err != nil {
		return PrepareWorkspaceOutput{}, err
	}
	if err := a.checkRepo(in.Repo); err != nil {
		return PrepareWorkspaceOutput{}, fmt.Errorf("prepare workspace: %w", err)
	}
	if strings.HasPrefix(in.Ref, "-") || strings.HasPrefix(in.Branch, "-") {
		return PrepareWorkspaceOutput{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("prepare workspace: invalid ref %q or branch %q", in.Ref, in.Branch), "InvalidInput", nil)
	}

	// A retried attempt finds the previous one's half-written clone. Start over
	// rather than trying to repair it.
	if err := removeWorkspace(dir); err != nil {
		return PrepareWorkspaceOutput{}, fmt.Errorf("prepare workspace: %w", err)
	}
	if err := os.MkdirAll(a.Root, 0o755); err != nil {
		return PrepareWorkspaceOutput{}, fmt.Errorf("prepare workspace: %w", err)
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
	}
	if err := keepGitConfig(dir); err != nil {
		return PrepareWorkspaceOutput{}, fmt.Errorf("prepare workspace: %w", err)
	}
	// The run works, and commits, as RunAs: the clone is its own. Root and the
	// configuration's copy stay the worker's.
	if err := a.RunAs.Give(dir); err != nil {
		return PrepareWorkspaceOutput{}, fmt.Errorf("prepare workspace: %w", err)
	}
	return PrepareWorkspaceOutput{Dir: dir, Commit: strings.TrimSpace(commit), Branch: in.Branch}, nil
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

// removeWorkspace deletes a run's directory, the copy of its git
// configuration and its CLI configuration.
func removeWorkspace(dir string) error {
	for _, path := range []string{dir, cliConfigDir(dir)} {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	if err := os.Remove(gitConfigCopy(dir)); err != nil && !os.IsNotExist(err) {
		return err
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
}

// RunClaudeCode runs one coding session and heartbeats while it does. A run the
// CLI reports as failed comes back as a Result, not an error: the workflow
// decides what a failed run means, and the report explains it better than an
// activity failure would.
func (a *ClaudeCodeActivities) RunClaudeCode(ctx context.Context, in RunClaudeCodeInput) (claudecode.Result, error) {
	if err := subproc.CheckRunAs(a.RunAs); err != nil {
		return claudecode.Result{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("claude code: %v", err), "RunAsRoot", nil)
	}
	dir, err := a.workspaceDir(in.Dir)
	if err != nil {
		return claudecode.Result{}, fmt.Errorf("claude code: %w", err)
	}
	// A configuration of the run's own, made afresh on every attempt: what an
	// earlier run, or attempt, wrote in one is never read by another. Without
	// an operator's configuration nor RunAs (a worker on a development
	// machine), the CLI keeps the worker's user's own (claudecode.OwnConfig).
	var configDir string
	if claudecode.OwnConfig(a.ClaudeConfigDir, a.RunAs) {
		configDir = cliConfigDir(dir)
		if err := claudecode.SeedConfigDir(configDir, a.ClaudeConfigDir, a.RunAs); err != nil {
			return claudecode.Result{}, fmt.Errorf("claude code: configuration: %w", err)
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
	if a.Runs != nil {
		runner.Runs = a.Runs
	}
	res, err := runner.Run(ctx, claudecode.Params{
		ConfigDir:          configDir,
		Cwd:                in.Dir,
		Task:               in.Task,
		Model:              a.Model,
		PermissionMode:     in.PermissionMode,
		AllowedTools:       in.AllowedTools,
		DisallowedTools:    in.DisallowedTools,
		AppendSystemPrompt: in.AppendSystemPrompt,
		MaxBudgetUSD:       lowerCap(a.MaxBudgetUSD, in.MaxBudgetUSD),
		SessionID:          in.SessionID,
		// The workspace is deleted at the end of the run, so a transcript on
		// disk would only outlive the tree it talks about.
		NoSessionPersistence: true,
	})
	if err != nil {
		return res, err
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
	if a.Root == "" {
		return "", fmt.Errorf("claude code: workspace root is not configured")
	}
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) {
		return "", fmt.Errorf("claude code: %q is not a valid workspace name", name)
	}
	return filepath.Join(filepath.Clean(a.Root), name), nil
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
var gitSafeArgs = []string{"-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-c", "submodule.recurse=false"}

// gitEnv is git with extra environment entries for this command only. Anything
// secret belongs here and never in the worker's own environment.
//
// The worker's environment is not passed on (subproc.Env): git and the ssh it
// starts need none of the platform's credentials. Nor is the system's or the
// user's git configuration read: what git does here is the code's decision.
func (a *ClaudeCodeActivities) gitEnv(ctx context.Context, dir string, env []string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", append(append([]string(nil), gitSafeArgs...), args...)...)
	cmd.Dir = dir
	cmd.Env = append(subproc.Env(os.Environ(), nil, nil), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
	cmd.Env = append(cmd.Env, env...)

	if activity.IsActivity(ctx) {
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			ticker := time.NewTicker(gitHeartbeat)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					activity.RecordHeartbeat(ctx, strings.Join(args, " "))
				}
			}
		}()
	}

	out, err := cmd.CombinedOutput()
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
	if _, err := a.workspaceDir(in.Dir); err != nil {
		return InspectWorkspaceOutput{}, fmt.Errorf("inspect workspace: %w", err)
	}
	var out InspectWorkspaceOutput
	changed, err := a.restoreGitConfig(in.Dir)
	if err != nil {
		return out, fmt.Errorf("inspect workspace: %w", err)
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
		return fmt.Errorf("push: %w", err)
	}
	if strings.HasPrefix(in.Branch, "-") {
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("push: invalid branch %q", in.Branch), "InvalidInput", nil)
	}
	if !isFullSHA(in.Commit) {
		return temporal.NewNonRetryableApplicationError(fmt.Sprintf("push: %q is not a full commit SHA", in.Commit), "InvalidInput", nil)
	}
	if _, err := a.workspaceDir(in.Dir); err != nil {
		return fmt.Errorf("push: %w", err)
	}
	// InspectWorkspace restored the configuration already: a change now was
	// made after it, by something the run left running.
	if changed, err := a.restoreGitConfig(in.Dir); err != nil {
		return fmt.Errorf("push: %w", err)
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
		return fmt.Errorf("push %s: %w: %s", in.Branch, err, out)
	}
	return nil
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
