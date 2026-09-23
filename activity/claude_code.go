package activity

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/victor/temporal-agent/claudecode"
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
	if in.Repo == "" {
		return PrepareWorkspaceOutput{}, fmt.Errorf("prepare workspace: repo is required")
	}

	// A retried attempt finds the previous one's half-written clone. Start over
	// rather than trying to repair it.
	if err := os.RemoveAll(dir); err != nil {
		return PrepareWorkspaceOutput{}, fmt.Errorf("prepare workspace: %w", err)
	}
	if err := os.MkdirAll(a.Root, 0o755); err != nil {
		return PrepareWorkspaceOutput{}, fmt.Errorf("prepare workspace: %w", err)
	}

	if out, err := a.git(ctx, "", "clone", "--quiet", in.Repo, dir); err != nil {
		return PrepareWorkspaceOutput{}, fmt.Errorf("clone %s: %w: %s", in.Repo, err, out)
	}
	if in.Ref != "" {
		// Detached: an analysis run reads a state, it does not continue a branch.
		if out, err := a.git(ctx, dir, "checkout", "--quiet", "--detach", in.Ref); err != nil {
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
	return PrepareWorkspaceOutput{Dir: dir, Commit: strings.TrimSpace(commit), Branch: in.Branch}, nil
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
	return os.RemoveAll(want)
}

type RunClaudeCodeInput struct {
	Dir  string `json:"dir"`
	Task string `json:"task"`

	// The rest is set by the workflow, never by the calling agent: it is what
	// keeps a read-only run read-only.
	Model              string   `json:"model,omitempty"`
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
	runner := a.Runner
	if runner == nil {
		runner = &claudecode.Runner{}
	}
	return runner.Run(ctx, claudecode.Params{
		Cwd:                in.Dir,
		Task:               in.Task,
		Model:              in.Model,
		PermissionMode:     in.PermissionMode,
		AllowedTools:       in.AllowedTools,
		DisallowedTools:    in.DisallowedTools,
		AppendSystemPrompt: in.AppendSystemPrompt,
		MaxBudgetUSD:       in.MaxBudgetUSD,
		SessionID:          in.SessionID,
		// The workspace is deleted at the end of the run, so a transcript on
		// disk would only outlive the tree it talks about.
		NoSessionPersistence: true,
	})
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

// gitEnv is git with extra environment entries for this command only. Anything
// secret belongs here and never in the worker's own environment: the Claude
// Code subprocess inherits os.Environ(), so a credential left there would be
// handed to the run itself.
func (a *ClaudeCodeActivities) gitEnv(ctx context.Context, dir string, env []string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	if len(env) > 0 {
		cmd.Env = append(os.Environ(), env...)
	}

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
	Commits []CommitInfo `json:"commits,omitempty"`
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
	var out InspectWorkspaceOutput

	branch, err := a.git(ctx, in.Dir, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return out, fmt.Errorf("read current branch: %w: %s", err, branch)
	}
	out.Branch = strings.TrimSpace(branch)

	status, err := a.git(ctx, in.Dir, "status", "--porcelain")
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
}

// PushBranch publishes the run's branch. This is the one step where a secret
// meets a working tree that the run had write access to, so it takes nothing
// from that tree: not the remote URL, and not the hooks git would otherwise
// run on the way out.
func (a *ClaudeCodeActivities) PushBranch(ctx context.Context, in PushBranchInput) error {
	if in.Dir == "" || in.Remote == "" || in.Branch == "" {
		return fmt.Errorf("push: dir, remote and branch are required")
	}

	var env []string
	if a.SSHKeyPath != "" {
		// IdentitiesOnly stops ssh from offering every other key it can find,
		// so this push can only reach what this key opens.
		env = append(env, fmt.Sprintf(
			"GIT_SSH_COMMAND=ssh -i %s -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new",
			a.SSHKeyPath))
	}

	// An explicit refspec: a tag the run happened to name like the branch
	// must not be what gets published.
	ref := "refs/heads/" + in.Branch
	out, err := a.gitEnv(ctx, in.Dir, env,
		"-c", "core.hooksPath=/dev/null",
		"push", in.Remote, ref+":"+ref)
	if err != nil {
		return fmt.Errorf("push %s: %w: %s", in.Branch, err, out)
	}
	return nil
}
