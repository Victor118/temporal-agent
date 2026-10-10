//go:build unix

package connect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/victor/temporal-agent/claudecode"
	"github.com/victor/temporal-agent/machine"
)

// Implement is the implement_feature executor: clone at the base, on the
// directive's branch; run the CLI, which commits its work; inspect what it
// produced; push the inspected commit to the branch with the owner's git
// identity, only to a repository of --repos and with --allow-push; publish
// the outputs; delete the clone. As ImplementFeatureWorkflow does on a
// worker, with the same rules: no push when the run changed the clone's git
// configuration, left HEAD elsewhere, or made no commit. The output goes
// along with an error too (a run that did not complete: what it committed
// may still be pushed).
func (a *Coder) Implement(ctx context.Context, input json.RawMessage, progress func(string)) (json.RawMessage, error) {
	var in machine.ImplementInput
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, fmt.Errorf("implement_feature input: %w", err)
	}
	if err := in.Check(); err != nil {
		return nil, err
	}
	// Refused before anything runs: the workflow takes it elsewhere. A
	// branch that could not be pushed would be lost with the clone.
	if !a.AllowPush {
		return nil, Refuse("this machine does not let its runs push (agent connect --allow-push)")
	}
	if err := a.refuse(in.Repo, in.Skills); err != nil {
		return nil, err
	}
	r, err := a.newRun(true)
	if err != nil {
		return nil, err
	}
	defer r.remove()

	progress(machine.CloneProgress)
	base, err := a.clone(ctx, in.Repo, in.Base, r.clone)
	if err != nil {
		return nil, err
	}
	// The run starts on the branch that will carry its commits: nothing can
	// land on the base by accident.
	if out, err := git(ctx, r.clone, "checkout", "--quiet", "-b", in.Branch); err != nil {
		return nil, fmt.Errorf("create branch %s: %v: %s", in.Branch, err, out)
	}
	if err := keepGitConfig(r); err != nil {
		return nil, err
	}
	out := machine.CodingOutput{Commit: base, Branch: in.Branch}
	// The CLI stops before the directive's deadline: the push and the
	// outputs keep their time.
	cliCtx, cancel := context.WithDeadline(ctx, cliDeadline(ctx, time.Now()))
	defer cancel()
	p := claudecode.Params{
		Cwd:                r.clone,
		Task:               in.Task,
		PermissionMode:     machine.ImplementPermissionMode,
		AllowedTools:       machine.ImplementAllowedTools,
		DisallowedTools:    machine.ImplementDeniedTools,
		AppendSystemPrompt: machine.ImplementSystemPrompt + "\n" + machine.OutputsPrompt(r.outputs),
		AddDirs:            []string{r.outputs},
		MaxBudgetUSD:       in.MaxBudgetUSD,
	}
	skills, err := r.withSkills(in.Skills, &p)
	if err != nil {
		return nil, err
	}
	runErr := a.runCLI(cliCtx, p, skills, &out, progress)
	var refusal *Refusal
	if errors.As(runErr, &refusal) {
		return nil, runErr
	}
	if ctx.Err() != nil {
		// Stopped, cancelled or out of time: nothing more is done.
		out.Error = joinErrors(out.Error, "nothing was pushed")
	} else {
		// A run that failed may still have committed something worth
		// keeping: the commits decide.
		a.publish(ctx, in, r, base, &out, progress)
	}
	out.Unpublished = a.publishOutputs(ctx, r.outputs)
	raw, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return raw, runErr
}

// pushReserve is what an implementation keeps of its directive's time for
// what comes after its CLI: the inspection, the push, the outputs.
const pushReserve = 10 * time.Minute

// cliDeadline is when the CLI of an implementation must be done: pushReserve
// before ctx's deadline, or half the time left when it is short; none:
// far away.
func cliDeadline(ctx context.Context, now time.Time) time.Time {
	deadline, ok := ctx.Deadline()
	if !ok {
		return now.Add(100 * 365 * 24 * time.Hour)
	}
	reserve := pushReserve
	if left := deadline.Sub(now); left < 2*reserve {
		reserve = left / 2
	}
	return deadline.Add(-reserve)
}

// publish inspects what the run produced and pushes it when it may.
func (a *Coder) publish(ctx context.Context, in machine.ImplementInput, r *codingRun, base string, out *machine.CodingOutput, progress func(string)) {
	// The run has no reason to touch the repository's git configuration,
	// and what it could put there redirects or rides on the push, which
	// runs with the owner's credentials.
	changed, err := restoreGitConfig(r)
	if err != nil {
		out.Error = joinErrors(out.Error, fmt.Sprintf("could not inspect the clone: %v; nothing was pushed", err))
		return
	}
	head, err := git(ctx, r.clone, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		out.Error = joinErrors(out.Error, fmt.Sprintf("could not read the current branch: %v: %s; nothing was pushed", err, head))
		return
	}
	// Not into submodules: their configuration is the run's.
	status, err := git(ctx, r.clone, "status", "--porcelain", "--ignore-submodules=all")
	if err != nil {
		out.Error = joinErrors(out.Error, fmt.Sprintf("could not read the clone's status: %v; nothing was pushed", err))
		return
	}
	out.Dirty = status != ""
	commits, err := git(ctx, r.clone, "log", "--format=%H %s", base+"..HEAD")
	if err != nil {
		out.Error = joinErrors(out.Error, fmt.Sprintf("could not list the commits since %s: %v: %s; nothing was pushed", base, err, machine.Cut(commits, 1024)))
		return
	}
	for _, line := range strings.Split(commits, "\n") {
		if sha, subject, _ := strings.Cut(strings.TrimSpace(line), " "); sha != "" {
			out.Commits = append(out.Commits, machine.Commit{SHA: sha, Subject: machine.Cut(subject, 200)})
		}
	}
	switch {
	case changed:
		out.Error = joinErrors(out.Error, "the run changed the repository's git configuration (.git/config), so nothing was pushed")
		return
	case len(out.Commits) == 0:
		out.Error = joinErrors(out.Error, "the run produced no commit, so nothing was pushed")
		return
	case head != in.Branch:
		out.Error = joinErrors(out.Error, fmt.Sprintf("the run left HEAD on %q instead of %q, so nothing was pushed", head, in.Branch))
		return
	case !a.AllowPush || !a.AllowsRepo(in.Repo):
		out.Error = joinErrors(out.Error, "this machine does not let its runs push there, so nothing was pushed")
		return
	}
	// Restored again: a change since the inspection was made by something
	// the run left running.
	if changed, err := restoreGitConfig(r); err != nil || changed {
		out.Error = joinErrors(out.Error, "the clone's git configuration changed since the inspection, so nothing was pushed")
		return
	}
	progress(machine.PushProgress)
	// The newest commit the inspection listed, to the branch: what is
	// published is what out.Commits says, whatever the clone's refs say
	// now, and the remote is the directive's, never the clone's.
	sha := out.Commits[0].SHA
	if pushed, err := git(ctx, r.clone, "push", "--", in.Repo, sha+":refs/heads/"+in.Branch); err != nil {
		out.Error = joinErrors(out.Error, fmt.Sprintf("the commits were not pushed: %v: %s%s", err, machine.Cut(pushed, 2048), gitHint(pushed)))
		return
	}
	out.Pushed = true
	log.Printf("connect: pushed %s (%d commits) to %s", in.Branch, len(out.Commits), in.Repo)
}

// keepGitConfig keeps a copy of the clone's git configuration, outside the
// clone, which the inspection and the push use in place of whatever the run
// left in .git/config; and drops the sample hooks.
func keepGitConfig(r *codingRun) error {
	config, err := os.ReadFile(filepath.Join(r.clone, ".git", "config"))
	if err != nil {
		return err
	}
	if err := os.RemoveAll(filepath.Join(r.clone, ".git", "hooks")); err != nil {
		return err
	}
	return os.WriteFile(r.gitConfig, config, 0o600)
}

// restoreGitConfig puts the clone's configuration back in .git/config before
// git runs, with the owner's credentials, in a tree the run had write access
// to, and reports whether the run had changed it. core.fsmonitor,
// credential.helper, core.sshCommand or url.<x>.pushInsteadOf there are
// commands git would run, or another repository it would push to; a
// linked-worktree layout (commondir, config.worktree) would make it read its
// configuration elsewhere. A .git that is no longer a directory is refused.
// The file is rewritten even when unchanged: a new one, never written
// through whatever the run left at its path.
func restoreGitConfig(r *codingRun) (changed bool, err error) {
	gitDir := filepath.Join(r.clone, ".git")
	if fi, err := os.Lstat(gitDir); err != nil || !fi.IsDir() {
		return false, fmt.Errorf("%s is no longer the clone's git directory", gitDir)
	}
	saved, err := os.ReadFile(r.gitConfig)
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
	case err != nil, !fi.Mode().IsRegular():
		changed = true
	default:
		current, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(current, saved) {
			changed = true
		}
	}
	if err := os.RemoveAll(path); err != nil {
		return changed, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return changed, err
	}
	if _, err := f.Write(saved); err != nil {
		f.Close()
		return changed, err
	}
	return changed, f.Close()
}

func joinErrors(existing, add string) string {
	if existing == "" {
		return add
	}
	return existing + "; " + add
}
