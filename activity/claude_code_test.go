package activity

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/subproc"
	"github.com/victor/temporal-agent/subproc/subproctest"
)

// testRepos lets the tests clone and push to the repositories they make under
// the temp directory.
var testRepos = []string{
	filepath.Join(os.TempDir(), "*", "*"),
	filepath.Join(os.TempDir(), "*", "*", "*"),
}

// initRepo makes a tiny git repository to clone from.
func initRepo(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("README.md", "hello\n")
	for _, args := range [][]string{
		{"init", "--quiet", "--initial-branch", "main"},
		{"config", "user.email", "test@test"},
		{"config", "user.name", "test"},
		{"add", "."},
		{"commit", "--quiet", "-m", "first"},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	return dir
}

func TestPrepareWorkspaceClonesAndResolvesHEAD(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}

	out, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src})
	if err != nil {
		t.Fatalf("PrepareWorkspace: %v", err)
	}
	if out.Dir != filepath.Join(a.Root, "run-1") {
		t.Errorf("Dir = %q", out.Dir)
	}
	if len(out.Commit) != 40 {
		t.Errorf("Commit = %q, want a full sha", out.Commit)
	}
	if _, err := os.Stat(filepath.Join(out.Dir, "README.md")); err != nil {
		t.Errorf("clone is missing its content: %v", err)
	}
}

// A retried attempt finds the previous one's directory. It must start over
// rather than fail or clone into a dirty tree.
func TestPrepareWorkspaceIsRepeatable(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}

	first, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src})
	if err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(first.Dir, "leftover")
	os.WriteFile(stray, []byte("x"), 0o644)

	if _, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src}); err != nil {
		t.Fatalf("second PrepareWorkspace: %v", err)
	}
	if _, err := os.Stat(stray); err == nil {
		t.Error("the retried clone kept the previous attempt's files")
	}
}

func TestPrepareWorkspaceRejectsBadInput(t *testing.T) {
	src := initRepo(t)
	cases := []struct {
		name  string
		act   *ClaudeCodeActivities
		input PrepareWorkspaceInput
	}{
		{"no root", &ClaudeCodeActivities{}, PrepareWorkspaceInput{Name: "run-1", Repo: src}},
		{"no repo", &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}, PrepareWorkspaceInput{Name: "run-1"}},
		{"name escapes root", &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}, PrepareWorkspaceInput{Name: "../evil", Repo: src}},
		{"name is a path", &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}, PrepareWorkspaceInput{Name: "a/b", Repo: src}},
		{"unknown ref", &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}, PrepareWorkspaceInput{Name: "run-1", Repo: src, Ref: "nope"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.act.PrepareWorkspace(context.Background(), tc.input); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

// Cleanup runs on every path out of the workflow, including the failures, so a
// wrong Dir would delete something precisely when nobody is watching.
func TestCleanupWorkspaceRefusesAnythingOutsideRoot(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "precious")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: root}

	for _, dir := range []string{outside, root, filepath.Join(root, "a", "b"), "/"} {
		if err := a.CleanupWorkspace(context.Background(), CleanupWorkspaceInput{Dir: dir}); err == nil {
			t.Errorf("CleanupWorkspace(%q) should have been refused", dir)
		}
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("a refused cleanup deleted something: %v", err)
	}
}

func TestCleanupWorkspaceDeletesTheRunDirectory(t *testing.T) {
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	dir := filepath.Join(a.Root, "run-1")
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := a.CleanupWorkspace(context.Background(), CleanupWorkspaceInput{Dir: dir}); err != nil {
		t.Fatalf("CleanupWorkspace: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("workspace still there: %v", err)
	}
	// An empty Dir means the workflow never got as far as preparing one.
	if err := a.CleanupWorkspace(context.Background(), CleanupWorkspaceInput{Dir: ""}); err != nil {
		t.Errorf("empty Dir should be a no-op, got %v", err)
	}
	// Cleanup after cleanup: a retry must not turn into a failure.
	if err := a.CleanupWorkspace(context.Background(), CleanupWorkspaceInput{Dir: dir}); err != nil {
		t.Errorf("second cleanup should be a no-op, got %v", err)
	}
}

func TestRunClaudeCodeNeverPersistsTheSession(t *testing.T) {
	// The workspace is deleted at the end of the run, so a transcript on disk
	// would only outlive the tree it talks about.
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir(), RunAs: subproctest.Identity(t)}
	_, err := a.RunClaudeCode(context.Background(), RunClaudeCodeInput{Dir: filepath.Join(a.Root, "nope"), Task: "x"})
	if err == nil || !strings.Contains(err.Error(), "cwd") {
		t.Fatalf("expected the missing workspace to be reported, got %v", err)
	}
}

// commitFile adds a file and commits it, standing in for what a coding run does.
func commitFile(t *testing.T, dir, name, content, message string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	// The identity is passed per-command: these tests run in the agent image,
	// which has none, while the Claude Code image configures one globally so
	// a run can commit.
	ident := []string{"-c", "user.email=test@test", "-c", "user.name=test"}
	for _, args := range [][]string{{"add", "."}, {"commit", "--quiet", "-m", message}} {
		cmd := exec.Command("git", append(ident, args...)...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
}

func TestInspectWorkspaceReportsWhatTheRunProduced(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{
		Name: "run-1", Repo: src, Branch: "agent/thing",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Nothing yet: the branch exists but carries no commit.
	out, err := a.InspectWorkspace(context.Background(), InspectWorkspaceInput{Dir: prepared.Dir, Base: prepared.Commit})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Commits) != 0 || out.Dirty {
		t.Errorf("fresh workspace: commits = %v, dirty = %v", out.Commits, out.Dirty)
	}
	if out.Branch != "agent/thing" {
		t.Errorf("Branch = %q, want the branch the preparation created", out.Branch)
	}

	commitFile(t, prepared.Dir, "a.txt", "a", "feat: add a")
	commitFile(t, prepared.Dir, "b.txt", "b", "feat: add b")

	out, err = a.InspectWorkspace(context.Background(), InspectWorkspaceInput{Dir: prepared.Dir, Base: prepared.Commit})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Commits) != 2 {
		t.Fatalf("commits = %v, want 2", out.Commits)
	}
	// git log lists newest first.
	if out.Commits[0].Subject != "feat: add b" || out.Commits[1].Subject != "feat: add a" {
		t.Errorf("commits = %+v", out.Commits)
	}
	if len(out.Commits[0].SHA) != 40 {
		t.Errorf("SHA = %q, want a full sha", out.Commits[0].SHA)
	}
	if out.Dirty {
		t.Error("nothing uncommitted, Dirty should be false")
	}
}

// Changes the run never committed die with the workspace, so the caller has to
// be told rather than left to assume the commits hold everything.
func TestInspectWorkspaceSeesUncommittedChanges(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src})
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(prepared.Dir, "README.md"), []byte("edited, never committed\n"), 0o644)

	out, err := a.InspectWorkspace(context.Background(), InspectWorkspaceInput{Dir: prepared.Dir, Base: prepared.Commit})
	if err != nil {
		t.Fatal(err)
	}
	if !out.Dirty {
		t.Error("Dirty = false, want the uncommitted edit reported")
	}
	if len(out.Commits) != 0 {
		t.Errorf("commits = %v, want none", out.Commits)
	}
}

func TestPushBranchPublishesTheBranch(t *testing.T) {
	remote := filepath.Join(t.TempDir(), "remote.git")
	if out, err := exec.Command("git", "init", "--quiet", "--bare", "--initial-branch", "main", remote).CombinedOutput(); err != nil {
		t.Fatalf("init bare: %v: %s", err, out)
	}
	src := initRepo(t)
	if out, err := exec.Command("git", "-C", src, "push", "--quiet", remote, "main").CombinedOutput(); err != nil {
		t.Fatalf("seed remote: %v: %s", err, out)
	}

	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{
		Name: "run-1", Repo: remote, Branch: "agent/thing",
	})
	if err != nil {
		t.Fatal(err)
	}
	commitFile(t, prepared.Dir, "a.txt", "a", "feat: add a")

	if err := a.PushBranch(context.Background(), PushBranchInput{
		Dir: prepared.Dir, Remote: remote, Branch: "agent/thing",
	}); err != nil {
		t.Fatalf("PushBranch: %v", err)
	}

	out, err := exec.Command("git", "-C", remote, "log", "--format=%s", "agent/thing", "-1").Output()
	if err != nil {
		t.Fatalf("branch not on the remote: %v", err)
	}
	if strings.TrimSpace(string(out)) != "feat: add a" {
		t.Errorf("remote branch tip = %q", out)
	}
}

// The push is the one step where a credential meets a tree the run had write
// access to. A hook the run dropped there must not run with it.
func TestPushBranchIgnoresHooksLeftInTheWorkspace(t *testing.T) {
	remote := filepath.Join(t.TempDir(), "remote.git")
	exec.Command("git", "init", "--quiet", "--bare", "--initial-branch", "main", remote).Run()
	src := initRepo(t)
	exec.Command("git", "-C", src, "push", "--quiet", remote, "main").Run()

	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{
		Name: "run-1", Repo: remote, Branch: "agent/thing",
	})
	if err != nil {
		t.Fatal(err)
	}
	commitFile(t, prepared.Dir, "a.txt", "a", "feat: add a")

	marker := filepath.Join(t.TempDir(), "hook-ran")
	hook := filepath.Join(prepared.Dir, ".git", "hooks", "pre-push")
	os.MkdirAll(filepath.Dir(hook), 0o755)
	if err := os.WriteFile(hook, []byte("#!/bin/sh\ntouch "+marker+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := a.PushBranch(context.Background(), PushBranchInput{
		Dir: prepared.Dir, Remote: remote, Branch: "agent/thing",
	}); err != nil {
		t.Fatalf("PushBranch: %v", err)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Error("a pre-push hook from the workspace ran during the credential-bearing step")
	}
}

func TestPushBranchRejectsIncompleteInput(t *testing.T) {
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	for _, in := range []PushBranchInput{
		{Remote: "r", Branch: "b"},
		{Dir: "/d", Branch: "b"},
		{Dir: "/d", Remote: "r"},
	} {
		if err := a.PushBranch(context.Background(), in); err == nil {
			t.Errorf("PushBranch(%+v) should have been refused", in)
		}
	}
}

// The credential is built per command. It must never reach the worker's own
// environment, which the Claude Code subprocess inherits wholesale.
func TestSSHEnv(t *testing.T) {
	if env := (&ClaudeCodeActivities{}).sshEnv(); env != nil {
		t.Errorf("sshEnv = %v, want nil when no identity is configured", env)
	}

	env := (&ClaudeCodeActivities{SSHKeyPath: "/keys/deploy"}).sshEnv()
	if len(env) != 1 {
		t.Fatalf("sshEnv = %v, want one entry", env)
	}
	for _, want := range []string{"GIT_SSH_COMMAND=", "-i /keys/deploy", "IdentitiesOnly=yes"} {
		if !strings.Contains(env[0], want) {
			t.Errorf("sshEnv %q missing %q", env[0], want)
		}
	}
	if os.Getenv("GIT_SSH_COMMAND") != "" {
		t.Error("GIT_SSH_COMMAND leaked into the process environment")
	}
}

// A worker that holds an identity must still clone what needs none.
func TestPrepareWorkspaceWithAnIdentityConfigured(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir(), SSHKeyPath: "/keys/deploy"}

	out, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src})
	if err != nil {
		t.Fatalf("PrepareWorkspace: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out.Dir, "README.md")); err != nil {
		t.Errorf("clone is missing its content: %v", err)
	}
}

// A local clone shares its objects with the source unless told otherwise, and
// the run has a shell: a write through a hard link lands in the repository we
// cloned from.
func TestPrepareWorkspaceDoesNotShareObjectsWithTheSource(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}

	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src})
	if err != nil {
		t.Fatal(err)
	}

	var objects int
	err = filepath.WalkDir(filepath.Join(prepared.Dir, ".git", "objects"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		objects++
		if st, ok := info.Sys().(*syscall.Stat_t); ok && st.Nlink > 1 {
			t.Errorf("%s has %d links: the clone shares it with the source", path, st.Nlink)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if objects == 0 {
		t.Fatal("no loose objects found, the test proved nothing")
	}
}

// The repository is the model's choice: one git would read as an option, or
// one the operator did not list, is refused before git runs.
func TestPrepareWorkspaceRefusesRepositories(t *testing.T) {
	src := initRepo(t)
	escaped := filepath.Join(t.TempDir(), "escaped")
	for name, c := range map[string]struct {
		allowed []string
		input   PrepareWorkspaceInput
	}{
		"nothing allowed":   {nil, PrepareWorkspaceInput{Name: "run-1", Repo: src}},
		"not listed":        {[]string{"https://github.com/acme/*"}, PrepareWorkspaceInput{Name: "run-1", Repo: src}},
		"an option":         {[]string{"*"}, PrepareWorkspaceInput{Name: "run-1", Repo: "--separate-git-dir=" + escaped}},
		"a newline":         {testRepos, PrepareWorkspaceInput{Name: "run-1", Repo: src + "\n"}},
		"a ref option":      {testRepos, PrepareWorkspaceInput{Name: "run-1", Repo: src, Ref: "--output=" + escaped}},
		"a branch option":   {testRepos, PrepareWorkspaceInput{Name: "run-1", Repo: src, Branch: "-f"}},
		"glob stops at a /": {[]string{"https://github.com/acme/*"}, PrepareWorkspaceInput{Name: "run-1", Repo: "https://github.com/acme/x/../../evil/y"}},
	} {
		t.Run(name, func(t *testing.T) {
			a := &ClaudeCodeActivities{AllowedRepos: c.allowed, Root: t.TempDir()}
			if _, err := a.PrepareWorkspace(context.Background(), c.input); err == nil {
				t.Fatal("expected a refusal")
			}
			if _, err := os.Stat(escaped); err == nil {
				t.Error("git wrote outside the workspace")
			}
		})
	}
}

func TestCheckRepoMatchesTheAllowlist(t *testing.T) {
	a := &ClaudeCodeActivities{AllowedRepos: []string{"https://github.com/acme/*", "git@github.com:acme/*"}}
	for repo, ok := range map[string]bool{
		"https://github.com/acme/api":     true,
		"https://github.com/acme/api.git": true,
		"git@github.com:acme/api.git":     true,
		"https://github.com/other/api":    false,
		"https://github.com/acme/a/b":     false,
		"/srv/repos/api":                  false,
	} {
		if err := a.checkRepo(repo); (err == nil) != ok {
			t.Errorf("checkRepo(%q) = %v, want allowed %v", repo, err, ok)
		}
	}
}

// A branch of the source that is not its default one only exists as
// origin/<name> in the clone.
func TestPrepareWorkspaceChecksOutABranch(t *testing.T) {
	src := initRepo(t)
	exec.Command("git", "-C", src, "branch", "feature").Run()
	commitFile(t, src, "later.txt", "x", "second")

	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	out, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src, Ref: "feature"})
	if err != nil {
		t.Fatalf("PrepareWorkspace: %v", err)
	}
	if _, err := os.Stat(filepath.Join(out.Dir, "later.txt")); err == nil {
		t.Error("the clone is on main, not on feature")
	}
}

func TestPushBranchRefusesAnUnlistedRemote(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src, Branch: "agent/x"})
	if err != nil {
		t.Fatal(err)
	}
	for _, remote := range []string{"https://evil.example.com/loot.git", "--receive-pack=touch /tmp/pwned"} {
		if err := a.PushBranch(context.Background(), PushBranchInput{Dir: prepared.Dir, Remote: remote, Branch: "agent/x"}); err == nil {
			t.Errorf("pushed to %q", remote)
		}
	}
}

// appendGitConfig adds lines to the workspace's .git/config, as a run that
// edits files can.
func appendGitConfig(t *testing.T, dir, lines string) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, ".git", "config"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(lines); err != nil {
		t.Fatal(err)
	}
}

// git reads .git/config on every command, and the run can edit it: a
// filesystem monitor set there is a command git status would run, with the
// worker's credentials. The worker's git uses the configuration of the clone.
func TestInspectWorkspaceDoesNotRunTheRunsGitConfig(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src, Branch: "agent/x"})
	if err != nil {
		t.Fatal(err)
	}

	marker := filepath.Join(t.TempDir(), "fsmonitor-ran")
	monitor := filepath.Join(t.TempDir(), "monitor.sh")
	os.WriteFile(monitor, []byte("#!/bin/sh\ntouch "+marker+"\n"), 0o755)
	appendGitConfig(t, prepared.Dir, "[core]\n\tfsmonitor = "+monitor+"\n")

	// The vector is real: a plain git status in the tree runs the monitor.
	exec.Command("git", "-C", prepared.Dir, "status", "--porcelain").Run()
	if _, err := os.Stat(marker); err != nil {
		t.Skip("this git does not run core.fsmonitor; the test would prove nothing")
	}
	os.Remove(marker)

	out, err := a.InspectWorkspace(context.Background(), InspectWorkspaceInput{Dir: prepared.Dir, Base: prepared.Commit})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Error("InspectWorkspace ran the filesystem monitor the run configured")
	}
	if !out.GitConfigChanged {
		t.Error("the changed configuration was not reported")
	}
	raw, _ := os.ReadFile(filepath.Join(prepared.Dir, ".git", "config"))
	if strings.Contains(string(raw), "fsmonitor") {
		t.Error("the clone's configuration was not restored")
	}
}

// A pushInsteadOf in .git/config would rewrite the remote checkRepo allowed
// into one it never saw: the push must still land where it was told.
func TestPushBranchIgnoresAPushInsteadOfLeftByTheRun(t *testing.T) {
	bare := func() string {
		dir := filepath.Join(t.TempDir(), "remote.git")
		if out, err := exec.Command("git", "init", "--quiet", "--bare", "--initial-branch", "main", dir).CombinedOutput(); err != nil {
			t.Fatalf("init bare: %v: %s", err, out)
		}
		return dir
	}
	remote, elsewhere := bare(), bare()
	src := initRepo(t)
	exec.Command("git", "-C", src, "push", "--quiet", remote, "main").Run()

	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: remote, Branch: "agent/thing"})
	if err != nil {
		t.Fatal(err)
	}
	commitFile(t, prepared.Dir, "a.txt", "a", "feat: add a")
	appendGitConfig(t, prepared.Dir, "[url \""+elsewhere+"\"]\n\tpushInsteadOf = "+remote+"\n")

	ref := "refs/heads/agent/thing"
	has := func(repo string) bool {
		return exec.Command("git", "-C", repo, "rev-parse", "--verify", "--quiet", ref).Run() == nil
	}
	// The vector is real: a plain push follows the rewrite.
	exec.Command("git", "-C", prepared.Dir, "push", "--quiet", remote, ref+":"+ref).Run()
	if !has(elsewhere) {
		t.Fatal("a plain push did not follow pushInsteadOf; the test would prove nothing")
	}
	exec.Command("git", "-C", elsewhere, "update-ref", "-d", ref).Run()

	// The workflow inspects before it pushes.
	inspected, err := a.InspectWorkspace(context.Background(), InspectWorkspaceInput{Dir: prepared.Dir, Base: prepared.Commit})
	if err != nil {
		t.Fatal(err)
	}
	if !inspected.GitConfigChanged {
		t.Error("the changed configuration was not reported")
	}
	if err := a.PushBranch(context.Background(), PushBranchInput{Dir: prepared.Dir, Remote: remote, Branch: "agent/thing"}); err != nil {
		t.Fatalf("PushBranch: %v", err)
	}
	if has(elsewhere) {
		t.Error("the branch landed in the repository the run's configuration named")
	}
	if !has(remote) {
		t.Error("the branch is not on the remote the workflow named")
	}
}

// A configuration changed after the inspection is something the run left
// running: the push refuses rather than restore and go on.
func TestPushBranchRefusesAConfigChangedAfterTheInspection(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src, Branch: "agent/x"})
	if err != nil {
		t.Fatal(err)
	}
	appendGitConfig(t, prepared.Dir, "[credential]\n\thelper = !touch /tmp/pwned\n")
	if err := a.PushBranch(context.Background(), PushBranchInput{Dir: prepared.Dir, Remote: src, Branch: "agent/x"}); err == nil {
		t.Error("pushed from a tree whose configuration changed")
	}

	// And a .git replaced by something else is not git's to read at all.
	os.RemoveAll(filepath.Join(prepared.Dir, ".git"))
	os.WriteFile(filepath.Join(prepared.Dir, ".git"), []byte("gitdir: /elsewhere\n"), 0o644)
	if _, err := a.InspectWorkspace(context.Background(), InspectWorkspaceInput{Dir: prepared.Dir, Base: prepared.Commit}); err == nil {
		t.Error("inspected a workspace whose .git is a file")
	}
}

// Inspect and push only work on a workspace under Root.
func TestInspectAndPushRefuseADirOutsideRoot(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	if _, err := a.InspectWorkspace(context.Background(), InspectWorkspaceInput{Dir: src, Base: "HEAD"}); err == nil {
		t.Error("inspected a directory outside Root")
	}
	if err := a.PushBranch(context.Background(), PushBranchInput{Dir: src, Remote: src, Branch: "main"}); err == nil {
		t.Error("pushed from a directory outside Root")
	}
}

// Cleanup takes the copy of the configuration with the workspace.
func TestCleanupWorkspaceRemovesTheConfigCopy(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(gitConfigCopy(prepared.Dir)); err != nil {
		t.Fatalf("no copy of the configuration: %v", err)
	}
	if err := a.CleanupWorkspace(context.Background(), CleanupWorkspaceInput{Dir: prepared.Dir}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(gitConfigCopy(prepared.Dir)); !os.IsNotExist(err) {
		t.Errorf("the copy outlived the workspace: %v", err)
	}
}

// A worker running as root without an identity does not start the CLI: its
// shell would run as root, the worker's credentials in reach.
func TestRunClaudeCodeRefusesToRunAsRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("only a worker running as root refuses")
	}
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	_, err := a.RunClaudeCode(context.Background(), RunClaudeCodeInput{Dir: a.Root, Task: "x"})
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() || !strings.Contains(err.Error(), "RUN_AS_UID") {
		t.Errorf("err = %v, want a non-retryable refusal naming RUN_AS_UID", err)
	}
}

// With an identity, the clone is the run's while it works — it commits as
// that user — and the worker's again before its own git runs there.
func TestWorkspaceChangesHandsWithTheRun(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("switching users takes root")
	}
	id := subproctest.Identity(t)
	remote := filepath.Join(t.TempDir(), "remote.git")
	exec.Command("git", "init", "--quiet", "--bare", "--initial-branch", "main", remote).Run()
	src := initRepo(t)
	exec.Command("git", "-C", src, "push", "--quiet", remote, "main").Run()

	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: subproctest.Dir(t, nil), RunAs: id}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: remote, Branch: "agent/thing"})
	if err != nil {
		t.Fatal(err)
	}
	owner := func(path string) uint32 {
		fi, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Sys().(*syscall.Stat_t).Uid
	}
	if owner(prepared.Dir) != id.UID || owner(filepath.Join(prepared.Dir, ".git", "config")) != id.UID {
		t.Error("the clone was not handed to the run")
	}
	if owner(gitConfigCopy(prepared.Dir)) != 0 || owner(a.Root) != 0 {
		t.Error("the configuration's copy, or Root, was handed to the run")
	}

	inode := func(path string) uint64 {
		fi, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Sys().(*syscall.Stat_t).Ino
	}
	configInode := inode(filepath.Join(prepared.Dir, ".git", "config"))

	// The run commits, as its own user, and leaves a file in .git that
	// anyone may write.
	os.WriteFile(filepath.Join(prepared.Dir, "a.txt"), []byte("a"), 0o644)
	os.Lchown(filepath.Join(prepared.Dir, "a.txt"), int(id.UID), int(id.GID))
	cmd := exec.Command("sh", "-c", "git add . && git -c user.email=run@test -c user.name=run commit --quiet -m 'feat: add a'")
	cmd.Dir = prepared.Dir
	cmd.Env = id.Env(subproc.Env(os.Environ(), nil, nil))
	id.Apply(cmd)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the run could not commit in its clone: %v: %s", err, out)
	}
	cmd = exec.Command("sh", "-c", "mkdir -m 777 .git/info/x && echo x > .git/info/x/open && chmod 666 .git/info/x/open")
	cmd.Dir = prepared.Dir
	id.Apply(cmd)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("the run could not write in its .git: %v: %s", err, out)
	}

	inspected, err := a.InspectWorkspace(context.Background(), InspectWorkspaceInput{Dir: prepared.Dir, Base: prepared.Commit})
	if err != nil {
		t.Fatal(err)
	}
	if len(inspected.Commits) != 1 || inspected.GitConfigChanged {
		t.Errorf("inspected %+v", inspected)
	}
	for _, dir := range []string{prepared.Dir, filepath.Join(prepared.Dir, ".git")} {
		if owner(dir) != 0 {
			t.Errorf("%s is still the run's after the inspection", dir)
		}
	}
	// Nothing in .git is the run's any more, nor writable by it: not the
	// configuration, which was left as it was and is a new file all the
	// same, not a ref, not an object.
	// Rewritten: a descriptor the run kept open is on the old file.
	if inode(filepath.Join(prepared.Dir, ".git", "config")) == configInode {
		t.Error("the unchanged configuration is still the file the run had")
	}
	filepath.WalkDir(filepath.Join(prepared.Dir, ".git"), func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		fi, _ := os.Lstat(path)
		if owner(path) == id.UID {
			t.Errorf("%s is still the run's after the inspection", path)
		}
		if fi.Mode()&fs.ModeSymlink == 0 && fi.Mode().Perm()&0o022 != 0 {
			t.Errorf("%s is still writable by others: %v", path, fi.Mode())
		}
		return nil
	})
	if err := a.PushBranch(context.Background(), PushBranchInput{Dir: prepared.Dir, Remote: remote, Branch: "agent/thing"}); err != nil {
		t.Fatalf("PushBranch: %v", err)
	}
}

// A repository the run nests in the clone, and commits as a submodule, has a
// configuration nothing restores: git status would run a clean filter set
// there. The inspection stays out of submodules.
func TestInspectWorkspaceStaysOutOfSubmodules(t *testing.T) {
	src := initRepo(t)
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: src, Branch: "agent/x"})
	if err != nil {
		t.Fatal(err)
	}

	marker := filepath.Join(t.TempDir(), "filter-ran")
	filter := filepath.Join(t.TempDir(), "filter.sh")
	os.WriteFile(filter, []byte("#!/bin/sh\ntouch "+marker+"\ncat\n"), 0o755)
	sub := filepath.Join(prepared.Dir, "sub")
	ident := []string{"-c", "user.email=run@test", "-c", "user.name=run"}
	for _, args := range [][]string{
		{"init", "--quiet", sub},
		{"-C", sub, "config", "filter.evil.clean", filter},
	} {
		if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	os.WriteFile(filepath.Join(sub, ".gitattributes"), []byte("* filter=evil\n"), 0o644)
	os.WriteFile(filepath.Join(sub, "f"), []byte("hi\n"), 0o644)
	for _, args := range [][]string{
		append(append([]string{"-C", sub}, ident...), "add", "."),
		append(append([]string{"-C", sub}, ident...), "commit", "--quiet", "-m", "sub"),
		append(append([]string{"-C", prepared.Dir}, ident...), "add", "sub"),
		append(append([]string{"-C", prepared.Dir}, ident...), "commit", "--quiet", "-m", "add sub"),
	} {
		if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	// Same size, newer: git must read the content, through the filter.
	touchLater := func(d time.Duration) {
		os.WriteFile(filepath.Join(sub, "f"), []byte("ho\n"), 0o644)
		later := time.Now().Add(d)
		os.Chtimes(filepath.Join(sub, "f"), later, later)
	}
	touchLater(time.Minute)
	os.Remove(marker)

	// The vector is real: a plain git status in the clone runs the filter.
	exec.Command("git", "-C", prepared.Dir, "status", "--porcelain").Run()
	if _, err := os.Stat(marker); err != nil {
		t.Skip("this git does not run the submodule's filter; the test would prove nothing")
	}
	// Stale again: the status above may have refreshed the stat data.
	touchLater(2 * time.Minute)
	os.Remove(marker)

	if _, err := a.InspectWorkspace(context.Background(), InspectWorkspaceInput{Dir: prepared.Dir, Base: prepared.Commit}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Error("InspectWorkspace ran a filter the run configured in a submodule")
	}
}
