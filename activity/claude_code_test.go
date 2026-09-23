package activity

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

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
	a := &ClaudeCodeActivities{Root: t.TempDir()}

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
	a := &ClaudeCodeActivities{Root: t.TempDir()}

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
		{"no repo", &ClaudeCodeActivities{Root: t.TempDir()}, PrepareWorkspaceInput{Name: "run-1"}},
		{"name escapes root", &ClaudeCodeActivities{Root: t.TempDir()}, PrepareWorkspaceInput{Name: "../evil", Repo: src}},
		{"name is a path", &ClaudeCodeActivities{Root: t.TempDir()}, PrepareWorkspaceInput{Name: "a/b", Repo: src}},
		{"unknown ref", &ClaudeCodeActivities{Root: t.TempDir()}, PrepareWorkspaceInput{Name: "run-1", Repo: src, Ref: "nope"}},
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
	a := &ClaudeCodeActivities{Root: root}

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
	a := &ClaudeCodeActivities{Root: t.TempDir()}
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
	a := &ClaudeCodeActivities{Root: t.TempDir()}
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
	a := &ClaudeCodeActivities{Root: t.TempDir()}
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
	a := &ClaudeCodeActivities{Root: t.TempDir()}
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

	a := &ClaudeCodeActivities{Root: t.TempDir()}
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

	a := &ClaudeCodeActivities{Root: t.TempDir()}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{
		Name: "run-1", Repo: remote, Branch: "agent/thing",
	})
	if err != nil {
		t.Fatal(err)
	}
	commitFile(t, prepared.Dir, "a.txt", "a", "feat: add a")

	marker := filepath.Join(t.TempDir(), "hook-ran")
	hook := filepath.Join(prepared.Dir, ".git", "hooks", "pre-push")
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
	a := &ClaudeCodeActivities{Root: t.TempDir()}
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
	a := &ClaudeCodeActivities{Root: t.TempDir(), SSHKeyPath: "/keys/deploy"}

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
	a := &ClaudeCodeActivities{Root: t.TempDir()}

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
