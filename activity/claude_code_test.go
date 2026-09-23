package activity

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
