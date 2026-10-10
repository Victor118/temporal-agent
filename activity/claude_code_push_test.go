package activity

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/tool"
)

// gitIn runs git in dir, failing the test.
func gitIn(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// A run that will commit has its push tried first: a remote that will not
// take it ends the preparation for good, git's words said, the clone gone.
// An analysis pushes nothing: the same remote is cloned.
func TestPrepareWorkspaceChecksThePush(t *testing.T) {
	remote := bareRemote(t, initRepo(t))
	// The remote's receive-pack fails to start: any push fails, a fetch not.
	gitIn(t, remote, "config", "receive.unpackLimit", "notanumber")
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir()}
	_, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: remote, Branch: "agent/x"})
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != ErrPushCheckFailed || !appErr.NonRetryable() {
		t.Fatalf("PrepareWorkspace: %v, want a non-retryable %s", err, ErrPushCheckFailed)
	}
	for _, want := range []string{"may not push to " + remote, "--dry-run", "receive.unpacklimit", "CLAUDE_CODE_SSH_KEY", "nothing was run"} {
		if !strings.Contains(appErr.Message(), want) {
			t.Errorf("message lacks %q: %s", want, appErr.Message())
		}
	}
	if entries, _ := os.ReadDir(a.Root); len(entries) != 0 {
		t.Errorf("left in Root: %v", entries)
	}
	if _, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-2", Repo: remote}); err != nil {
		t.Errorf("an analysis: %v", err)
	}
}

// pushRefused is a run whose push its remote refuses (a hook of the
// remote's; a dry run runs none): prepared, committed, inspected.
func pushRefused(t *testing.T, a *ClaudeCodeActivities) (remote string, prepared PrepareWorkspaceOutput, inspected InspectWorkspaceOutput) {
	t.Helper()
	remote = bareRemote(t, initRepo(t))
	if err := os.WriteFile(filepath.Join(remote, "hooks", "pre-receive"), []byte("#!/bin/sh\necho refused by policy >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	prepared, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-1", Repo: remote, Branch: "agent/thing"})
	if err != nil {
		t.Fatalf("PrepareWorkspace: %v", err)
	}
	commitFile(t, prepared.Dir, "thing.txt", "thing\n", "Add the thing")
	inspected, err = a.InspectWorkspace(context.Background(), InspectWorkspaceInput{Dir: prepared.Dir, Base: prepared.Commit, Branch: "agent/thing"})
	if err != nil || len(inspected.Commits) != 1 {
		t.Fatalf("InspectWorkspace: %+v %v", inspected, err)
	}
	return remote, prepared, inspected
}

// A push git refuses is an ErrPushFailed, git's words said; its commits are
// kept: a bundle of the branch at the inspected commit, from the base,
// published for the call, from which a clone of the remote fetches that
// commit; deleted with the workspace.
func TestBundleBranchKeepsTheCommitsOfAFailedPush(t *testing.T) {
	saver := &fileSaver{}
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir(), Publisher: &tool.Publisher{Store: saver}}
	remote, prepared, inspected := pushRefused(t, a)
	sha := inspected.Commits[0].SHA
	err := a.PushBranch(context.Background(), PushBranchInput{Dir: prepared.Dir, Remote: remote, Branch: "agent/thing", Commit: sha})
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != ErrPushFailed || appErr.NonRetryable() || !strings.Contains(appErr.Message(), "refused by policy") {
		t.Fatalf("PushBranch: %v, want a %s with git's words", err, ErrPushFailed)
	}
	// Something left on the branch since the inspection: the bundle holds
	// the inspected commit all the same.
	commitFile(t, prepared.Dir, "later.txt", "later\n", "Later")

	call := tool.CallContext{UserID: "u-1", CallID: "call-1", AgentChain: []string{"jarvis"}, Turn: &tool.TurnRef{SessionID: "s-1", TurnKey: "m3.jarvis"}}
	in := BundleBranchInput{Dir: prepared.Dir, Base: prepared.Commit, Branch: "agent/thing", Commit: sha, Call: call}
	f, err := a.BundleBranch(context.Background(), in)
	if err != nil || f.Name != "agent-thing.bundle" || f.ID == "" || len(saver.files) != 1 {
		t.Fatalf("BundleBranch: %+v %v", f, err)
	}
	if s := saver.files[0]; s.SessionID != "s-1" || s.TurnKey != "m3.jarvis" || s.CallID != "call-1" || s.AgentID != "jarvis" || s.UserID != "u-1" {
		t.Errorf("stored %+v", s)
	}
	if _, err := os.Stat(bundlePath(prepared.Dir)); err != nil {
		t.Errorf("no bundle in Root: %v", err)
	}
	// The user's way: a clone of the remote, the bundle fetched.
	dir := t.TempDir()
	saved := filepath.Join(dir, f.Name)
	os.WriteFile(saved, saver.contents[f.Name], 0o600)
	clone := filepath.Join(dir, "clone")
	if out, err := exec.Command("git", "clone", "--quiet", remote, clone).CombinedOutput(); err != nil {
		t.Fatalf("clone: %v %s", err, out)
	}
	gitIn(t, clone, "fetch", "--quiet", saved, "agent/thing:agent/thing")
	if got := gitIn(t, clone, "rev-parse", "refs/heads/agent/thing"); got != sha {
		t.Errorf("fetched %s, want %s", got, sha)
	}

	// Again (a retry): nothing more stored.
	if _, err := a.BundleBranch(context.Background(), in); err != nil || len(saver.files) != 1 {
		t.Errorf("retry: %v, %d files", err, len(saver.files))
	}
	if err := a.CleanupWorkspace(context.Background(), CleanupWorkspaceInput{Dir: prepared.Dir}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(bundlePath(prepared.Dir)); !os.IsNotExist(err) {
		t.Errorf("bundle left: %v", err)
	}
}

// A bundle is not made from a clone whose configuration changed since the
// inspection, nor published past the Publisher's bound: said, for good.
func TestBundleBranchRefuses(t *testing.T) {
	t.Run("configuration changed", func(t *testing.T) {
		saver := &fileSaver{}
		a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir(), Publisher: &tool.Publisher{Store: saver}}
		_, prepared, inspected := pushRefused(t, a)
		appendGitConfig(t, prepared.Dir, "[core]\n\tfsmonitor = touch /tmp/pwned\n")
		_, err := a.BundleBranch(context.Background(), BundleBranchInput{Dir: prepared.Dir, Base: prepared.Commit, Branch: "agent/thing",
			Commit: inspected.Commits[0].SHA})
		if !hasType(err, "WorkspaceTampered") || len(saver.files) != 0 {
			t.Errorf("BundleBranch: %v, %d files", err, len(saver.files))
		}
	})
	t.Run("too large", func(t *testing.T) {
		saver := &fileSaver{}
		a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir(), Publisher: &tool.Publisher{Store: saver, MaxBytes: 10}}
		_, prepared, inspected := pushRefused(t, a)
		_, err := a.BundleBranch(context.Background(), BundleBranchInput{Dir: prepared.Dir, Base: prepared.Commit, Branch: "agent/thing",
			Commit: inspected.Commits[0].SHA})
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) || appErr.Type() != ErrBundleFailed || !appErr.NonRetryable() ||
			!strings.Contains(appErr.Message(), "a published file may be") || len(saver.files) != 0 {
			t.Errorf("BundleBranch: %v, %d files", err, len(saver.files))
		}
	})
	t.Run("not a full SHA", func(t *testing.T) {
		a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir(), Publisher: &tool.Publisher{Store: &fileSaver{}}}
		if _, err := a.BundleBranch(context.Background(), BundleBranchInput{Dir: filepath.Join(a.Root, "run-1"), Base: "main",
			Branch: "agent/thing", Commit: strings.Repeat("a", 40)}); !hasType(err, "InvalidInput") {
			t.Errorf("BundleBranch: %v", err)
		}
	})
}
