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
	// Not a refusal of the identity: retried with the preparation.
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != ErrPushCheckFailed || appErr.NonRetryable() {
		t.Fatalf("PrepareWorkspace: %v, want a retryable %s", err, ErrPushCheckFailed)
	}
	for _, want := range []string{"may not push to " + remote, "--dry-run", "receive.unpacklimit", "nothing was run"} {
		if !strings.Contains(appErr.Message(), want) {
			t.Errorf("message lacks %q: %s", want, appErr.Message())
		}
	}
	// A local path is not reached with a key: no word of one.
	if strings.Contains(appErr.Message(), "CLAUDE_CODE_SSH_KEY") {
		t.Errorf("message: %s", appErr.Message())
	}
	if entries, _ := os.ReadDir(a.Root); len(entries) != 0 {
		t.Errorf("left in Root: %v", entries)
	}
	if _, err := a.PrepareWorkspace(context.Background(), PrepareWorkspaceInput{Name: "run-2", Repo: remote}); err != nil {
		t.Errorf("an analysis: %v", err)
	}
}

// refusedPushRun is a run whose push its remote refuses (a hook of the
// remote's; a dry run runs none): prepared, committed, inspected.
func refusedPushRun(t *testing.T, a *ClaudeCodeActivities) (remote string, prepared PrepareWorkspaceOutput, inspected InspectWorkspaceOutput) {
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
	remote, prepared, inspected := refusedPushRun(t, a)
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
		_, prepared, inspected := refusedPushRun(t, a)
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
		_, prepared, inspected := refusedPushRun(t, a)
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

// A refusal of the identity is told from a failure worth trying again.
func TestPushRefused(t *testing.T) {
	for out, want := range map[string]bool{
		"git@github.com: Permission denied (publickey).\nfatal: Could not read from remote repository.":                   true,
		"fatal: could not read Username for 'https://github.com': terminal prompts disabled":                              true,
		"remote: Permission to me/app.git denied to bot.\nfatal: unable to access: The requested URL returned error: 403": true,
		"remote: You are not allowed to push code to this project.":                                                       true,
		"ERROR: Repository not found.": true,
		"ssh: connect to host github.com port 22: Connection refused\nfatal: Could not read from remote repository.": false,
		"ssh: Could not resolve hostname github.com: Temporary failure in name resolution":                           false,
		"fatal: unable to access 'https://github.com/me/app.git/': Recv failure: Connection reset by peer":           false,
	} {
		if got := pushRefused(out); got != want {
			t.Errorf("pushRefused(%q) = %v, want %v", out, got, want)
		}
	}
}

func TestIsSSHRemote(t *testing.T) {
	for repo, want := range map[string]bool{
		"git@github.com:me/app.git": true, "ssh://git@host/app.git": true, "host:app": true,
		"https://github.com/me/app.git": false, "/srv/git/app.git": false, "./a:b": false, "file:///srv/app.git": false,
	} {
		if got := isSSHRemote(repo); got != want {
			t.Errorf("isSSHRemote(%q) = %v, want %v", repo, got, want)
		}
	}
}

// The outputs may not take the name of the branch's bundle: a file the run
// left under it is not published, and the bundle is. An attempt that
// published the bundle, then failed, leaves it for the next one, whose
// bytes it returns whatever they are.
func TestBundleBranchOwnsItsName(t *testing.T) {
	saver := &fileSaver{}
	a := &ClaudeCodeActivities{AllowedRepos: testRepos, Root: t.TempDir(), Publisher: &tool.Publisher{Store: saver}}
	_, prepared, inspected := refusedPushRun(t, a)
	os.WriteFile(filepath.Join(outputsDir(prepared.Dir), "agent-thing.bundle"), []byte("fake"), 0o600)
	call := tool.CallContext{UserID: "u-1", CallID: "call-1", AgentChain: []string{"jarvis"}, Turn: &tool.TurnRef{SessionID: "s-1", TurnKey: "m3.jarvis"}}
	res, err := a.PublishOutputs(context.Background(), PublishOutputsInput{Dir: prepared.Dir, Call: call, Branch: "agent/thing"})
	if err != nil || len(res.Files) != 0 || len(res.Unpublished) != 1 || !strings.Contains(res.Unpublished[0], "reserved for the branch's bundle") {
		t.Fatalf("PublishOutputs: %+v %v", res, err)
	}
	in := BundleBranchInput{Dir: prepared.Dir, Base: prepared.Commit, Branch: "agent/thing", Commit: inspected.Commits[0].SHA, Call: call}
	f, err := a.BundleBranch(context.Background(), in)
	if err != nil || f.Name != "agent-thing.bundle" || string(saver.contents[f.Name]) == "fake" {
		t.Fatalf("BundleBranch: %+v %v", f, err)
	}

	// Other bytes under the name, an earlier attempt's: that file.
	saver.files[0].SHA256 = "an-earlier-attempt"
	again, err := a.BundleBranch(context.Background(), in)
	if err != nil || again.ID != f.ID || len(saver.files) != 1 {
		t.Errorf("replay: %+v %v, %d files", again, err, len(saver.files))
	}
}
