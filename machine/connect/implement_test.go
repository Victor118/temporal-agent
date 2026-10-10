//go:build unix

package connect

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/victor/temporal-agent/machine"
)

// commitScript is a stand-in CLI that commits a change on the branch it is
// on, leaves a report in its outputs, then plays extra and reports.
func commitScript(extra string) string {
	return `export GIT_AUTHOR_NAME=a GIT_AUTHOR_EMAIL=a@b GIT_COMMITTER_NAME=a GIT_COMMITTER_EMAIL=a@b
echo '{"type":"system","subtype":"init","session_id":"s","apiKeySource":"none"}'
echo '{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Edit","input":{}}]},"session_id":"s"}'
echo hello > health.txt
git add health.txt
git commit --quiet -m "Add health"
echo "# What I did" > ../outputs/notes.md
` + extra + `
echo '{"type":"result","subtype":"success","is_error":false,"result":"Added health.","session_id":"s","num_turns":3,"total_cost_usd":0.4}'
`
}

// published records what a Coder uploads.
type published struct {
	mu    sync.Mutex
	files map[string]string
}

func (p *published) upload(_ context.Context, name string, content []byte) (machine.FileRef, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.files == nil {
		p.files = map[string]string{}
	}
	p.files[name] = string(content)
	return machine.FileRef{ID: "f-" + name, Name: name, Size: int64(len(content))}, nil
}

// newImplementer is a Coder that may push to a bare copy of a fresh
// repository, which it returns.
func newImplementer(t *testing.T, script string) (*Coder, string, string, *published) {
	t.Helper()
	a, repo, seen := newAnalyzer(t, script)
	remote := filepath.Join(t.TempDir(), "remote.git")
	if out, err := exec.Command("git", "clone", "--quiet", "--bare", repo, remote).CombinedOutput(); err != nil {
		t.Fatalf("bare: %v %s", err, out)
	}
	a.Repos, a.AllowPush = []string{remote}, true
	pub := &published{}
	a.Upload = pub.upload
	return a, remote, seen, pub
}

func implementInput(repo string) json.RawMessage {
	raw, _ := json.Marshal(machine.ImplementInput{Repo: repo, Base: "main", Task: "Add a health file", Branch: "agent/health-1234abcd", MaxBudgetUSD: 1})
	return raw
}

// remoteBranch is the commit a branch of the remote points at; "" = none.
func remoteBranch(t *testing.T, remote, branch string) string {
	out, err := exec.Command("git", "-C", remote, "rev-parse", "--verify", "--quiet", "refs/heads/"+branch).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func TestCoder_Implement(t *testing.T) {
	// The owner's git identity may live in a configuration directory of
	// their own.
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(t.TempDir(), "xdg"))
	a, remote, seen, pub := newImplementer(t, commitScript(""))
	var progresses []string
	raw, err := a.Implement(context.Background(), implementInput(remote), func(p string) { progresses = append(progresses, p) })
	var out machine.CodingOutput
	if err != nil || json.Unmarshal(raw, &out) != nil {
		t.Fatalf("implement: %s %v", raw, err)
	}
	if !out.Pushed || out.Branch != "agent/health-1234abcd" || len(out.Commits) != 1 || out.Commits[0].Subject != "Add health" ||
		out.Dirty || out.Error != "" || out.Report != "Added health." || len(out.Unpublished) != 0 {
		t.Fatalf("output %+v", out)
	}
	if got := remoteBranch(t, remote, out.Branch); got != out.Commits[0].SHA {
		t.Errorf("the remote's branch: %q, want %s", got, out.Commits[0].SHA)
	}
	if remoteBranch(t, remote, "main") == out.Commits[0].SHA {
		t.Error("the base branch moved")
	}
	if pub.files["notes.md"] != "# What I did\n" {
		t.Errorf("outputs published: %v", pub.files)
	}
	args, _ := os.ReadFile(filepath.Join(seen, "args"))
	for _, want := range []string{"--permission-mode acceptEdits", "Bash(git commit:*)", "--disallowedTools Bash(git push:*)", "Edit(.git/**)",
		"--max-budget-usd 1 ", "--add-dir ", "/outputs", "--setting-sources user", "--strict-mcp-config", "Commit your work on that branch"} {
		if !strings.Contains(string(args), want) {
			t.Errorf("args lack %q: %s", want, args)
		}
	}
	for _, unwanted := range []string{"git log", "git diff", "git show"} {
		if strings.Contains(string(args), unwanted) {
			t.Errorf("args allow %q: %s", unwanted, args)
		}
	}
	env, _ := os.ReadFile(filepath.Join(seen, "env"))
	if !strings.Contains(string(env), "XDG_CONFIG_HOME="+os.Getenv("XDG_CONFIG_HOME")) || !strings.Contains(string(env), "repo.bin:") {
		t.Errorf("the CLI's XDG_CONFIG_HOME or PATH: %s", env)
	}
	if !strings.Contains(string(env), "GIT_CONFIG_KEY_0=core.hooksPath") || !strings.Contains(string(env), "GIT_CONFIG_VALUE_0=/dev/null") {
		t.Errorf("the CLI's git environment: %s", env)
	}
	if progresses[0] != machine.CloneProgress || progresses[len(progresses)-1] != machine.PushProgress {
		t.Errorf("progresses %v", progresses)
	}
	if entries, _ := os.ReadDir(a.WorkDir); len(entries) != 0 {
		t.Errorf("clone left behind: %v", entries)
	}
}

// The git the run starts runs no program its clone's configuration names:
// a hook, a filesystem monitor and an editor it set itself stay idle through
// its own commit (machine.RunGitEnv); and the configuration it changed means
// nothing is pushed.
func TestCoder_ImplementGitRunsNoProgramOfTheClone(t *testing.T) {
	a, remote, _, _ := newImplementer(t, `export GIT_AUTHOR_NAME=a GIT_AUTHOR_EMAIL=a@b GIT_COMMITTER_NAME=a GIT_COMMITTER_EMAIL=a@b
mkdir -p .git/hooks
printf '#!/bin/sh\ntouch "%s/../hook"\n' "$PWD" > .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
printf '[core]\n\tfsmonitor = touch %s/../fsmonitor; false\n\teditor = touch %s/../editor; true\n' "$PWD" "$PWD" >> .git/config
echo x > trapped.txt
git add trapped.txt
git commit --quiet -e -m trap
git status --porcelain >/dev/null
for f in hook fsmonitor editor; do [ -e ../$f ] && cp ../$f ../outputs/$f; done
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}'
`)
	pub := &published{}
	a.Upload = pub.upload
	raw, err := a.Implement(context.Background(), implementInput(remote), func(string) {})
	var out machine.CodingOutput
	if err != nil || json.Unmarshal(raw, &out) != nil {
		t.Fatalf("implement: %s %v", raw, err)
	}
	if len(pub.files) != 0 {
		t.Errorf("git ran the clone's programs: %v", pub.files)
	}
	if len(out.Commits) != 1 || out.Pushed || !strings.Contains(out.Error, "git configuration") {
		t.Errorf("output %+v", out)
	}
}

// The run's git reads a commit's message from the clone only: a secret of
// its owner's, outside, never reaches a pushed commit (subproc.WriteGitShim).
func TestCoder_ImplementCommitsNoFileFromOutside(t *testing.T) {
	secret := filepath.Join(t.TempDir(), "id_ed25519")
	os.WriteFile(secret, []byte("PRIVATE KEY"), 0o600)
	a, remote, _, _ := newImplementer(t, `export GIT_AUTHOR_NAME=a GIT_AUTHOR_EMAIL=a@b GIT_COMMITTER_NAME=a GIT_COMMITTER_EMAIL=a@b
echo hello > health.txt
git add health.txt
git commit --quiet -F '`+secret+`' && exit 3
/usr/bin/env git commit --quiet --file='`+secret+`' && exit 4
echo "Add health, from the clone" > msg.txt
git commit --quiet -F msg.txt || exit 5
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}'
`)
	raw, err := a.Implement(context.Background(), implementInput(remote), func(string) {})
	var out machine.CodingOutput
	if err != nil || json.Unmarshal(raw, &out) != nil {
		t.Fatalf("implement: %s %v", raw, err)
	}
	if !out.Pushed || len(out.Commits) != 1 || out.Commits[0].Subject != "Add health, from the clone" {
		t.Fatalf("output %+v", out)
	}
	log, _ := exec.Command("git", "-C", remote, "log", "--all", "--format=%B").Output()
	if strings.Contains(string(log), "PRIVATE") {
		t.Errorf("a secret pushed: %s", log)
	}
}

// Without --allow-push, an implementation is turned down before anything
// runs: the workflow takes it elsewhere.
func TestCoder_ImplementWithoutPush(t *testing.T) {
	a, remote, seen, _ := newImplementer(t, commitScript(""))
	a.AllowPush = false
	_, err := a.Implement(context.Background(), implementInput(remote), func(string) {})
	var refusal *Refusal
	if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "--allow-push") {
		t.Fatalf("not refused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(seen, "args")); err == nil {
		t.Error("the CLI ran")
	}
	if entries, _ := os.ReadDir(a.WorkDir); len(entries) != 0 {
		t.Errorf("cloned: %v", entries)
	}
	// A repository outside --repos, a branch outside agent/: refused too.
	a.AllowPush = true
	raw, _ := json.Marshal(machine.ImplementInput{Repo: "/elsewhere", Task: "x", Branch: "agent/x"})
	if _, err := a.Implement(context.Background(), raw, func(string) {}); !errors.As(err, &refusal) {
		t.Errorf("a repository not allowed: %v", err)
	}
	raw, _ = json.Marshal(machine.ImplementInput{Repo: remote, Task: "x", Branch: "main"})
	if _, err := a.Implement(context.Background(), raw, func(string) {}); err == nil || !strings.Contains(err.Error(), "agent/") {
		t.Errorf("the base branch as the run's: %v", err)
	}
}

// What the run leaves decides whether anything is pushed: a changed git
// configuration, no commit, HEAD on another branch — nothing is.
func TestCoder_ImplementNotPushed(t *testing.T) {
	for _, c := range []struct {
		name, script, says string
	}{
		{"git configuration changed", commitScript(`printf '[url "/tmp/elsewhere.git"]\n\tpushInsteadOf = x\n' >> .git/config`), "git configuration"},
		{"a linked worktree's configuration", commitScript(`echo x > .git/config.worktree`), "git configuration"},
		{"HEAD elsewhere", commitScript(`git checkout --quiet -b other`), `instead of "agent/health-1234abcd"`},
		{"no commit", `echo '{"type":"system","subtype":"init","session_id":"s"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"Nothing to do.","session_id":"s"}'`, "no commit"},
	} {
		t.Run(c.name, func(t *testing.T) {
			a, remote, _, _ := newImplementer(t, c.script)
			raw, err := a.Implement(context.Background(), implementInput(remote), func(string) {})
			var out machine.CodingOutput
			if err != nil || json.Unmarshal(raw, &out) != nil {
				t.Fatalf("implement: %s %v", raw, err)
			}
			// No push tried: no bundle either.
			if out.Pushed || !strings.Contains(out.Error, c.says) || !strings.Contains(out.Error, "nothing was pushed") || out.Bundle != "" || out.BundleError != "" {
				t.Errorf("output %+v", out)
			}
			if got := remoteBranch(t, remote, "agent/health-1234abcd"); got != "" {
				t.Errorf("pushed %s", got)
			}
		})
	}
}

// A push the remote refuses is said, the commits listed, and kept: a bundle
// of the branch, published for the directive, from which the user's own
// clone fetches the commit pushed; deleted with the run. A file the run left
// in its outputs under the bundle's name is not published: the name is the
// bundle's.
func TestCoder_ImplementPushRefused(t *testing.T) {
	a, remote, _, pub := newImplementer(t, commitScript(`echo fake > ../outputs/agent-health-1234abcd.bundle`))
	// A hook of the remote's refuses every push; a dry run runs none.
	refuseEveryPush(t, remote)
	raw, err := a.Implement(context.Background(), implementInput(remote), func(string) {})
	var out machine.CodingOutput
	json.Unmarshal(raw, &out)
	if err != nil || out.Pushed || len(out.Commits) != 1 || !strings.Contains(out.Error, "were not pushed") || !strings.Contains(out.Error, "refused by policy") {
		t.Fatalf("output %+v %v", out, err)
	}
	if out.Bundle != "agent-health-1234abcd.bundle" || out.BundleError != "" || pub.files[out.Bundle] == "" {
		t.Fatalf("bundle %q %q, published %v", out.Bundle, out.BundleError, pub.files)
	}
	if len(out.Unpublished) != 1 || !strings.Contains(out.Unpublished[0], "agent-health-1234abcd.bundle: the name agent-health-1234abcd.bundle is reserved") {
		t.Errorf("unpublished %v", out.Unpublished)
	}
	if got := fetchBundle(t, remote, []byte(pub.files[out.Bundle]), "agent/health-1234abcd"); got != out.Commits[0].SHA {
		t.Errorf("fetched %s, want %s", got, out.Commits[0].SHA)
	}
	if entries, _ := os.ReadDir(a.WorkDir); len(entries) != 0 {
		t.Errorf("left behind: %v", entries)
	}

	// Not kept: said, never claimed.
	for name, setup := range map[string]func(a *Coder){
		"too large":       func(a *Coder) { a.MaxFileBytes = 10 },
		"nothing uploads": func(a *Coder) { a.Upload = nil },
		"upload refused": func(a *Coder) {
			a.Upload = func(context.Context, string, []byte) (machine.FileRef, error) {
				return machine.FileRef{}, &UploadRefused{Reason: "over"}
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			a, remote, _, _ := newImplementer(t, commitScript(""))
			refuseEveryPush(t, remote)
			setup(a)
			raw, _ := a.Implement(context.Background(), implementInput(remote), func(string) {})
			var out machine.CodingOutput
			json.Unmarshal(raw, &out)
			if out.Pushed || out.Bundle != "" || out.BundleError == "" {
				t.Errorf("output %+v", out)
			}
		})
	}
}

// refuseEveryPush gives remote a hook that refuses every push.
func refuseEveryPush(t *testing.T, remote string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(remote, "hooks", "pre-receive"), []byte("#!/bin/sh\necho refused by policy >&2\nexit 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
}

// fetchBundle fetches branch from a bundle into a fresh clone of remote, as
// its user would, and returns the commit it got.
func fetchBundle(t *testing.T, remote string, bundle []byte, branch string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "saved.bundle")
	os.WriteFile(path, bundle, 0o600)
	clone := filepath.Join(dir, "clone")
	for _, args := range [][]string{{"clone", "--quiet", remote, clone}, {"-C", clone, "fetch", "--quiet", path, branch + ":" + branch}} {
		if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	got, err := exec.Command("git", "-C", clone, "rev-parse", "refs/heads/"+branch).Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(got))
}

// A remote that will not take the push is found before the CLI runs: the
// directive is refused (the workflow takes it elsewhere), git's words and
// the clone gone.
func TestCoder_ImplementPushCheckRefused(t *testing.T) {
	a, remote, seen, _ := newImplementer(t, commitScript(""))
	// The remote's receive-pack fails to start: any push fails, a fetch not.
	if out, err := exec.Command("git", "-C", remote, "config", "receive.unpackLimit", "notanumber").CombinedOutput(); err != nil {
		t.Fatalf("config: %v %s", err, out)
	}
	_, err := a.Implement(context.Background(), implementInput(remote), func(string) {})
	var refusal *Refusal
	if !errors.As(err, &refusal) || !strings.Contains(err.Error(), "may not push") || !strings.Contains(err.Error(), "--dry-run") ||
		!strings.Contains(err.Error(), "receive.unpacklimit") {
		t.Fatalf("not refused: %v", err)
	}
	if _, err := os.Stat(filepath.Join(seen, "args")); err == nil {
		t.Error("the CLI ran")
	}
	if entries, _ := os.ReadDir(a.WorkDir); len(entries) != 0 {
		t.Errorf("clone left behind: %v", entries)
	}
	// An analysis does not push: the same remote is analysed.
	a.Runner.Binary = fakeClaude(t, t.TempDir(), analyzeStream)
	if _, err := a.Analyze(context.Background(), input(remote, ""), func(string) {}); err != nil {
		t.Errorf("analysis: %v", err)
	}
}

// A push refused to an identity that cloned is its right to push; the same
// words at a clone are not said so.
func TestPushHint(t *testing.T) {
	const forbidden = "remote: Permission to me/app.git denied to bot.\nfatal: unable to access: The requested URL returned error: 403"
	for out, want := range map[string]string{
		"fatal: could not read Username for 'https://github.com': terminal prompts disabled": "credential helper",
		forbidden:                        "not push to it",
		"Permission denied (publickey).": "ssh key",
	} {
		if got := pushHint(out); !strings.Contains(got, want) {
			t.Errorf("pushHint(%q) = %q, want %q", out, got, want)
		}
	}
	if got := gitHint(forbidden); strings.Contains(got, "push") {
		t.Errorf("gitHint at a clone: %q", got)
	}
}

// An upload goes to the server with the machine's token, read again at each
// try; what the server refuses is said, not tried again.
func TestClient_Upload(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		q := r.URL.Query()
		switch {
		case r.Method != http.MethodPut || r.URL.Path != machine.FilesPath || q.Get("directive") != "d-1":
			w.WriteHeader(http.StatusBadRequest)
		case q.Get("name") == "taken.md":
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(machine.UploadError{Error: "a file named taken.md was already published"})
		case n == 1:
			// The token was just replaced: the next try reads the new one.
			w.WriteHeader(http.StatusUnauthorized)
		case r.Header.Get("Authorization") != "Bearer agm_new":
			w.WriteHeader(http.StatusUnauthorized)
		default:
			json.NewEncoder(w).Encode(machine.FileRef{ID: "f-1", Name: q.Get("name"), Size: r.ContentLength})
		}
	}))
	defer srv.Close()
	st := State{Dir: t.TempDir()}
	st.Init()
	st.Save(Config{Server: srv.URL, MachineID: "m-1", Token: "agm_new"})
	c := &Client{State: st}
	ctx := withDirective(context.Background(), "d-1")
	ref, err := c.Upload(ctx, "notes.md", []byte("# Notes"))
	if err != nil || ref.ID != "f-1" || ref.Size != 7 || calls.Load() != 2 {
		t.Errorf("upload: %+v %v after %d calls", ref, err, calls.Load())
	}
	var refused *UploadRefused
	before := calls.Load()
	if _, err := c.Upload(ctx, "taken.md", []byte("x")); !errors.As(err, &refused) || !strings.Contains(err.Error(), "already published") || calls.Load() != before+1 {
		t.Errorf("refused: %v", err)
	}
	if _, err := c.Upload(context.Background(), "x.md", nil); err == nil {
		t.Error("an upload outside a directive")
	}
}

// A directive's progress reaches its owner's terminal, a line per LogEvery
// at most, and only when it changed.
func TestClient_LogsProgress(t *testing.T) {
	var lines strings.Builder
	c := &Client{Log: log.New(&lines, "", 0), LogEvery: 50 * time.Millisecond}
	j := &job{loggedAt: time.Now(), done: true} // done: nothing is sent
	c.progress("d-1", j, "1 outil (dernier : Read)")
	time.Sleep(60 * time.Millisecond)
	c.progress("d-1", j, "2 outils (dernier : Grep)")
	c.progress("d-1", j, "3 outils (dernier : Grep)") // within the minute
	time.Sleep(60 * time.Millisecond)
	c.progress("d-1", j, "3 outils (dernier : Grep)")
	time.Sleep(60 * time.Millisecond)
	c.progress("d-1", j, "3 outils (dernier : Grep)") // unchanged
	if got := lines.String(); got != "connect: directive d-1: 2 outils (dernier : Grep)\nconnect: directive d-1: 3 outils (dernier : Grep)\n" {
		t.Errorf("logged %q", got)
	}
}

// The CLI of an implementation ends before its directive does: the push and
// the outputs keep their time.
func TestCLIDeadline(t *testing.T) {
	now := time.Now()
	ctx, cancel := context.WithDeadline(context.Background(), now.Add(2*time.Hour))
	defer cancel()
	if got := cliDeadline(ctx, now); !got.Equal(now.Add(2*time.Hour - pushReserve)) {
		t.Errorf("long: %s", got.Sub(now))
	}
	short, cancel2 := context.WithDeadline(context.Background(), now.Add(10*time.Minute))
	defer cancel2()
	if got := cliDeadline(short, now); !got.Equal(now.Add(5 * time.Minute)) {
		t.Errorf("short: %s", got.Sub(now))
	}
	if got := cliDeadline(context.Background(), now); got.Sub(now) < 24*time.Hour {
		t.Errorf("none: %s", got.Sub(now))
	}
}
