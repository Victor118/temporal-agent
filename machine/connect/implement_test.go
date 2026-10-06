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
	for _, want := range []string{"--permission-mode acceptEdits", "Bash(git commit:*)", "--disallowedTools Bash(git push:*)",
		"--max-budget-usd 1 ", "--add-dir ", "/outputs/**)", "--setting-sources user", "--strict-mcp-config", "Commit your work on that branch"} {
		if !strings.Contains(string(args), want) {
			t.Errorf("args lack %q: %s", want, args)
		}
	}
	if progresses[0] != machine.CloneProgress || progresses[len(progresses)-1] != machine.PushProgress {
		t.Errorf("progresses %v", progresses)
	}
	if entries, _ := os.ReadDir(a.WorkDir); len(entries) != 0 {
		t.Errorf("clone left behind: %v", entries)
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
			if out.Pushed || !strings.Contains(out.Error, c.says) || !strings.Contains(out.Error, "nothing was pushed") {
				t.Errorf("output %+v", out)
			}
			if got := remoteBranch(t, remote, "agent/health-1234abcd"); got != "" {
				t.Errorf("pushed %s", got)
			}
		})
	}
}

// A push the remote refuses is said, the commits listed.
func TestCoder_ImplementPushRefused(t *testing.T) {
	a, remote, _, _ := newImplementer(t, commitScript(""))
	// A hook of the remote's refuses every push.
	hook := filepath.Join(remote, "hooks", "pre-receive")
	os.WriteFile(hook, []byte("#!/bin/sh\necho refused by policy >&2\nexit 1\n"), 0o755)
	raw, err := a.Implement(context.Background(), implementInput(remote), func(string) {})
	var out machine.CodingOutput
	json.Unmarshal(raw, &out)
	if err != nil || out.Pushed || len(out.Commits) != 1 || !strings.Contains(out.Error, "were not pushed") || !strings.Contains(out.Error, "refused by policy") {
		t.Errorf("output %+v %v", out, err)
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
