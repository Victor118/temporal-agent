//go:build unix

package connect

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/claudecode"
	"github.com/victor/temporal-agent/machine"
)

// fakeClaude writes a stand-in for the claude CLI: it keeps its arguments
// and environment in dir, then plays script.
func fakeClaude(t *testing.T, dir, script string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "claude")
	body := "#!/bin/sh\necho \"$@\" > " + filepath.Join(dir, "args") + "\nenv > " + filepath.Join(dir, "env") + "\ncat >/dev/null\n" + script
	if err := os.WriteFile(bin, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

const analyzeStream = `cat <<'EOF'
{"type":"system","subtype":"init","session_id":"s","apiKeySource":"none"}
{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Grep","input":{}}]},"session_id":"s"}
{"type":"result","subtype":"success","is_error":false,"result":"The handler is in main.go.","session_id":"s","num_turns":2,"duration_ms":1500,"total_cost_usd":0.03}
EOF
`

// gitRepo makes a repository with one commit, and returns its path.
func gitRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	for _, args := range [][]string{
		{"init", "--quiet", "-b", "main"},
		{"-c", "user.email=a@b", "-c", "user.name=a", "commit", "--quiet", "--allow-empty", "-m", "first"},
	} {
		if out, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	return repo
}

func newAnalyzer(t *testing.T, script string) (*Analyzer, string, string) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git")
	}
	seen := t.TempDir()
	home := t.TempDir()
	os.MkdirAll(filepath.Join(home, ".claude"), 0o700)
	os.WriteFile(filepath.Join(home, ".claude", ".credentials.json"), []byte(`{"claudeAiOauth":{"accessToken":"a"}}`), 0o600)
	repo := gitRepo(t)
	a := &Analyzer{Runner: claudecode.Runner{Binary: fakeClaude(t, seen, script)}, Auth: claudecode.AuthSubscription,
		Repos: []string{repo}, MaxBudgetUSD: 2, WorkDir: t.TempDir(), Home: home, ProgressEvery: 100 * time.Millisecond}
	return a, repo, seen
}

func input(repo, ref string) json.RawMessage {
	raw, _ := json.Marshal(machine.AnalyzeInput{Repo: repo, Ref: ref, Task: "Where is the handler?"})
	return raw
}

func TestAnalyzer_Run(t *testing.T) {
	// A key in the owner's shell never reaches a subscription run.
	t.Setenv("ANTHROPIC_API_KEY", "sk-should-not-leak")
	a, repo, seen := newAnalyzer(t, analyzeStream)
	if a.Login() != claudecode.LoginOK {
		t.Fatalf("login %s", a.Login())
	}
	var progresses []string
	raw, err := a.Run(context.Background(), input(repo, "main"), func(p string) { progresses = append(progresses, p) })
	var out machine.CodingOutput
	if err != nil || json.Unmarshal(raw, &out) != nil {
		t.Fatalf("run: %s %v", raw, err)
	}
	if out.Report != "The handler is in main.go." || len(out.Commit) != 40 || out.NumTurns != 2 || out.CostUSD != 0.03 ||
		out.PaidBy != "subscription" || out.Interrupted {
		t.Errorf("output %+v", out)
	}
	args, _ := os.ReadFile(filepath.Join(seen, "args"))
	for _, want := range []string{"--permission-mode plan", "--max-budget-usd 2", "--no-session-persistence", "read-only analysis", "--setting-sources user", "--strict-mcp-config"} {
		if !strings.Contains(string(args), want) {
			t.Errorf("args lack %q: %s", want, args)
		}
	}
	env, _ := os.ReadFile(filepath.Join(seen, "env"))
	if strings.Contains(string(env), "sk-should-not-leak") || strings.Contains(string(env), "CLAUDE_CONFIG_DIR") {
		t.Errorf("the CLI's environment: %s", env)
	}
	if len(progresses) == 0 || progresses[0] != "clone" {
		t.Errorf("progresses %v", progresses)
	}
	if entries, _ := os.ReadDir(a.WorkDir); len(entries) != 0 {
		t.Errorf("clone left behind: %v", entries)
	}

	var refusal *Refusal
	if _, err := a.Run(context.Background(), input("/elsewhere", ""), func(string) {}); !errors.As(err, &refusal) || !strings.Contains(err.Error(), "--repos") {
		t.Errorf("a repository not allowed: %v", err)
	}
	if _, err := a.Run(context.Background(), input("--upload-pack=x", ""), func(string) {}); err == nil {
		t.Error("a repository read as an option")
	}
	if _, err := a.Run(context.Background(), input(repo, "nope"), func(string) {}); err == nil || !strings.Contains(err.Error(), "unknown ref") {
		t.Errorf("unknown ref: %v", err)
	}
}

func TestAnalyzer_CancelEndsTheCLI(t *testing.T) {
	a, repo, _ := newAnalyzer(t, `echo '{"type":"system","subtype":"init","session_id":"s"}'
echo '{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Read","input":{}}]},"session_id":"s"}'
sleep 60
`)
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() { time.Sleep(500 * time.Millisecond); cancel(errCanceled) }()
	start := time.Now()
	raw, err := a.Run(ctx, input(repo, ""), func(string) {})
	var out machine.CodingOutput
	json.Unmarshal(raw, &out)
	if err == nil || !out.Interrupted || out.ToolCalls != 1 || out.LastTool != "Read" {
		t.Errorf("cancelled: %+v %v", out, err)
	}
	if took := time.Since(start); took > 15*time.Second {
		t.Errorf("took %s", took)
	}
}

func TestAnalyzer_LoginRefusedThenBack(t *testing.T) {
	a, repo, _ := newAnalyzer(t, `cat <<'EOF'
{"type":"system","subtype":"init","session_id":"s"}
{"type":"result","subtype":"success","is_error":true,"result":"Invalid API key · Please run /login","session_id":"s"}
EOF
`)
	told := 0
	a.OnLoginRefused = func() { told++ }
	_, err := a.Run(context.Background(), input(repo, ""), func(string) {})
	var refusal *Refusal
	if !errors.As(err, &refusal) || told != 1 || a.Login() != claudecode.LoginNone {
		t.Fatalf("refused before any tool: %v, told %d, login %s", err, told, a.Login())
	}
	// Refused at once now, without running the CLI.
	if _, err := a.Run(context.Background(), input(repo, ""), func(string) {}); !errors.As(err, &refusal) || told != 1 {
		t.Errorf("logged out: %v", err)
	}
	// A new /login rewrites the file: announced again (past the cache).
	later := time.Now().Add(time.Minute)
	os.Chtimes(filepath.Join(a.Home, ".claude", ".credentials.json"), later, later)
	time.Sleep(loginCacheFor)
	if a.Login() != claudecode.LoginOK {
		t.Errorf("after a new login: %s", a.Login())
	}

	// Nothing to tell a new login apart (a token): tried again after
	// RetryAfter, or at the next connection.
	a.loginRefused()
	a.Environ, a.RetryAfter = []string{claudecode.OAuthTokenEnv + "=t"}, 300*time.Millisecond
	if a.Login() != claudecode.LoginNone {
		t.Error("refused token announced")
	}
	time.Sleep(loginCacheFor + 300*time.Millisecond)
	if a.Login() != claudecode.LoginOK {
		t.Error("not tried again after RetryAfter")
	}
	a.loginRefused()
	a.Retry()
	if a.Login() != claudecode.LoginOK {
		t.Error("not tried again at a new connection")
	}

	a.Runner.Binary = filepath.Join(t.TempDir(), "no-claude")
	a.Retry() // past the cache
	if a.Login() != claudecode.LoginAbsent {
		t.Errorf("no CLI: %s", a.Login())
	}
	a.Runner.Binary = ""
	a.Auth = claudecode.AuthAPI
	if _, ok := claudecode.FindLogin(a.Auth, nil, a.Home); ok {
		t.Error("api without a key")
	}
}

// A clone never waits on a prompt: a repository that asks for credentials
// fails at once, saying what to do; ext:: and plaintext transports are
// refused.
func TestAnalyzer_CloneNeverPrompts(t *testing.T) {
	a, _, _ := newAnalyzer(t, analyzeStream)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	// The owner's global git configuration is read: here, one that trusts
	// the test server's certificate.
	home := t.TempDir()
	os.WriteFile(filepath.Join(home, ".gitconfig"), []byte("[http]\n\tsslVerify = false\n"), 0o644)
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", "")
	private := srv.URL + "/me/app.git"
	marker := filepath.Join(t.TempDir(), "ran")
	refused := []string{"ext::sh -c touch% " + marker, "http://127.0.0.1:1/x.git", "git://127.0.0.1:1/x.git"}
	a.Repos = append([]string{private}, refused...)
	start := time.Now()
	_, err := a.Run(context.Background(), input(private, ""), func(string) {})
	if err == nil || !strings.Contains(err.Error(), "asks for credentials") {
		t.Errorf("credentials asked: %v", err)
	}
	if took := time.Since(start); took > 20*time.Second {
		t.Errorf("waited %s", took)
	}
	for _, repo := range refused {
		if _, err := a.Run(context.Background(), input(repo, ""), func(string) {}); err == nil || !strings.Contains(err.Error(), "not allowed") {
			t.Errorf("%s: %v", repo, err)
		}
	}
	if _, err := os.Stat(marker); err == nil {
		t.Error("ext:: ran a command")
	}
}
