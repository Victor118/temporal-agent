package tool

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/subproc"
	"github.com/victor/temporal-agent/subproc/subproctest"
)

// setupExecWorkspace registers exec the way a worker does: as root (the
// agent's container), commands run as nobody, in a workspace given to it.
func setupExecWorkspace(t *testing.T) (string, *Registry) {
	t.Helper()
	id := subproctest.Identity(t)
	dir := subproctest.Dir(t, id)
	r := NewRegistry()
	RegisterExecTool(r, dir, id, subproc.NewRuns(id))
	return dir, r
}

func execExec(t *testing.T, r *Registry, params interface{}) (string, error) {
	t.Helper()
	input, _ := json.Marshal(params)
	return r.Execute(context.Background(), "exec", input)
}

func TestExec_SimpleCommand(t *testing.T) {
	_, r := setupExecWorkspace(t)

	result, err := execExec(t, r, map[string]interface{}{
		"command": "echo hello",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "hello" {
		t.Errorf("got %q, want %q", result, "hello")
	}
}

func TestExec_WorkingDirectory(t *testing.T) {
	dir, r := setupExecWorkspace(t)
	os.WriteFile(filepath.Join(dir, "test.txt"), []byte("content"), 0644)

	result, err := execExec(t, r, map[string]interface{}{
		"command": "ls test.txt",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "test.txt" {
		t.Errorf("got %q, want %q", result, "test.txt")
	}
}

func TestExec_FailedCommand(t *testing.T) {
	_, r := setupExecWorkspace(t)

	result, err := execExec(t, r, map[string]interface{}{
		"command": "false",
	})
	if err != nil {
		t.Fatal(err) // should not return error, but include failure in result
	}
	if !strings.Contains(result, "Command failed") {
		t.Errorf("expected failure message: %s", result)
	}
}

func TestExec_Timeout(t *testing.T) {
	_, r := setupExecWorkspace(t)

	_, err := execExec(t, r, map[string]interface{}{
		"command":         "sleep 10",
		"timeout_seconds": 1,
	})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("expected timeout message: %s", err)
	}
}

func TestExec_DefaultTimeout(t *testing.T) {
	_, r := setupExecWorkspace(t)

	// Timeout of 0 should default to 30s, not panic
	result, err := execExec(t, r, map[string]interface{}{
		"command":         "echo ok",
		"timeout_seconds": 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "ok" {
		t.Errorf("got %q", result)
	}
}

func TestExec_MultilineOutput(t *testing.T) {
	_, r := setupExecWorkspace(t)

	result, err := execExec(t, r, map[string]interface{}{
		"command": "printf 'line1\nline2\nline3'",
	})
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(result, "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 lines, got %d: %q", len(lines), result)
	}
}

// The worker's credentials stay out of a command the model chose.
func TestExec_DoesNotInheritSecrets(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://agent:secret@db/agent")
	t.Setenv("LLM_API_KEY", "sk-secret")
	_, r := setupExecWorkspace(t)

	result, err := execExec(t, r, map[string]interface{}{
		"command": `echo "db=$DATABASE_URL key=$LLM_API_KEY path=${PATH:+set}"`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != "db= key= path=set" {
		t.Errorf("got %q", result)
	}

	// Nor can the command read them back from the worker's own environment.
	result, err = execExec(t, r, map[string]interface{}{
		"command": "cat /proc/" + strconv.Itoa(os.Getpid()) + "/environ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(result, "sk-secret") {
		t.Error("the command read the worker's environment from /proc")
	}
}

// A worker running as root without an identity to run commands as refuses
// them all: they would run as root.
func TestExec_RefusesToRunAsRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("only a worker running as root refuses")
	}
	r := NewRegistry()
	RegisterExecTool(r, t.TempDir(), nil, nil)
	_, err := execExec(t, r, map[string]interface{}{"command": "touch /tmp/ran-as-root"})
	if !errors.Is(err, subproc.ErrRootWithoutIdentity) {
		t.Errorf("err = %v, want a refusal", err)
	}
}

// The command runs as the identity, its Go caches its own.
func TestExec_RunsAsTheIdentity(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("switching users takes root")
	}
	_, r := setupExecWorkspace(t)
	result, err := execExec(t, r, map[string]interface{}{"command": `echo "$(id -u) $GOPATH"`})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(result, strconv.Itoa(int(subproctest.UID))+" ") || strings.HasSuffix(result, " /go") {
		t.Errorf("got %q, want nobody with a GOPATH of its own", result)
	}
}

// A command past its timeout takes its children with it.
func TestExec_TimeoutKillsChildren(t *testing.T) {
	dir, r := setupExecWorkspace(t)

	_, err := execExec(t, r, map[string]interface{}{
		"command":         "sleep 60 & echo $! > child.pid; wait",
		"timeout_seconds": 1,
	})
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("err = %v, want a timeout", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "child.pid"))
	if err != nil {
		t.Fatal(err)
	}
	pid := strings.TrimSpace(string(raw))
	deadline := time.Now().Add(2 * time.Second)
	for {
		stat, err := os.ReadFile("/proc/" + pid + "/stat")
		// Gone, or a zombie nobody reaps: either way it no longer runs.
		if err != nil || strings.Contains(string(stat), ") Z ") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("child %s still runs after the timeout", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A command that returns leaves nothing running behind it as its user: not
// what it started in the background, which held its output or did not, nor
// what left its process group.
func TestExec_LeavesNothingRunning(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("switching users takes root")
	}
	_, r := setupExecWorkspace(t)
	for _, command := range []string{
		"sleep 300 & echo started",
		"sleep 300 >/dev/null 2>&1 & echo started",
		"setsid sleep 300 >/dev/null 2>&1 < /dev/null & echo started",
	} {
		result, err := execExec(t, r, map[string]interface{}{"command": command})
		if err != nil || !strings.HasPrefix(result, "started") || strings.Contains(result, "failed") {
			t.Errorf("%s: %q, %v", command, result, err)
		}
		subproctest.NoProcessLeft(t, subproctest.UID)
	}
}

// Commands run as another user without a count of them are refused: what one
// left running would never be ended.
func TestExec_RefusesAnIdentityWithoutRuns(t *testing.T) {
	r := NewRegistry()
	me := uint32(os.Geteuid())
	RegisterExecTool(r, t.TempDir(), &subproc.Identity{UID: me, GID: me, Home: t.TempDir()}, nil)
	if _, err := execExec(t, r, map[string]interface{}{"command": "true"}); err == nil || !strings.Contains(err.Error(), "subproc.Runs") {
		t.Errorf("err = %v, want a refusal", err)
	}
}
