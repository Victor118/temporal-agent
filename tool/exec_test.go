package tool

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func setupExecWorkspace(t *testing.T) (string, *Registry) {
	t.Helper()
	dir := t.TempDir()
	r := NewRegistry()
	RegisterExecTool(r, dir)
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
