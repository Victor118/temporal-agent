package tool

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
