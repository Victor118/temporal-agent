package main

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/subproc"
)

// A manual run starts the CLI as the worker would: as RUN_AS_UID, and never
// as root.
func TestNewDebugRunner_RunsAsTheWorkerWould(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("only a process running as root refuses, or switches users")
	}
	if _, err := newDebugRunner(&config.Config{}, ""); !errors.Is(err, subproc.ErrRootWithoutIdentity) {
		t.Errorf("as root without RUN_AS_UID: %v, want a refusal", err)
	}
	if _, err := newDebugRunner(&config.Config{RunAsUID: "agent-run"}, ""); err == nil {
		t.Error("an invalid RUN_AS_UID was accepted")
	}

	r, err := newDebugRunner(&config.Config{RunAsUID: "40001"}, "/bin/claude")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(filepath.Join(os.TempDir(), "run-as-40001")) })
	if r.RunAs == nil || r.RunAs.UID != 40001 || r.Binary != "/bin/claude" {
		t.Errorf("runner = %+v, want the CLI run as 40001", r)
	}
}

// Run by hand on a development machine — no CLAUDE_CONFIG_DIR, no user to
// switch to — the CLI keeps its default configuration and the login in it;
// with either, it gets a copy of its own, removed once done.
func TestDebugConfigDir(t *testing.T) {
	dir, done, err := debugConfigDir("", nil, nil, false)
	if err != nil || dir != "" {
		t.Errorf("no base, no identity: dir %q, %v, want the CLI's default", dir, err)
	}
	done()

	base := t.TempDir()
	os.WriteFile(filepath.Join(base, "settings.json"), []byte(`{}`), 0o600)
	dir, done, err = debugConfigDir(base, nil, nil, false)
	if err != nil || dir == "" {
		t.Fatalf("with a base: dir %q, %v", dir, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "settings.json")); err != nil {
		t.Errorf("the copy lacks the operator's settings: %v", err)
	}
	done()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the copy outlived the run: %v", err)
	}
}

// A manual run in a worker's container is bounded as the worker's runs are,
// unless a flag says otherwise; a mistyped cap stops it as it stops the worker.
func TestDebugRunLimits(t *testing.T) {
	cfg := &config.Config{ClaudeCodeModel: "sonnet", ClaudeCodeMaxBudgetUSD: "2"}
	unset := func(string) bool { return false }
	if model, budget, err := debugRunLimits(unset, cfg); err != nil || model != "sonnet" || budget != 2 {
		t.Errorf("defaults: %q, %g, %v; want the worker's", model, budget, err)
	}
	if _, _, err := debugRunLimits(unset, &config.Config{ClaudeCodeMaxBudgetUSD: "2$"}); err == nil {
		t.Error("a mistyped cap was accepted")
	}

	claudeCodeFlags.model, claudeCodeFlags.maxBudgetUSD = "opus", 10
	t.Cleanup(func() { claudeCodeFlags.model, claudeCodeFlags.maxBudgetUSD = "", 0 })
	set := func(string) bool { return true }
	if model, budget, err := debugRunLimits(set, cfg); err != nil || model != "opus" || budget != 10 {
		t.Errorf("flags: %q, %g, %v; want them over the worker's", model, budget, err)
	}
}
