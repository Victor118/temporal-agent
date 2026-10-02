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
