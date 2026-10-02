package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/subproc"
)

// At startup, the user exec runs as gets its home and the workspace, existing
// files included: what the file tools wrote as root before must stay
// writable by exec.
func TestPrepareRunAs_GivesTheWorkspace(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("giving files away takes root")
	}
	root := t.TempDir()
	cfg := &config.Config{WorkspacePath: filepath.Join(root, "workspace")}
	os.MkdirAll(cfg.WorkspacePath, 0o755)
	os.WriteFile(filepath.Join(cfg.WorkspacePath, "old.txt"), []byte("x"), 0o644)
	id := &subproc.Identity{UID: 65534, GID: 65534, Home: filepath.Join(root, "home")}

	if err := prepareRunAs(cfg, id); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{id.Home, cfg.WorkspacePath, filepath.Join(cfg.WorkspacePath, "old.txt")} {
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if uid := fi.Sys().(*syscall.Stat_t).Uid; uid != 65534 {
			t.Errorf("%s belongs to %d", path, uid)
		}
	}

	// Without an identity, a worker running as root prepares nothing and
	// still starts: exec refuses its commands one by one.
	if err := prepareRunAs(&config.Config{WorkspacePath: filepath.Join(root, "other")}, nil); err != nil {
		t.Errorf("no identity: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "other")); err == nil {
		t.Error("prepared a workspace with no identity to give it to")
	}
}
