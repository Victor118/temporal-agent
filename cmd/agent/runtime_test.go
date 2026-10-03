package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/victor/temporal-agent/activity"
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

// The CLI's configuration stays the worker's: each run works on a copy of
// its own. One an earlier version gave away is taken back.
func TestPrepareRunAs_KeepsTheCLIConfiguration(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("giving files away takes root")
	}
	root := t.TempDir()
	cfg := &config.Config{WorkspacePath: filepath.Join(root, "workspace"), ClaudeConfigDir: filepath.Join(root, "config")}
	os.MkdirAll(cfg.ClaudeConfigDir, 0o777)
	os.WriteFile(filepath.Join(cfg.ClaudeConfigDir, "settings.json"), []byte("{}"), 0o666)
	id := &subproc.Identity{UID: 65534, GID: 65534, Home: filepath.Join(root, "home")}
	if err := id.Give(cfg.ClaudeConfigDir); err != nil {
		t.Fatal(err)
	}

	if err := prepareRunAs(cfg, id); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{cfg.ClaudeConfigDir, filepath.Join(cfg.ClaudeConfigDir, "settings.json")} {
		fi, err := os.Lstat(path)
		if err != nil {
			t.Fatal(err)
		}
		if uid := fi.Sys().(*syscall.Stat_t).Uid; uid != 0 || fi.Mode().Perm()&0o022 != 0 {
			t.Errorf("%s: uid %d, mode %v; want the worker's alone", path, uid, fi.Mode())
		}
	}
}

// A cap the operator mistyped stops the worker: read as no cap, it would let
// every run spend without limit.
func TestParseBudget(t *testing.T) {
	for raw, want := range map[string]float64{"": 0, "1": 1, "2.5": 2.5} {
		if got, err := parseBudget(raw); err != nil || got != want {
			t.Errorf("parseBudget(%q) = %g, %v; want %g", raw, got, err, want)
		}
	}
	for _, raw := range []string{"1$", "abc", "0", "-3", "NaN", "Inf", "1,5"} {
		if _, err := parseBudget(raw); err == nil {
			t.Errorf("parseBudget(%q) accepted", raw)
		}
	}
}

// A context bound the operator mistyped stops the worker: read as the
// default, it would hide the mistake.
func TestParseContextBytes(t *testing.T) {
	for raw, want := range map[string]int{"": 0, "400000": 400000} {
		if got, err := parseContextBytes(raw); err != nil || got != want {
			t.Errorf("parseContextBytes(%q) = %d, %v; want %d", raw, got, err, want)
		}
	}
	for _, raw := range []string{"2MB", "0", "-1", "1.5"} {
		if _, err := parseContextBytes(raw); err == nil {
			t.Errorf("parseContextBytes(%q) accepted", raw)
		}
	}
}

// A limit the operator mistyped stops the worker: read as zero, the SDK
// would take a thousand runs at once.
func TestParseMaxRuns(t *testing.T) {
	for raw, want := range map[string]int{"": 1, "1": 1} {
		if got, err := parseMaxRuns(raw); err != nil || got != want {
			t.Errorf("parseMaxRuns(%q) = %d, %v; want %d", raw, got, err, want)
		}
	}
	for _, raw := range []string{"0", "-1", "two", "1.5", " 2"} {
		if _, err := parseMaxRuns(raw); err == nil {
			t.Errorf("parseMaxRuns(%q) accepted", raw)
		}
	}
	// More than one run at a time shares the one RunAs uid: refused, and
	// the worker says why.
	for _, raw := range []string{"2", "4"} {
		_, err := parseMaxRuns(raw)
		if err == nil || !strings.Contains(err.Error(), "one uid per run slot") {
			t.Errorf("parseMaxRuns(%q) = %v, want a refusal that names the uid per run", raw, err)
		}
	}
}

// A root this worker cannot claim stops it: serving runs without a claim, it
// would see its clones deleted by the next worker to start alone.
func TestClaimRunsRoot_FailureIsFatal(t *testing.T) {
	root := t.TempDir()
	// The claim file is never opened through a link.
	if err := os.Symlink(filepath.Join(t.TempDir(), "elsewhere"), filepath.Join(root, ".workers.lock")); err != nil {
		t.Fatal(err)
	}
	if release, err := claimRunsRoot(&activity.ClaudeCodeActivities{Root: root}, time.Second); err == nil {
		release()
		t.Fatal("claimed a root whose claim file is a link")
	}

	ok := t.TempDir()
	release, err := claimRunsRoot(&activity.ClaudeCodeActivities{Root: ok}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	release()
}

// A coding run's wait for a free worker: a Go duration, empty the default;
// anything else stops the worker rather than leave runs waiting for ever.
func TestParseQueueWait(t *testing.T) {
	for raw, want := range map[string]time.Duration{"": activity.DefaultRunQueueWait, "45m": 45 * time.Minute, "1h": time.Hour} {
		if got, err := parseQueueWait(raw); err != nil || got != want {
			t.Errorf("parseQueueWait(%q) = %s, %v; want %s", raw, got, err, want)
		}
	}
	for _, raw := range []string{"0", "0s", "-5m", "30", "soon"} {
		if _, err := parseQueueWait(raw); err == nil {
			t.Errorf("parseQueueWait(%q) accepted", raw)
		}
	}
}
