package activity

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/victor/temporal-agent/subproc"
	"github.com/victor/temporal-agent/subproc/subproctest"
)

// countingRuns is a RunCounter that counts the sweeps of strays.
type countingRuns struct{ kills int }

func (r *countingRuns) Hold() func() { return func() {} }
func (r *countingRuns) KillStrays()  { r.kills++ }

// sweepFixture lays out a root as a crashed worker leaves it, with what is
// not a run's next to it.
func sweepFixture(t *testing.T, root string) {
	t.Helper()
	mk := func(path string) {
		if err := os.MkdirAll(filepath.Join(root, path), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path string) {
		if err := os.WriteFile(filepath.Join(root, path), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	mk("run-a/.git")
	write("run-a/.git/config")
	mk("run-a.claude")
	write("run-a.gitconfig")
	mk("run-b/sub")
	mk("other")
	write("notes.txt")
	mk("run-") // no run's name
}

func entries(t *testing.T, root string) []string {
	t.Helper()
	des, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range des {
		names = append(names, d.Name())
	}
	return names
}

// Alone on its root, a starting worker deletes every run's workspace there,
// and its companions: they belong to runs that are over. Nothing else.
func TestSweepWorkspaces_AloneRemovesEveryRun(t *testing.T) {
	root := t.TempDir()
	sweepFixture(t, root)
	runs := &countingRuns{}
	a := &ClaudeCodeActivities{Root: root, Runs: runs}

	removed, release, err := a.SweepWorkspaces(time.Hour)
	defer release()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := entries(t, root), []string{".workers.lock", "notes.txt", "other", "run-"}; !slices.Equal(got, want) {
		t.Errorf("left %v, want %v", got, want)
	}
	if len(removed) != 4 {
		t.Errorf("removed %v, want the four entries of runs a and b", removed)
	}
	if runs.kills != 1 {
		t.Errorf("strays swept %d times, want once before deleting", runs.kills)
	}
}

// A link under root, whatever its name and wherever it points, is removed as
// a link: its target is not the worker's to delete.
func TestSweepWorkspaces_NeverFollowsALink(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	keep := filepath.Join(outside, "keep.txt")
	os.WriteFile(keep, []byte("x"), 0o644)
	os.Symlink(outside, filepath.Join(root, "run-link"))
	os.MkdirAll(filepath.Join(root, "run-c"), 0o755)
	os.Symlink(outside, filepath.Join(root, "run-c", "escape"))

	a := &ClaudeCodeActivities{Root: root}
	_, release, err := a.SweepWorkspaces(time.Hour)
	defer release()
	if err != nil {
		t.Fatal(err)
	}
	if got := entries(t, root); !slices.Equal(got, []string{".workers.lock"}) {
		t.Errorf("left %v", got)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Errorf("the link's target was deleted: %v", err)
	}
}

// Another live worker on the same root (it holds its claim): a starting one
// is not alone, and leaves what may be a live run of the other's — anything
// newer than a run's lifetime. Once the other is gone, it is alone again.
func TestSweepWorkspaces_SharedRootKeepsRecentRuns(t *testing.T) {
	root := t.TempDir()
	first := &ClaudeCodeActivities{Root: root}
	_, releaseFirst, err := first.SweepWorkspaces(time.Hour)
	if err != nil {
		t.Fatal(err)
	}

	sweepFixture(t, root)
	old := time.Now().Add(-2 * time.Hour)
	for _, name := range []string{"run-a", "run-a.claude", "run-a.gitconfig"} {
		if err := os.Chtimes(filepath.Join(root, name), old, old); err != nil {
			t.Fatal(err)
		}
	}

	runs := &countingRuns{}
	second := &ClaudeCodeActivities{Root: root, Runs: runs}
	removed, releaseSecond, err := second.SweepWorkspaces(time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(removed) != 3 {
		t.Errorf("removed %v, want run a's three entries", removed)
	}
	if got := entries(t, root); !slices.Contains(got, "run-b") || slices.Contains(got, "run-a") {
		t.Errorf("left %v, want run-b kept and run-a gone", got)
	}
	if runs.kills != 0 {
		t.Error("a worker that is not alone swept the strays of the run user")
	}

	releaseFirst()
	releaseSecond()
	third := &ClaudeCodeActivities{Root: root}
	_, releaseThird, err := third.SweepWorkspaces(time.Hour)
	defer releaseThird()
	if err != nil {
		t.Fatal(err)
	}
	if got := entries(t, root); slices.Contains(got, "run-b") {
		t.Errorf("left %v: alone again, run-b should be gone", got)
	}
}

func TestSweepWorkspaces_CreatesTheRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "runs")
	_, release, err := (&ClaudeCodeActivities{Root: root}).SweepWorkspaces(time.Hour)
	defer release()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Error(err)
	}

	if _, release, err := (&ClaudeCodeActivities{}).SweepWorkspaces(time.Hour); err == nil {
		release()
		t.Error("swept with no root configured")
	}
}

func TestIsRunEntry(t *testing.T) {
	for name, want := range map[string]bool{
		"run-0e2f":           true,
		"run-0e2f.claude":    true,
		"run-0e2f.gitconfig": true,
		"run-":               false,
		"run-.claude":        false,
		"other":              false,
		".workers.lock":      false,
		"xrun-1":             false,
	} {
		if got := isRunEntry(name); got != want {
			t.Errorf("isRunEntry(%q) = %v, want %v", name, got, want)
		}
	}
}

// A clone is the run user's when its worker dies: the sweep takes it back,
// and removes it, closed directories included.
func TestSweepWorkspaces_TakesTheRunsCloneBack(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("giving files away takes root")
	}
	id := subproctest.Identity(t)
	root := subproctest.Dir(t, nil)
	dir := filepath.Join(root, "run-x")
	os.MkdirAll(filepath.Join(dir, "closed"), 0o755)
	os.WriteFile(filepath.Join(dir, "closed", "f"), []byte("x"), 0o600)
	if err := id.Give(dir); err != nil {
		t.Fatal(err)
	}
	os.Chmod(filepath.Join(dir, "closed"), 0o500)

	_, release, err := (&ClaudeCodeActivities{Root: root, RunAs: id, Runs: subproc.NewRuns(id)}).SweepWorkspaces(time.Hour)
	defer release()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(dir); !os.IsNotExist(err) {
		t.Errorf("run-x left behind: %v", err)
	}
}
