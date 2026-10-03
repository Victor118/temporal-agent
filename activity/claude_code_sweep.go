package activity

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/victor/temporal-agent/subproc"
)

// RunWorkspacePrefix starts the name of every run's workspace under Root,
// which the workflows choose: the sweep deletes only what bears it.
const RunWorkspacePrefix = "run-"

// rootClaimFile is held, flock(2), by every live worker process using Root.
const rootClaimFile = ".workers.lock"

// SweepWorkspaces deletes what runs left under Root when their worker
// stopped mid-run, and returns release, to call when this process stops.
//
// A worker that dies leaves its clone behind, and no other worker can reach
// it: the clone is on this machine's disk. Root is normally this process's
// alone (a volume per container): everything there at startup belongs to a
// run that is over, and goes. In case another live process shares Root —
// replicas on one volume — each holds a shared lock on a file in it for as
// long as it runs: a process that cannot take that lock exclusively is not
// alone, and deletes only what is older than lifetime, past which no run's
// workspace is still in use (workflow.RunWorkspaceLifetime). flock(2) holds
// across containers sharing a local volume; on a network filesystem it
// depends on the filesystem.
//
// Only entries named after a run (RunWorkspacePrefix), and their companions
// (the CLI's configuration, the copy of the git configuration), directly
// under Root, are touched: a directory is taken back (subproc.Reclaim) then
// removed without following a link (os.RemoveAll removes a link, not its
// target); anything else is removed as an entry. A failure is reported and
// the sweep goes on.
func (a *ClaudeCodeActivities) SweepWorkspaces(lifetime time.Duration) (removed []string, release func(), err error) {
	release = func() {}
	if a.Root == "" {
		return nil, release, fmt.Errorf("claude code: workspace root is not configured")
	}
	if err := os.MkdirAll(a.Root, 0o755); err != nil {
		return nil, release, err
	}
	claim, alone, err := claimRoot(filepath.Join(a.Root, rootClaimFile))
	if err != nil {
		return nil, release, fmt.Errorf("claim %s: %w", a.Root, err)
	}
	release = func() { claim.Close() }

	var keepAfter time.Time
	if !alone {
		keepAfter = time.Now().Add(-lifetime)
	}
	removed, err = a.sweep(keepAfter, alone)

	// From here on, this process shares Root: a newcomer is not alone. Its
	// own runs start only once this returns, with the shared lock held.
	if lerr := syscall.Flock(int(claim.Fd()), syscall.LOCK_SH); lerr != nil {
		err = errors.Join(err, fmt.Errorf("claim %s: %w", a.Root, lerr))
	}
	return removed, release, err
}

// claimRoot opens the claim file and tells whether this process is the only
// live one using Root: it then holds the lock exclusively. Otherwise it
// holds it shared, once a sweeping newcomer is done.
func claimRoot(path string) (f *os.File, alone bool, err error) {
	f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, false, err
	}
	fd := int(f.Fd())
	switch err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); {
	case err == nil:
		return f, true, nil
	case errors.Is(err, syscall.EWOULDBLOCK):
		if err := syscall.Flock(fd, syscall.LOCK_SH); err != nil {
			f.Close()
			return nil, false, err
		}
		return f, false, nil
	default:
		f.Close()
		return nil, false, err
	}
}

// sweep removes the runs' entries of Root not modified after keepAfter (zero:
// all of them). Alone, it first ends what a run left running as RunAs
// (Runs.KillStrays): nothing of this process runs yet, and a stray could
// still write in a tree being removed.
func (a *ClaudeCodeActivities) sweep(keepAfter time.Time, alone bool) (removed []string, err error) {
	entries, err := os.ReadDir(a.Root)
	if err != nil {
		return nil, err
	}
	var stale []string
	for _, e := range entries {
		if !isRunEntry(e.Name()) {
			continue
		}
		path, perr := a.workspacePath(e.Name())
		if perr != nil {
			continue
		}
		fi, ierr := os.Lstat(path)
		if ierr != nil {
			continue // gone meanwhile
		}
		if !keepAfter.IsZero() && fi.ModTime().After(keepAfter) {
			continue
		}
		stale = append(stale, path)
	}
	if len(stale) > 0 && alone && a.Runs != nil {
		a.Runs.KillStrays()
	}
	var errs []error
	for _, path := range stale {
		if rerr := removeEntry(path); rerr != nil {
			errs = append(errs, rerr)
			continue
		}
		removed = append(removed, path)
	}
	return removed, errors.Join(errs...)
}

// isRunEntry tells whether name is a run's workspace or one of its
// companions (cliConfigDir, gitConfigCopy).
func isRunEntry(name string) bool {
	base := strings.TrimSuffix(strings.TrimSuffix(name, ".claude"), ".gitconfig")
	return strings.HasPrefix(base, RunWorkspacePrefix) && len(base) > len(RunWorkspacePrefix)
}

// removeEntry removes one entry of Root: a directory once taken back from
// RunAs, so that nothing left running as it adds to it meanwhile; anything
// else, a link included, as the entry itself.
func removeEntry(path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		if err := subproc.Reclaim(path); err != nil {
			return err
		}
		return os.RemoveAll(path)
	}
	return os.Remove(path)
}
