package activity

import (
	"errors"
	"fmt"
	"log"
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

// RootClaim is a worker process's claim on Root, held for as long as it
// runs: a flock(2) on a file there, shared by every live process using Root.
//
// A worker that dies leaves its clone behind, and no other worker can reach
// it: the clone is on this machine's disk. Root is normally this process's
// alone (a volume per container): everything there at startup belongs to a
// run that is over, and goes (Sweep). In case another live process shares
// Root (replicas on one volume), the claim tells: a process that cannot take
// the lock exclusively is not alone, and deletes only what is older than a
// run's lifetime. flock(2) holds across containers sharing a local volume; on
// a network filesystem it depends on the filesystem.
type RootClaim struct {
	root string
	f    *os.File
	fd   int
	// alone: the lock is held exclusively, no other live process uses Root.
	alone bool
}

// ClaimRoot claims root, the coding runs' (ClaudeCodeActivities.Root), for
// this process: exclusively when no other live process holds a claim, else
// shared. A process sweeping root holds it exclusively: this one waits for
// it at most wait, then fails. Every failure is an error, the worker's end:
// a process that served runs without a claim could see its clones deleted
// by the next worker to start, which would think itself alone. Whatever runs
// the worker starts it again; by then the sweep is over.
//
// A function, not a method of ClaudeCodeActivities: every exported method of
// that struct is registered as an activity.
func ClaimRoot(root string, wait time.Duration) (*RootClaim, error) {
	if root == "" {
		return nil, fmt.Errorf("claude code: workspace root is not configured")
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(root, rootClaimFile), os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	c := &RootClaim{root: root, f: f, fd: int(f.Fd())}
	switch err := syscall.Flock(c.fd, syscall.LOCK_EX|syscall.LOCK_NB); {
	case err == nil:
		c.alone = true
		return c, nil
	case !errors.Is(err, syscall.EWOULDBLOCK):
		f.Close()
		return nil, err
	}
	if err := c.share(wait); err != nil {
		f.Close()
		return nil, err
	}
	return c, nil
}

// share takes the lock shared, waiting at most wait for a process that holds
// it exclusively; past that, it fails.
func (c *RootClaim) share(wait time.Duration) error {
	c.alone = false
	deadline := time.Now().Add(wait)
	for logged := false; ; logged = true {
		err := syscall.Flock(c.fd, syscall.LOCK_SH|syscall.LOCK_NB)
		if err == nil {
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return err
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("another worker process still holds %s to itself (it sweeps it) after %s; try again later", c.root, wait)
		}
		if !logged {
			log.Printf("Another worker process holds %s to itself (it sweeps it): waiting up to %s", c.root, wait)
		}
		time.Sleep(min(claimPoll, time.Until(deadline)))
	}
}

// claimPoll is how often share tries the lock again.
const claimPoll = 500 * time.Millisecond

// Alone tells whether no other live process uses Root.
func (c *RootClaim) Alone() bool { return c.alone }

// Sweep deletes what runs left under Root when their worker stopped mid-run:
// alone, every run's entry; else only those not modified within lifetime,
// past which no run's workspace is still in use
// (workflow.RunWorkspaceLifetime). runs is the worker's count of the
// commands run as RunAs (ClaudeCodeActivities.Runs); nil = none.
//
// Only entries named after a run (RunWorkspacePrefix), and their companions
// (the CLI's configuration, the copy of the git configuration, the outputs,
// the plugin), directly under Root, are touched: a directory is taken back
// (subproc.Reclaim) then removed without following a link (os.RemoveAll
// removes a link, not its target); anything else is removed as an entry. A
// failure is reported and the sweep goes on.
func (c *RootClaim) Sweep(lifetime time.Duration, runs RunCounter) (removed []string, err error) {
	var keepAfter time.Time
	if !c.alone {
		keepAfter = time.Now().Add(-lifetime)
	}
	return sweepRoot(c.root, runs, keepAfter, c.alone)
}

// Share turns an exclusive claim into a shared one, once its sweep is done:
// from then on a newcomer is not alone. The runs of this process start only
// after it returns. The conversion is not atomic (flock(2)): a newcomer may
// take the lock exclusively in between, and sweep, before this process has
// any run there; Share then waits for it as ClaimRoot does, and fails as it
// does past wait.
func (c *RootClaim) Share(wait time.Duration) error {
	if !c.alone {
		return nil
	}
	return c.share(wait)
}

// Release gives the claim up: when the process stops.
func (c *RootClaim) Release() { c.f.Close() }

// sweepRoot removes the runs' entries of root not modified after keepAfter
// (zero: all of them). Alone, it first ends what a run left running as RunAs
// (runs.KillStrays): nothing of this process runs yet, and a stray could
// still write in a tree being removed.
func sweepRoot(root string, runs RunCounter, keepAfter time.Time, alone bool) (removed []string, err error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var stale []string
	for _, e := range entries {
		if !isRunEntry(e.Name()) {
			continue
		}
		path, perr := rootPath(root, e.Name())
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
	if len(stale) > 0 && alone && runs != nil {
		runs.KillStrays()
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
// companions (cliConfigDir, gitConfigCopy, outputsDir, pluginDir,
// subproc.GitShimDir).
func isRunEntry(name string) bool {
	base := name
	for _, companion := range []string{".claude", ".gitconfig", ".outputs", ".plugin", ".bin"} {
		base = strings.TrimSuffix(base, companion)
	}
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
