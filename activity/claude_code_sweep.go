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
	a  *ClaudeCodeActivities
	f  *os.File
	fd int
	// alone: the lock is held exclusively, no other live process uses Root.
	alone bool
	// held: the lock is held. Not held = another process kept it exclusive
	// (its own sweep) past the wait: this one takes it, shared, as soon as
	// that process lets it go (claimLater).
	held bool
}

// ClaimRoot claims Root for this process: exclusively when no other live
// process holds a claim, else shared. A process sweeping Root holds it
// exclusively: this one waits for it at most wait, then goes on without
// holding the claim yet (Held). Any other failure is an error: a process
// that serves runs without a claim could see its clones deleted by the next
// worker to start, which would think itself alone.
func (a *ClaudeCodeActivities) ClaimRoot(wait time.Duration) (*RootClaim, error) {
	if a.Root == "" {
		return nil, fmt.Errorf("claude code: workspace root is not configured")
	}
	if err := os.MkdirAll(a.Root, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(a.Root, rootClaimFile), os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	c := &RootClaim{a: a, f: f, fd: int(f.Fd())}
	switch err := syscall.Flock(c.fd, syscall.LOCK_EX|syscall.LOCK_NB); {
	case err == nil:
		c.alone, c.held = true, true
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
// it exclusively; past that, it is taken in the background (claimLater).
func (c *RootClaim) share(wait time.Duration) error {
	c.alone, c.held = false, false
	deadline := time.Now().Add(wait)
	for logged := false; ; logged = true {
		err := syscall.Flock(c.fd, syscall.LOCK_SH|syscall.LOCK_NB)
		if err == nil {
			c.held = true
			return nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			return err
		}
		if !time.Now().Before(deadline) {
			c.claimLater()
			return nil
		}
		if !logged {
			log.Printf("Another worker process holds %s to itself (it sweeps it): waiting up to %s", c.a.Root, wait)
		}
		time.Sleep(min(claimPoll, time.Until(deadline)))
	}
}

// claimPoll is how often share tries the lock again.
const claimPoll = 500 * time.Millisecond

// claimLater takes the lock shared once the process holding it exclusively
// lets it go, for as long as this one runs.
func (c *RootClaim) claimLater() {
	log.Printf("Warning: %s is still held by another worker process: no sweep of it this time; "+
		"it is claimed as soon as that process lets it go", c.a.Root)
	go func() {
		if err := syscall.Flock(c.fd, syscall.LOCK_SH); err != nil {
			log.Printf("Warning: claim %s: %v", c.a.Root, err)
			return
		}
		log.Printf("Claimed %s, shared with the other worker processes", c.a.Root)
	}()
}

// Alone tells whether no other live process uses Root.
func (c *RootClaim) Alone() bool { return c.alone }

// Held tells whether the claim was held when ClaimRoot returned. Not held,
// Sweep refuses: another process is sweeping Root.
func (c *RootClaim) Held() bool { return c.held }

// Sweep deletes what runs left under Root when their worker stopped mid-run:
// alone, every run's entry; else only those not modified within lifetime,
// past which no run's workspace is still in use
// (workflow.RunWorkspaceLifetime).
//
// Only entries named after a run (RunWorkspacePrefix), and their companions
// (the CLI's configuration, the copy of the git configuration), directly
// under Root, are touched: a directory is taken back (subproc.Reclaim) then
// removed without following a link (os.RemoveAll removes a link, not its
// target); anything else is removed as an entry. A failure is reported and
// the sweep goes on.
func (c *RootClaim) Sweep(lifetime time.Duration) (removed []string, err error) {
	if !c.held {
		return nil, fmt.Errorf("claim on %s not held: another worker process is sweeping it", c.a.Root)
	}
	var keepAfter time.Time
	if !c.alone {
		keepAfter = time.Now().Add(-lifetime)
	}
	return c.a.sweep(keepAfter, c.alone)
}

// Share turns an exclusive claim into a shared one, once its sweep is done:
// from then on a newcomer is not alone. The runs of this process start only
// after it returns. The conversion is not atomic (flock(2)): a newcomer may
// take the lock exclusively in between, and sweep, before this process has
// any run there; Share then waits for it as ClaimRoot does.
func (c *RootClaim) Share(wait time.Duration) error {
	if !c.alone {
		return nil
	}
	return c.share(wait)
}

// Release gives the claim up: when the process stops.
func (c *RootClaim) Release() { c.f.Close() }

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
