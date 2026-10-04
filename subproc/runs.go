package subproc

import (
	"context"
	"errors"
	"log"
	"os/exec"
	"sync"
	"time"
)

// sweepTimeout bounds the sweep of an identity's processes (Runs.sweep): it
// runs with every command start as that identity waiting on it.
const sweepTimeout = 5 * time.Second

// Runs counts the commands a worker runs as one identity, and ends every
// process of that identity's once none of them runs: what left a command's
// process group (setsid, a daemon) and would outlive it otherwise.
//
// One per worker process, shared by everything that starts commands as the
// identity or relies on none of its processes running (exec, the coding CLI,
// the activity that takes a clone back): a count kept apart would sweep while
// another's command still runs. It takes the identity to be the worker's
// alone (RUN_AS_UID is a user of its own, as in the images): any process
// running as it in this pid namespace is ended, another worker process's
// commands with the same identity included.
//
// A nil Runs counts and ends nothing; one with no identity counts (Idle)
// and ends nothing.
type Runs struct {
	id *Identity
	// sweep ends every process of id's; replaced by the tests.
	sweep func()

	mu sync.Mutex
	n  int
	// idle is closed while no command runs (n == 0, the strays swept), and
	// replaced by an open one when one starts; nil reads as closed.
	idle chan struct{}
}

// NewRuns returns the count of the commands run as id. A nil id gets a Runs
// that does nothing.
func NewRuns(id *Identity) *Runs {
	r := &Runs{id: id}
	r.sweep = r.killAll
	return r
}

// Hold counts a command about to start as the identity until release is
// called, once it has been waited for. release then ends every process of
// the identity's if no other command runs as it.
func (r *Runs) Hold() (release func()) {
	if r == nil {
		return func() {}
	}
	r.mu.Lock()
	if r.n == 0 {
		r.idle = make(chan struct{})
	}
	r.n++
	r.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.n--; r.n == 0 {
				if r.id != nil {
					r.sweep()
				}
				close(r.idle)
			}
		})
	}
}

// Idle waits until no command runs as the identity, the strays of the last
// one swept, or until ctx is done, and tells which: true when none runs. It
// is how a step that must not run beside a command — one a run was given up
// on, which its worker has yet to end — waits for it to be gone. A nil Runs
// is always idle.
func (r *Runs) Idle(ctx context.Context) bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	idle := r.idle
	r.mu.Unlock()
	if idle == nil {
		return true
	}
	select {
	case <-idle:
		return true
	case <-ctx.Done():
		return false
	}
}

// KillStrays ends every process of the identity's, unless a command runs as
// it (Hold): none of them is then the worker's. It is what makes "nothing the
// run started still runs" true before the worker relies on it.
func (r *Runs) KillStrays() {
	if r == nil || r.id == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.n == 0 {
		r.sweep()
	}
}

// killAll ends every process of the identity's, with the lock held: no
// command may start as it meanwhile. kill(-1) from a process running as the
// identity reaches exactly those, and none can fork past it: the kernel
// signals them all under the lock that fork takes. The worker's own user is
// never swept.
//
// Bounded (sweepTimeout) and logged when it fails: every command start as the
// identity waits on it, and a sweep that cannot run (the switch of user
// refused) leaves processes running that the worker assumes are gone.
func (r *Runs) killAll() {
	if int(r.id.UID) == geteuid() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), sweepTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "kill -9 -1")
	cmd.Dir = "/"
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	cmd.WaitDelay = time.Second
	r.id.Apply(cmd)
	err := cmd.Run()
	// An exit status only says whether there was anything to end.
	var exit *exec.ExitError
	switch {
	case ctx.Err() != nil:
		log.Printf("Warning: ending the processes of uid %d timed out after %s", r.id.UID, sweepTimeout)
	case err != nil && !errors.As(err, &exit):
		log.Printf("Warning: could not end the processes of uid %d: %v", r.id.UID, err)
	}
}
