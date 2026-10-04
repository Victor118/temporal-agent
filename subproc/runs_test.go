package subproc

import (
	"os"
	"testing"
	"time"
)

// countingRuns is a Runs for id whose sweeps are counted, not run.
func countingRuns(id *Identity) (*Runs, *int) {
	r := NewRuns(id)
	sweeps := 0
	r.sweep = func() { sweeps++ }
	return r, &sweeps
}

// The processes are swept once the last command held is released, never
// while one still runs; KillStrays sweeps only when none does.
func TestRuns_SweepsOnlyWhenNoCommandRuns(t *testing.T) {
	r, sweeps := countingRuns(&Identity{UID: 10001, GID: 10001})

	first, second := r.Hold(), r.Hold()
	r.KillStrays()
	first()
	if *sweeps != 0 {
		t.Fatalf("%d sweeps while a command still ran", *sweeps)
	}
	second()
	if *sweeps != 1 {
		t.Fatalf("%d sweeps once the last command was done, want 1", *sweeps)
	}
	r.KillStrays()
	if *sweeps != 2 {
		t.Errorf("%d sweeps after KillStrays with nothing held, want 2", *sweeps)
	}

	// Each Runs counts its own commands: another's held command does not
	// stop this one's sweep.
	other, _ := countingRuns(&Identity{UID: 10001, GID: 10001})
	defer other.Hold()()
	r.Hold()()
	if *sweeps != 3 {
		t.Errorf("%d sweeps, want one more, whatever another Runs holds", *sweeps)
	}
}

// With no identity, or none at all, nothing is counted nor swept.
func TestRuns_WithoutAnIdentityDoesNothing(t *testing.T) {
	var none *Runs
	none.Hold()()
	none.KillStrays()

	r, sweeps := countingRuns(nil)
	r.Hold()()
	r.KillStrays()
	if *sweeps != 0 {
		t.Errorf("%d sweeps without an identity", *sweeps)
	}
}

// What left the command's group (setsid) is ended once no other command runs
// as the same user, and only then.
func TestRuns_EndsStraysWhenTheLastCommandIsDone(t *testing.T) {
	requireRoot(t)
	id := nobody(t)
	r := NewRuns(id)

	first := r.Hold()
	second := r.Hold()
	startAs(t, id, "setsid sleep 301 >/dev/null 2>&1 < /dev/null &")
	time.Sleep(100 * time.Millisecond)
	if runningAs(id) == 0 {
		t.Fatal("the detached process is not running: the test would prove nothing")
	}
	first()
	r.KillStrays()
	if runningAs(id) == 0 {
		t.Fatal("a process was ended while a command still ran as its user")
	}
	second()
	waitNoneRunningAs(t, id, "the last command")

	// With nothing held, KillStrays ends them at once.
	startAs(t, id, "setsid sleep 302 >/dev/null 2>&1 < /dev/null &")
	time.Sleep(100 * time.Millisecond)
	r.KillStrays()
	waitNoneRunningAs(t, id, "KillStrays")
}

// waitNoneRunningAs fails t if a process of id's still runs a moment after
// what should have ended them all: a signal is delivered, not waited for.
func waitNoneRunningAs(t *testing.T, id *Identity, after string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for runningAs(id) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runningAs(id); n > 0 {
		t.Errorf("%d processes outlived %s", n, after)
	}
}

// Ending the strays never ends the worker's own user's processes.
func TestRuns_NeverTheWorker(t *testing.T) {
	requireRoot(t)
	id := nobody(t)
	r := NewRuns(id)
	startAs(t, id, "setsid sleep 303 >/dev/null 2>&1 < /dev/null &")
	defer r.KillStrays()
	time.Sleep(100 * time.Millisecond)
	defer func(f func() int) { geteuid = f }(geteuid)
	geteuid = func() int { return int(id.UID) } // the worker runs as id
	r.KillStrays()
	r.Hold()()
	time.Sleep(100 * time.Millisecond)
	if runningAs(id) == 0 {
		t.Error("the worker's own user's processes were ended")
	}
	geteuid = os.Geteuid
}
