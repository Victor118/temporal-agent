// Package subproctest gives tests an identity to run commands as, the way a
// worker running as root does.
package subproctest

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/victor/temporal-agent/subproc"
)

// UID is the user the tests of this test binary run commands as when they run
// as root: one with no account, of this binary's own. Once none of a worker's
// commands runs as a user, every process of that user is ended (subproc.Runs),
// whichever process started it, and the test binaries of several packages run
// at the same time: they must not end each other's commands.
var UID = uint32(40000 + os.Getpid()%20000)

// Identity returns the identity a test's commands run as: UID when the test
// runs as root (as in the agent's container), where a command must not run as
// root; nil otherwise, the test's own user.
func Identity(t testing.TB) *subproc.Identity {
	t.Helper()
	if os.Geteuid() != 0 {
		return nil
	}
	id := &subproc.Identity{UID: UID, GID: UID, Home: Dir(t, nil)}
	if err := id.PrepareHome(); err != nil {
		t.Fatal(err)
	}
	return id
}

// Dir returns a temporary directory that id can reach and write: t.TempDir
// is under a directory only its owner can enter. Removed with the test.
func Dir(t testing.TB, id *subproc.Identity) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "subproctest-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := id.Give(dir); err != nil {
		t.Fatal(err)
	}
	return dir
}

// Processes lists the processes of uid that still run — zombies aside, which
// run nothing — as "pid command".
func Processes(uid uint32) []string {
	var found []string
	entries, _ := os.ReadDir("/proc")
	for _, e := range entries {
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		status, err := os.ReadFile(filepath.Join("/proc", e.Name(), "status"))
		if err != nil {
			continue
		}
		var name, state, owner string
		for _, line := range strings.Split(string(status), "\n") {
			key, value, _ := strings.Cut(line, ":")
			fields := strings.Fields(value)
			if len(fields) == 0 {
				continue
			}
			switch key {
			case "Name":
				name = fields[0]
			case "State":
				state = fields[0]
			case "Uid":
				owner = fields[0]
			}
		}
		if owner == strconv.FormatUint(uint64(uid), 10) && state != "Z" && state != "X" {
			found = append(found, e.Name()+" "+name)
		}
	}
	return found
}

// NoProcessLeft fails t if a process of uid still runs a moment after it
// should all have been ended: a signal is delivered, not waited for.
func NoProcessLeft(t testing.TB, uid uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		left := Processes(uid)
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("processes of uid %d still run: %v", uid, left)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}
