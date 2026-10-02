// Package subproctest gives tests an identity to run commands as, the way a
// worker running as root does.
package subproctest

import (
	"os"
	"testing"

	"github.com/victor/temporal-agent/subproc"
)

// Nobody is the user the tests run commands as when they run as root.
const Nobody = 65534

// Identity returns the identity a test's commands run as: nobody when the test
// runs as root (as in the agent's container), where a command must not run as
// root; nil otherwise, the test's own user.
func Identity(t testing.TB) *subproc.Identity {
	t.Helper()
	if os.Geteuid() != 0 {
		return nil
	}
	id := &subproc.Identity{UID: Nobody, GID: Nobody, Home: Dir(t, nil)}
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
