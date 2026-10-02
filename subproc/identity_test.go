package subproc

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func TestParseIdentity(t *testing.T) {
	if id, err := ParseIdentity("", "123"); id != nil || err != nil {
		t.Errorf("no uid: %+v, %v", id, err)
	}
	id, err := ParseIdentity(" 10001 ", "")
	if err != nil || id.UID != 10001 || id.GID != 10001 || id.Home == "" {
		t.Errorf("uid alone: %+v, %v", id, err)
	}
	if id, err := ParseIdentity("10001", "20002"); err != nil || id.GID != 20002 {
		t.Errorf("uid and gid: %+v, %v", id, err)
	}
	// An account's home comes from /etc/passwd; nobody has none worth using.
	if id, _ := ParseIdentity("65534", ""); id.Home == "/nonexistent" {
		t.Errorf("nobody's home = %q", id.Home)
	}
	for _, bad := range [][2]string{{"0", ""}, {"10001", "0"}, {"agent-run", ""}, {"-1", ""}, {"10001", "x"}} {
		if _, err := ParseIdentity(bad[0], bad[1]); err == nil {
			t.Errorf("ParseIdentity(%q, %q) was accepted", bad[0], bad[1])
		}
	}
}

func TestCheckRunAs(t *testing.T) {
	defer func(f func() int) { geteuid = f }(geteuid)
	someone := &Identity{UID: 10001, GID: 10001}

	geteuid = func() int { return 0 }
	if err := CheckRunAs(nil); !errors.Is(err, ErrRootWithoutIdentity) {
		t.Errorf("root without an identity: %v", err)
	}
	if err := CheckRunAs(someone); err != nil {
		t.Errorf("root with an identity: %v", err)
	}

	geteuid = func() int { return 1000 }
	if err := CheckRunAs(nil); err != nil {
		t.Errorf("a user without an identity: %v", err)
	}
	if err := CheckRunAs(someone); err == nil {
		t.Error("a user that cannot switch to the identity was accepted")
	}
	if err := CheckRunAs(&Identity{UID: 1000, GID: 1000}); err != nil {
		t.Errorf("a user running as itself: %v", err)
	}
}

func TestIdentityEnv(t *testing.T) {
	id := &Identity{UID: 10001, GID: 10001, Home: "/home/agent-run"}
	got := strings.Join(id.Env([]string{"PATH=/bin", "HOME=/root", "GOPATH=/go", "GOCACHE=/root/.cache/go-build", "GOMODCACHE=/go/pkg/mod", "GOFLAGS=-x"}), " ")
	want := "PATH=/bin GOFLAGS=-x HOME=/home/agent-run GOPATH=/home/agent-run/go"
	if got != want {
		t.Errorf("Env = %q, want %q", got, want)
	}
	var none *Identity
	if got := none.Env([]string{"HOME=/root"}); len(got) != 1 || got[0] != "HOME=/root" {
		t.Errorf("nil identity changed the environment: %v", got)
	}
}

// requireRoot skips a test that switches users, which takes root.
func requireRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() != 0 {
		t.Skip("switching users takes root")
	}
}

// nobody is an identity the tests can switch to, with a home of its own.
func nobody(t *testing.T) *Identity {
	t.Helper()
	home, err := os.MkdirTemp("", "nobody-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(home) })
	id := &Identity{UID: 65534, GID: 65534, Home: home}
	if err := id.PrepareHome(); err != nil {
		t.Fatal(err)
	}
	return id
}

// A command run as another user cannot read the worker's environment back
// from /proc, which a filtered environment alone does not prevent.
func TestApply_TheWorkersEnvironmentIsOutOfReach(t *testing.T) {
	requireRoot(t)
	environ := "/proc/" + strconv.Itoa(os.Getpid()) + "/environ"

	// As the worker's user, the command reads it.
	if out, err := exec.Command("sh", "-c", "cat "+environ+" >/dev/null").CombinedOutput(); err != nil {
		t.Fatalf("the worker's own user cannot read %s: %v: %s; the test would prove nothing", environ, err, out)
	}

	id := nobody(t)
	cmd := exec.Command("sh", "-c", "id -u; id -G; cat "+environ)
	cmd.Dir = id.Home
	cmd.Env = id.Env(Env(os.Environ(), nil, nil))
	id.Apply(cmd)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the command read the worker's environment:\n%s", out)
	}
	lines := strings.Split(string(out), "\n")
	if lines[0] != "65534" {
		t.Errorf("the command ran as %q, want 65534", lines[0])
	}
	if len(lines) < 2 || lines[1] != "65534" {
		t.Errorf("the command's groups are %q, want its own alone", lines[1])
	}
}

// Reclaim stops a process left behind from adding, removing or renaming
// entries of a directory it was given.
func TestReclaim(t *testing.T) {
	requireRoot(t)
	id := nobody(t)
	dir := filepath.Join(id.Home, "ws")
	os.MkdirAll(filepath.Join(dir, ".git"), 0o777)
	if err := id.Give(dir); err != nil {
		t.Fatal(err)
	}

	asNobody := func(script string) error {
		cmd := exec.Command("sh", "-c", script)
		cmd.Dir = dir
		id.Apply(cmd)
		return cmd.Run()
	}
	if err := asNobody("touch given"); err != nil {
		t.Fatalf("the given directory is not writable by its new owner: %v", err)
	}

	if err := Reclaim(dir); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(dir)
	if st := fi.Sys().(*syscall.Stat_t); st.Uid != 0 || fi.Mode().Perm() != 0o755 {
		t.Errorf("reclaimed directory: owner %d, mode %v", st.Uid, fi.Mode().Perm())
	}
	for _, script := range []string{"touch new", "mv .git elsewhere", "rm given"} {
		if err := asNobody(script); err == nil {
			t.Errorf("%q succeeded in a reclaimed directory", script)
		}
	}
	if err := Reclaim(filepath.Join(dir, "given")); err == nil {
		t.Error("reclaimed a file as a directory")
	}
}
