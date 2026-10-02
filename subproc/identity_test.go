package subproc

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
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

// testUID is the user this test binary runs commands as: one of its own, as
// subproctest.UID is for the others (Hold ends every process of a user once
// none of its commands runs, and the packages' tests run at the same time).
var testUID = uint32(40000 + os.Getpid()%20000)

// nobody is an identity the tests can switch to, with a home of its own.
func nobody(t *testing.T) *Identity {
	t.Helper()
	home, err := os.MkdirTemp("", "nobody-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(home) })
	id := &Identity{UID: testUID, GID: testUID, Home: home}
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
	want := strconv.Itoa(int(testUID))
	if lines[0] != want {
		t.Errorf("the command ran as %q, want %s", lines[0], want)
	}
	if len(lines) < 2 || lines[1] != want {
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

// startAs starts script as id in a process group of its own, as the worker
// starts a command, and waits for it.
func startAs(t *testing.T, id *Identity, script string) *exec.Cmd {
	t.Helper()
	cmd := exec.CommandContext(context.Background(), "sh", "-c", script)
	cmd.Dir = id.Home
	id.Apply(cmd)
	KillGroupOnCancel(cmd, syscall.SIGKILL, time.Second)
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	return cmd
}

// runningAs counts the processes of id's still running, zombies aside.
func runningAs(id *Identity) int {
	n := 0
	entries, _ := os.ReadDir("/proc")
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		var st syscall.Stat_t
		if syscall.Stat("/proc/"+e.Name(), &st) == nil && st.Uid == id.UID && runs(pid) {
			n++
		}
	}
	return n
}

// A command that returns leaves nothing behind in its group: KillGroup ends
// what it started in the background.
func TestKillGroup_AfterACommandReturns(t *testing.T) {
	requireRoot(t)
	id := nobody(t)
	cmd := startAs(t, id, "sleep 300 >/dev/null 2>&1 & echo $! > child.pid")
	raw, _ := os.ReadFile(filepath.Join(id.Home, "child.pid"))
	pid, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
	if !alive(pid) {
		t.Fatal("the background process is not running: the test would prove nothing")
	}
	KillGroup(cmd)
	if alive(pid) {
		t.Errorf("the background process %d survived its command", pid)
	}
}

// What left the command's group (setsid) is ended once no other command runs
// as the same user, and only then.
func TestHold_EndsStraysWhenTheLastCommandIsDone(t *testing.T) {
	requireRoot(t)
	id := nobody(t)

	first := id.Hold()
	second := id.Hold()
	startAs(t, id, "setsid sleep 301 >/dev/null 2>&1 < /dev/null &")
	time.Sleep(100 * time.Millisecond)
	if runningAs(id) == 0 {
		t.Fatal("the detached process is not running: the test would prove nothing")
	}
	first()
	id.KillStrays()
	if runningAs(id) == 0 {
		t.Fatal("a process was ended while a command still ran as its user")
	}
	second()
	deadline := time.Now().Add(2 * time.Second)
	for runningAs(id) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runningAs(id); n > 0 {
		t.Errorf("%d processes outlived the last command", n)
	}

	// With nothing held, KillStrays ends them at once.
	startAs(t, id, "setsid sleep 302 >/dev/null 2>&1 < /dev/null &")
	time.Sleep(100 * time.Millisecond)
	id.KillStrays()
	deadline = time.Now().Add(2 * time.Second)
	for runningAs(id) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runningAs(id); n > 0 {
		t.Errorf("%d processes outlived KillStrays", n)
	}
}

// Ending the strays never ends the worker: not when the identity is the
// worker's own user, nor with no identity.
func TestKillStrays_NeverTheWorker(t *testing.T) {
	requireRoot(t)
	var none *Identity
	none.KillStrays()
	none.Hold()()

	id := nobody(t)
	startAs(t, id, "setsid sleep 303 >/dev/null 2>&1 < /dev/null &")
	defer id.KillStrays()
	time.Sleep(100 * time.Millisecond)
	defer func(f func() int) { geteuid = f }(geteuid)
	geteuid = func() int { return int(id.UID) } // the worker runs as id
	id.KillStrays()
	id.Hold()()
	time.Sleep(100 * time.Millisecond)
	if runningAs(id) == 0 {
		t.Error("the worker's own user's processes were ended")
	}
	geteuid = os.Geteuid
}

// ReclaimTree takes every entry of the tree back and closes it to others,
// without following a link out of it nor changing a file another path shares.
func TestReclaimTree(t *testing.T) {
	requireRoot(t)
	id := nobody(t)
	tree := filepath.Join(id.Home, "tree")
	outside := filepath.Join(id.Home, "outside")
	shared := filepath.Join(id.Home, "shared")
	os.MkdirAll(filepath.Join(tree, "sub"), 0o777)
	os.WriteFile(filepath.Join(tree, "sub", "f"), []byte("f"), 0o666)
	os.WriteFile(outside, []byte("o"), 0o666)
	os.WriteFile(shared, []byte("s"), 0o666)
	os.Symlink(outside, filepath.Join(tree, "link"))
	os.Link(shared, filepath.Join(tree, "hard"))
	for _, p := range []string{tree, outside, shared} {
		if err := id.Give(p); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{tree, filepath.Join(tree, "sub"), filepath.Join(tree, "sub", "f"), outside, shared} {
		os.Chmod(p, 0o777)
	}

	if err := ReclaimTree(tree); err != nil {
		t.Fatal(err)
	}
	stat := func(p string) (uint32, os.FileMode) {
		fi, err := os.Lstat(p)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Sys().(*syscall.Stat_t).Uid, fi.Mode()
	}
	for _, p := range []string{tree, filepath.Join(tree, "sub"), filepath.Join(tree, "sub", "f"), filepath.Join(tree, "link")} {
		uid, mode := stat(p)
		if uid != 0 || (mode&os.ModeSymlink == 0 && mode.Perm()&0o022 != 0) {
			t.Errorf("%s: uid %d, mode %v after ReclaimTree", p, uid, mode)
		}
	}
	for _, p := range []string{outside, shared} {
		if uid, mode := stat(p); uid != id.UID || mode.Perm() != 0o777 {
			t.Errorf("%s, outside the tree, changed: uid %d, mode %v", p, uid, mode)
		}
	}
}
