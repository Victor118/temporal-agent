package subproc

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// Identity is the user a command chosen by a model runs as, in place of the
// worker's.
//
// Filtering the environment (Env) keeps the platform's credentials out of a
// command's own environment, not out of its reach: run as the worker's user,
// it reads them back from /proc/<worker pid>/environ, and opens whatever key
// file the worker can. Run as another user, it can do neither: the worker's
// /proc entries and its 0600 files are closed to it.
type Identity struct {
	UID, GID uint32
	// Home is the user's home: HOME for its commands, and where their Go
	// caches go (IdentityEnv).
	Home string
}

// ErrRootWithoutIdentity is a worker running as root with no identity to run
// commands as: they would run as root, the platform's credentials in reach.
var ErrRootWithoutIdentity = errors.New("the worker runs as root and RUN_AS_UID is not set: commands chosen by a model would run as root, with the worker's credentials in reach")

// geteuid is the worker's user, replaced by the tests.
var geteuid = os.Geteuid

// ParseIdentity reads RUN_AS_UID and RUN_AS_GID. An empty uid is no identity
// (nil); an empty gid is the uid's value. The home is the user's from
// /etc/passwd, or a directory of its own under the temporary directory for a
// uid with no account.
func ParseIdentity(uid, gid string) (*Identity, error) {
	uid, gid = strings.TrimSpace(uid), strings.TrimSpace(gid)
	if uid == "" {
		return nil, nil
	}
	if gid == "" {
		gid = uid
	}
	u, err := strconv.ParseUint(uid, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("RUN_AS_UID %q is not a number", uid)
	}
	g, err := strconv.ParseUint(gid, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("RUN_AS_GID %q is not a number", gid)
	}
	if u == 0 || g == 0 {
		return nil, errors.New("RUN_AS_UID and RUN_AS_GID must not be root's")
	}
	id := &Identity{UID: uint32(u), GID: uint32(g)}
	if account, err := user.LookupId(uid); err == nil && account.HomeDir != "" && account.HomeDir != "/nonexistent" {
		id.Home = account.HomeDir
	} else {
		id.Home = filepath.Join(os.TempDir(), "run-as-"+uid)
	}
	return id, nil
}

// CheckRunAs tells whether this worker may start a command chosen by a model
// as id: never as root, and only as a user the worker can switch to. A nil id
// runs the command as the worker's user, which is fine unless that is root.
func CheckRunAs(id *Identity) error {
	euid := geteuid()
	switch {
	case id == nil && euid == 0:
		return ErrRootWithoutIdentity
	case id != nil && euid != 0 && int(id.UID) != euid:
		return fmt.Errorf("RUN_AS_UID=%d needs a worker running as root, to switch to it", id.UID)
	}
	return nil
}

// Apply makes cmd run as id, with no supplementary group: the worker's (root's
// group, for one) must not come along. A nil id leaves cmd as it is.
func (id *Identity) Apply(cmd *exec.Cmd) {
	if id == nil {
		return
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: id.UID, Gid: id.GID, Groups: []uint32{}}
}

// Env adapts a command's environment (from Env) to id: its own home, and Go
// caches under it. The worker's module and build caches stay the worker's: a
// command that could write them could plant code in the next build of the
// agent itself, which compiles from them. A nil id leaves env as it is.
func (id *Identity) Env(env []string) []string {
	if id == nil {
		return env
	}
	out := make([]string, 0, len(env)+2)
	for _, kv := range env {
		name, _, _ := strings.Cut(kv, "=")
		switch name {
		case "HOME", "GOPATH", "GOCACHE", "GOMODCACHE":
			continue
		}
		out = append(out, kv)
	}
	return append(out, "HOME="+id.Home, "GOPATH="+filepath.Join(id.Home, "go"))
}

// PrepareHome creates id's home if it is missing, and gives it to id.
func (id *Identity) PrepareHome() error {
	if id == nil {
		return nil
	}
	if err := os.MkdirAll(id.Home, 0o700); err != nil {
		return err
	}
	return os.Lchown(id.Home, int(id.UID), int(id.GID))
}

// Give hands the tree at root over to id, so that its commands can write
// there. Links are given, never followed. A nil id gives nothing.
func (id *Identity) Give(root string) error {
	if id == nil {
		return nil
	}
	return filepath.WalkDir(root, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Lchown(path, int(id.UID), int(id.GID))
	})
}

// Reclaim takes dir back from whoever the commands ran as: owned by the
// worker, writable by it alone. After it, a process left behind by a command
// can no longer add, remove or rename an entry of dir — what the worker is
// about to rely on stays put. The entries themselves keep their owner.
func Reclaim(dir string) error {
	fi, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", dir)
	}
	if err := os.Lchown(dir, geteuid(), os.Getegid()); err != nil {
		return err
	}
	return os.Chmod(dir, 0o755)
}
