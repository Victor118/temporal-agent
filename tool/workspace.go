package tool

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/victor/temporal-agent/subproc"
)

// workspace is the directory the file tools (read_file, write_file,
// edit_file, list_directory, grep, glob) work in, as the worker's user.
//
// It belongs to the user exec runs as, whose processes can change any entry of
// it at any moment: swap a file for a symbolic link to /proc/<worker
// pid>/environ between the moment a path is checked and the moment it is
// opened, for one. So a path is never checked and then opened: every entry is
// reached through an os.Root on the workspace, which resolves each component
// as it opens it and refuses one leading out — "..", or a symbolic link whose
// target is absolute or climbs out of the workspace. Links that stay inside
// are followed.
type workspace struct {
	dir string
	// owner is the user exec runs as, given what the tools create so that
	// its commands can change it; nil leaves it to the worker's user.
	owner *subproc.Identity
}

func newWorkspace(path string, owner *subproc.Identity) workspace {
	dir, _ := filepath.Abs(path)
	return workspace{dir: dir, owner: owner}
}

// open opens the workspace for one tool call; the caller closes it.
func (w workspace) open() (*os.Root, error) {
	return os.OpenRoot(w.dir)
}

// relPath turns a path a model gave into one relative to the workspace, as
// os.Root takes it: an absolute path is taken relative to the workspace, and
// ".." may not climb out of it. Links are os.Root's to check, as it opens.
func relPath(p string) (string, error) {
	rel := filepath.Clean(strings.TrimLeft(p, "/"))
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("path escapes workspace: %s", p)
	}
	return rel, nil
}

// openRegular opens name in root for flag, and refuses anything but a regular
// file: a FIFO left in the workspace would block a read until the activity
// times out (O_NONBLOCK keeps the open itself from blocking; it changes
// nothing for a regular file).
func openRegular(root *os.Root, name string, flag int) (*os.File, error) {
	f, err := root.OpenFile(name, flag|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err == nil && !fi.Mode().IsRegular() {
		err = fmt.Errorf("%s is not a regular file", name)
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// readFile reads the regular file name in root.
func readFile(root *os.Root, name string) ([]byte, error) {
	f, err := openRegular(root, name, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// writeFile writes data to name in root, creating it and its missing parents.
// What it creates goes to the owner; a file that was there keeps its owner and
// mode.
func (w workspace) writeFile(root *os.Root, name string, data []byte) error {
	if err := w.mkdirAll(root, filepath.Dir(name)); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	// O_EXCL creates nothing through a link, not even a dangling one: it
	// fails on any existing entry, and the file is then opened as it is.
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	switch {
	case err == nil:
		if err := w.owner.GiveFile(f); err != nil {
			f.Close()
			return err
		}
	case errors.Is(err, fs.ErrExist):
		if f, err = openRegular(root, name, os.O_WRONLY); err != nil {
			return err
		}
		if err := f.Truncate(0); err != nil {
			f.Close()
			return err
		}
	default:
		return err
	}
	_, err = f.Write(data)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// mkdirAll creates dir and its missing parents in root, and gives the ones it
// creates to the owner.
func (w workspace) mkdirAll(root *os.Root, dir string) error {
	if dir == "." {
		return nil
	}
	cur := ""
	for _, part := range strings.Split(dir, string(filepath.Separator)) {
		cur = filepath.Join(cur, part)
		err := root.Mkdir(cur, 0o755)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return err
		}
		if err := w.giveDir(root, cur); err != nil {
			return err
		}
	}
	return nil
}

// giveDir gives the directory name, just created, to the owner. A process of
// the owner's could have swapped it in between, for a link to another
// directory: os.Root keeps that one inside the workspace, which is the
// owner's already.
func (w workspace) giveDir(root *os.Root, name string) error {
	if w.owner == nil {
		return nil
	}
	d, err := root.OpenFile(name, os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	defer d.Close()
	return w.owner.GiveFile(d)
}

// readDir lists the directory name in root, sorted by name. O_DIRECTORY
// refuses anything else without opening it: a FIFO would block.
func readDir(root *os.Root, name string) ([]fs.DirEntry, error) {
	d, err := root.OpenFile(name, os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	entries, err := d.ReadDir(-1)
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(a.Name(), b.Name()) })
	return entries, err
}

// walkFS is root as fs.WalkDir reads it: every directory through readDir.
type walkFS struct{ root *os.Root }

func (w walkFS) Open(name string) (fs.File, error) { return openRegular(w.root, name, os.O_RDONLY) }

func (w walkFS) Stat(name string) (fs.FileInfo, error) { return w.root.Stat(name) }

func (w walkFS) ReadDir(name string) ([]fs.DirEntry, error) { return readDir(w.root, name) }
