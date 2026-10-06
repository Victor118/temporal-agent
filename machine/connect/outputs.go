//go:build unix

package connect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"syscall"
	"time"

	"github.com/victor/temporal-agent/machine"
)

// DefaultMaxFileBytes bounds a file of a run's outputs on the machine
// (Coder.MaxFileBytes): the server's own default, FILES_MAX_BYTES.
const DefaultMaxFileBytes = 20 << 20

// outputsBudget bounds the time the outputs take to publish once the run is
// over: what is left is said not published.
const outputsBudget = 60 * time.Second

func (a *Coder) maxFileBytes() int64 {
	if a.MaxFileBytes > 0 {
		return a.MaxFileBytes
	}
	return DefaultMaxFileBytes
}

// publishOutputs publishes what the run left in dir (its outputs), each
// file under its name, for the directive ctx runs, and returns what it did
// not publish, and why ("path: reason"). Regular files only, read through
// an os.Root of dir: a link is refused, not followed, and so is a file
// another path shares (a hard link: a run could link a file of its owner's
// there). At most machine.MaxOutputFiles files, machine.MaxOutputDepth
// directories deep, machine.MaxOutputEntries entries read. The CLI is gone
// by now (its session ended with it): nothing changes the directory under
// the walk. A run stopped or cancelled publishes nothing.
func (a *Coder) publishOutputs(ctx context.Context, dir string) []string {
	if ctx.Err() != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, outputsBudget)
	defer cancel()
	root, err := os.OpenRoot(dir)
	if err != nil {
		return []string{fmt.Sprintf("%s: %v", machine.OutputsDir, err)}
	}
	defer root.Close()

	var refused, files []string
	entries := 0
	walkErr := fs.WalkDir(root.FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			refused = append(refused, fmt.Sprintf("%s: %v", p, err))
			return nil
		}
		if p == "." {
			return nil
		}
		if entries++; entries > machine.MaxOutputEntries {
			refused = append(refused, fmt.Sprintf("the rest: more than %d entries, not looked at", machine.MaxOutputEntries))
			return fs.SkipAll
		}
		switch t := d.Type(); {
		case d.IsDir():
			if depth(p) >= machine.MaxOutputDepth {
				refused = append(refused, fmt.Sprintf("%s/: more than %d directories deep, not looked at", p, machine.MaxOutputDepth))
				return fs.SkipDir
			}
		case t&fs.ModeSymlink != 0:
			refused = append(refused, p+": a link, not published")
		case !t.IsRegular():
			refused = append(refused, p+": not a regular file")
		default:
			files = append(files, p)
		}
		return nil
	})
	if walkErr != nil {
		refused = append(refused, fmt.Sprintf("%s: %v", machine.OutputsDir, walkErr))
	}
	if len(files) > 0 && a.Upload == nil {
		return append(refused, fmt.Sprintf("%d files: this machine publishes no file", len(files)))
	}
	names := map[string]bool{}
	for i, p := range files {
		if i >= machine.MaxOutputFiles {
			refused = append(refused, fmt.Sprintf("%s: at most %d files are published", p, machine.MaxOutputFiles))
			continue
		}
		if ctx.Err() != nil {
			refused = append(refused, fmt.Sprintf("%s: the %s given to publish ran out", p, outputsBudget))
			continue
		}
		name := path.Base(p)
		if names[name] {
			refused = append(refused, fmt.Sprintf("%s: another file is published as %s", p, name))
			continue
		}
		names[name] = true
		content, err := a.readOutput(root, p)
		if err == nil {
			_, err = a.Upload(ctx, name, content)
		}
		if err != nil {
			refused = append(refused, fmt.Sprintf("%s: %v", p, err))
		}
	}
	return refused
}

// depth is how many directories deep p is in the outputs.
func depth(p string) int {
	n := 1
	for _, c := range p {
		if c == '/' {
			n++
		}
	}
	return n
}

// readOutput reads the file at p, refused unless it is regular, its own (one
// link), and within MaxFileBytes: read up to one byte past it.
func (a *Coder) readOutput(root *os.Root, p string) ([]byte, error) {
	f, err := root.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Nlink > 1 {
		return nil, errors.New("another path shares this file (a hard link): not published")
	}
	max := a.maxFileBytes()
	if fi.Size() > max {
		return nil, fmt.Errorf("too large: at most %d bytes are published", max)
	}
	content, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > max {
		return nil, fmt.Errorf("too large: at most %d bytes are published", max)
	}
	return content, nil
}
