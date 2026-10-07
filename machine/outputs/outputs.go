// Package outputs publishes what a coding run left in its outputs directory
// (machine.OutputsDir): on a user's machine through the gateway, on a worker
// through its file store. The same bounds on both: regular files only,
// never through a link nor a file another path shares, so many, so deep,
// so large.
package outputs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"time"

	"github.com/victor/temporal-agent/machine"
)

// Budget bounds the time the outputs take to publish once the run is over:
// what is left is said not published.
const Budget = 60 * time.Second

// Upload publishes one file under its name; its error, in words for the
// model, says why it was not.
type Upload func(ctx context.Context, name string, content []byte) error

// Publish publishes what a run left in dir, each file under its base name,
// and returns what it did not publish, and why ("path: reason"). Each file
// is read through an os.Root of dir, regular and its own (one link), at
// most max bytes; at most machine.MaxOutputFiles of them,
// machine.MaxOutputDepth directories deep, machine.MaxOutputEntries entries
// read, within Budget. The run must be over: nothing should change the
// directory under the walk, and what does cannot lead it out of dir. A
// context already done publishes nothing, and says why.
func Publish(ctx context.Context, dir string, max int64, upload Upload) []string {
	switch err := ctx.Err(); {
	case errors.Is(err, context.DeadlineExceeded):
		return []string{"outputs not published: the run's time ran out"}
	case err != nil:
		return []string{"outputs not published: the run was stopped"}
	}
	ctx, cancel := context.WithTimeout(ctx, Budget)
	defer cancel()
	root, err := os.OpenRoot(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
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
	if len(files) > 0 && upload == nil {
		return append(refused, fmt.Sprintf("%d files: nothing publishes them here", len(files)))
	}
	names := map[string]bool{}
	for i, p := range files {
		if i >= machine.MaxOutputFiles {
			refused = append(refused, fmt.Sprintf("%s: at most %d files are published", p, machine.MaxOutputFiles))
			continue
		}
		if ctx.Err() != nil {
			refused = append(refused, fmt.Sprintf("%s: the %s given to publish ran out", p, Budget))
			continue
		}
		name := path.Base(p)
		if names[name] {
			refused = append(refused, fmt.Sprintf("%s: another file is published as %s", p, name))
			continue
		}
		names[name] = true
		content, err := read(root, p, max)
		if err == nil {
			err = upload(ctx, name, content)
		}
		if err != nil {
			refused = append(refused, fmt.Sprintf("%s: %v", p, err))
		}
	}
	return refused
}

// depth is how many directories deep p is.
func depth(p string) int {
	n := 1
	for _, c := range p {
		if c == '/' {
			n++
		}
	}
	return n
}

// read reads the file at p, refused unless it is regular, its own (one
// link: a run could link a file of its user's there), and within max bytes:
// read up to one byte past it. Not through a link, even inside the root.
func read(root *os.Root, p string, max int64) ([]byte, error) {
	f, err := root.OpenFile(p, openFlags, 0)
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
	if shared(fi) {
		return nil, errors.New("another path shares this file (a hard link): not published")
	}
	if fi.Size() > max {
		return nil, fmt.Errorf("too large: at most %s are published", size(max))
	}
	content, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > max {
		return nil, fmt.Errorf("too large: at most %s are published", size(max))
	}
	return content, nil
}

func size(n int64) string {
	switch {
	case n < 1024:
		return fmt.Sprintf("%d B", n)
	case n < 1024*1024:
		return fmt.Sprintf("%.1f KB", float64(n)/1024)
	}
	return fmt.Sprintf("%.1f MB", float64(n)/(1024*1024))
}
