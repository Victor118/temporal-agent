package machine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
)

// Git runs one git command in a clone, with the environment and the options
// of whoever runs it (a machine's owner, a worker): its output, and its
// error.
type Git func(ctx context.Context, args ...string) (string, error)

// WriteBundle writes at path a git bundle of branch at sha, from base: the
// commits of the run alone (the user's clone has the base), under
// refs/heads/<branch>, which git fetch names. git bundle takes refs, not a
// bare commit: the branch is set to sha first, whatever the run left it at.
// The bundle is verified once written. The clone's configuration must have
// been restored, and the run be over: git reads the clone.
func WriteBundle(ctx context.Context, git Git, branch, base, sha, path string) error {
	ref := "refs/heads/" + branch
	if out, err := git(ctx, "update-ref", "--no-deref", ref, sha); err != nil {
		return fmt.Errorf("git update-ref: %v: %s", err, Cut(out, 512))
	}
	if out, err := git(ctx, "bundle", "create", "--quiet", path, "--end-of-options", ref, "^"+base); err != nil {
		return fmt.Errorf("git bundle create: %v: %s", err, Cut(out, 512))
	}
	if out, err := git(ctx, "bundle", "verify", "--quiet", path); err != nil {
		return fmt.Errorf("git bundle verify: %v: %s", err, Cut(out, 512))
	}
	return nil
}

// ReadBundle reads the bundle at path, refused past max bytes (a file the
// server would not take): said with its size.
func ReadBundle(path string, max int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, errors.New("the bundle is not a regular file")
	}
	if fi.Size() > max {
		return nil, fmt.Errorf("the bundle is %s, over the %s a published file may be", megabytes(fi.Size()), megabytes(max))
	}
	content, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > max {
		return nil, fmt.Errorf("the bundle is over the %s a published file may be", megabytes(max))
	}
	return content, nil
}

func megabytes(n int64) string {
	return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
}
