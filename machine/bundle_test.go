package machine

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// A bundle written twice from the same commits is the same bytes (a retry
// publishes the same file), and a relative path lands where the caller
// stands, never in the clone git runs in.
func TestWriteBundle(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git")
	}
	clone := t.TempDir()
	run := func(ctx context.Context, args ...string) (string, error) {
		cmd := exec.CommandContext(ctx, "git", append([]string{"-c", "user.email=a@b", "-c", "user.name=a"}, args...)...)
		cmd.Dir = clone
		out, err := cmd.CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	must := func(args ...string) string {
		out, err := run(context.Background(), args...)
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return out
	}
	must("init", "--quiet", "-b", "main")
	must("commit", "--quiet", "--allow-empty", "-m", "base")
	base := must("rev-parse", "HEAD")
	must("checkout", "--quiet", "-b", "agent/x-1")
	for i := range 20 {
		os.WriteFile(filepath.Join(clone, "f.txt"), bytes.Repeat([]byte{byte('a' + i)}, 4096*(i+1)), 0o600)
		must("add", "f.txt")
		must("commit", "--quiet", "-m", "work")
	}
	sha := must("rev-parse", "HEAD")

	here := t.TempDir()
	t.Chdir(here)
	if err := WriteBundle(context.Background(), run, "agent/x-1", base, sha, "one.bundle"); err != nil {
		t.Fatal(err)
	}
	if err := WriteBundle(context.Background(), run, "agent/x-1", base, sha, filepath.Join(here, "two.bundle")); err != nil {
		t.Fatal(err)
	}
	one, err := os.ReadFile(filepath.Join(here, "one.bundle"))
	if err != nil {
		t.Fatalf("not where the caller stands: %v", err)
	}
	if _, err := os.Stat(filepath.Join(clone, "one.bundle")); err == nil {
		t.Error("written in the clone")
	}
	two, _ := os.ReadFile(filepath.Join(here, "two.bundle"))
	if !bytes.Equal(one, two) {
		t.Error("two bundles of the same commits differ")
	}
	if _, err := ReadBundle(filepath.Join(here, "two.bundle"), 10); err == nil || !strings.Contains(err.Error(), "a published file may be") {
		t.Errorf("past its bound: %v", err)
	}
}
