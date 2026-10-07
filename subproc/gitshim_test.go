package subproc

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The git a run finds first: commit's and add's file options read a file of
// the clone only, links resolved; everything else reaches the real git.
func TestGitShim(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git")
	}
	base := t.TempDir()
	clone := filepath.Join(base, "repo")
	for _, args := range [][]string{{"init", "--quiet", "-b", "main", clone}} {
		if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("%v %s", err, out)
		}
	}
	secret := filepath.Join(base, "secret")
	os.WriteFile(secret, []byte("PRIVATE KEY"), 0o600)
	os.WriteFile(filepath.Join(clone, "msg.txt"), []byte("A message from the clone"), 0o644)
	os.WriteFile(filepath.Join(clone, "f.txt"), []byte("x"), 0o644)
	os.Symlink(secret, filepath.Join(clone, "link"))
	dir, err := WriteGitShim(clone)
	if err != nil {
		t.Fatal(err)
	}
	if dir != GitShimDir(clone) {
		t.Errorf("dir %s", dir)
	}
	if fi, err := os.Stat(filepath.Join(dir, "git")); err != nil || fi.Mode().Perm()&0o222 != 0 {
		t.Errorf("the shim: %v %v", fi, err)
	}
	git := func(args ...string) (string, error) {
		cmd := exec.Command(filepath.Join(dir, "git"), args...)
		cmd.Dir = clone
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=a", "GIT_AUTHOR_EMAIL=a@b", "GIT_COMMITTER_NAME=a", "GIT_COMMITTER_EMAIL=a@b",
			"PATH=/nonexistent")
		cmd.Stdin = strings.NewReader("from stdin")
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	if out, err := git("add", "f.txt"); err != nil {
		t.Fatalf("add: %v %s", err, out)
	}
	for _, args := range [][]string{
		{"commit", "-F", secret},
		{"commit", "-F" + secret},
		{"commit", "--file", secret},
		{"commit", "--file=" + secret},
		{"commit", "--fil=" + secret},
		{"commit", "-aF", secret},
		{"commit", "-qF" + secret},
		{"commit", "-t", secret, "-m", "x"},
		{"commit", "--template=" + secret, "-m", "x"},
		{"commit", "--pathspec-from-file=" + secret, "-m", "x"},
		{"add", "--pathspec-from-file", secret},
		{"commit", "-F", "link"},
		{"commit", "-F", "../secret"},
		{"commit", "-F", "-"},
		{"commit", "-F"},
	} {
		if out, err := git(args...); err == nil || !strings.Contains(out, "refused for this run") {
			t.Errorf("%v: %v %s", args, err, out)
		}
	}
	if out, _ := exec.Command("git", "-C", clone, "log", "--format=%B").CombinedOutput(); strings.Contains(string(out), "PRIVATE") {
		t.Fatalf("a secret committed: %s", out)
	}
	// A message given otherwise: -m, a value that looks like an option, a
	// file of the clone.
	if out, err := git("commit", "-q", "-m", "-F"+secret); err != nil {
		t.Errorf("-m: %v %s", err, out)
	}
	os.WriteFile(filepath.Join(clone, "g.txt"), []byte("y"), 0o644)
	git("add", "g.txt")
	if out, err := git("commit", "-q", "-F", "msg.txt"); err != nil {
		t.Errorf("-F of the clone: %v %s", err, out)
	}
	if out, _ := exec.Command("git", "-C", clone, "log", "-1", "--format=%B").CombinedOutput(); !strings.Contains(string(out), "A message from the clone") {
		t.Errorf("the last commit: %s", out)
	}
	if out, err := git("status", "--porcelain"); err != nil {
		t.Errorf("status: %v %s", err, out)
	}
}
