package skill

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// The cache is the process's alone, at the clone as at every pull: a
// private repository's URL keeps its credentials there.
func TestGitStore_CacheIsPrivate(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git")
	}
	repo := t.TempDir()
	os.MkdirAll(filepath.Join(repo, "tdd"), 0o755)
	os.WriteFile(filepath.Join(repo, "tdd", "SKILL.md"), []byte("---\nname: tdd\nruns: true\n---\nRED."), 0o644)
	for _, args := range [][]string{
		{"init", "--quiet", "-b", "main"},
		{"add", "."},
		{"-c", "user.email=a@b", "-c", "user.name=a", "commit", "--quiet", "-m", "first"},
	} {
		if out, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	cache := filepath.Join(t.TempDir(), "cache")
	if err := os.Mkdir(cache, 0o755); err != nil {
		t.Fatal(err)
	}
	s := &GitStore{RepoURL: repo, Branch: "main", CacheDir: cache}
	for _, step := range []string{"clone", "pull"} {
		skills, err := s.LoadAll(context.Background())
		if err != nil || len(skills) != 1 || !skills[0].Runs {
			t.Fatalf("%s: %+v %v", step, skills, err)
		}
		if fi, err := os.Stat(cache); err != nil || fi.Mode().Perm() != 0o700 {
			t.Errorf("%s: cache mode %v %v", step, fi.Mode(), err)
		}
		os.Chmod(cache, 0o755) // an earlier version's
	}
	if v := Version(context.Background(), s); len(v) != 40 {
		t.Errorf("version %q", v)
	}
}
