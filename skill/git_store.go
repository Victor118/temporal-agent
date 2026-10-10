package skill

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// GitStore loads skills by cloning/pulling a git repository.
type GitStore struct {
	RepoURL  string
	Branch   string
	CacheDir string // local clone path
}

func (s *GitStore) LoadAll(ctx context.Context) ([]Skill, error) {
	if err := s.sync(ctx); err != nil {
		return nil, fmt.Errorf("git sync: %w", err)
	}

	// Delegate to filesystem scan of the cloned repo
	fs := &FileStore{Dir: s.CacheDir}
	return fs.LoadAll(ctx)
}

// sync clones the repo if not present, otherwise pulls latest changes.
func (s *GitStore) sync(ctx context.Context) error {
	branch := s.Branch
	if branch == "" {
		branch = "main"
	}

	// The process's own, before a clone as before a pull: a private
	// repository's URL keeps its credentials in .git/config, which a coding
	// run's user (RUN_AS_UID) must not read, and a cache an earlier version
	// left open is closed.
	if err := os.MkdirAll(s.CacheDir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(s.CacheDir, 0o700); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(s.CacheDir, ".git")); os.IsNotExist(err) {
		return s.clone(ctx, branch)
	}
	return s.pull(ctx, branch)
}

// clone clones the repository into CacheDir, empty and 0700 (sync).
func (s *GitStore) clone(ctx context.Context, branch string) error {
	args := []string{"clone", "--depth", "1", "--branch", branch, s.RepoURL, s.CacheDir}
	return s.git(ctx, args...)
}

// Version is the commit the skills were last loaded from: what a coding run
// says it took them from (docs/design/run-skills.md §7). Empty when it
// cannot be read.
func (s *GitStore) Version(ctx context.Context) string {
	cmd := exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	cmd.Dir = s.CacheDir
	cmd.Env = s.gitEnv()
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func (s *GitStore) pull(ctx context.Context, branch string) error {
	if err := s.gitInRepo(ctx, "fetch", "origin", branch); err != nil {
		return err
	}
	return s.gitInRepo(ctx, "reset", "--hard", "origin/"+branch)
}

func (s *GitStore) git(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = s.gitEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %s: %w", strings.Join(args, " "), string(out), err)
	}
	return nil
}

func (s *GitStore) gitInRepo(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = s.CacheDir
	cmd.Env = s.gitEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %s: %w", strings.Join(args, " "), string(out), err)
	}
	return nil
}

func (s *GitStore) gitEnv() []string {
	env := os.Environ()
	// Disable interactive prompts
	env = append(env, "GIT_TERMINAL_PROMPT=0")
	return env
}
