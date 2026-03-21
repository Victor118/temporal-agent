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

	if _, err := os.Stat(filepath.Join(s.CacheDir, ".git")); os.IsNotExist(err) {
		return s.clone(ctx, branch)
	}
	return s.pull(ctx, branch)
}

func (s *GitStore) clone(ctx context.Context, branch string) error {
	args := []string{"clone", "--depth", "1", "--branch", branch, s.RepoURL, s.CacheDir}
	return s.git(ctx, args...)
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
