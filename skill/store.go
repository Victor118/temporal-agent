package skill

import "context"

// Store loads skills from a source (filesystem, git repo, etc.).
type Store interface {
	// LoadAll returns all available skills.
	LoadAll(ctx context.Context) ([]Skill, error)
}
