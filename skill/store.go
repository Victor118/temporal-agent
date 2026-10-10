package skill

import "context"

// Store loads skills from a source (filesystem, git repo, etc.).
type Store interface {
	// LoadAll returns all available skills.
	LoadAll(ctx context.Context) ([]Skill, error)
}

// Version is what the skills s loaded last were taken from, when s can say
// (a repository's commit, GitStore); empty otherwise.
func Version(ctx context.Context, s Store) string {
	if v, ok := s.(interface{ Version(context.Context) string }); ok {
		return v.Version(ctx)
	}
	return ""
}
