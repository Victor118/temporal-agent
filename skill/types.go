package skill

type Skill struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Content     string `json:"content,omitempty"` // Full markdown body, loaded on demand
	Path        string `json:"-"`
	// Runs: the skill goes with the coding runs of the agents that have it
	// (analyze_repo, implement_feature), to their CLI. Set by "runs: true"
	// in its frontmatter, and nothing else.
	Runs bool `json:"runs,omitempty"`
}
