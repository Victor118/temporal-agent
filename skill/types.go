package skill

type Skill struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Content     string `json:"content,omitempty"` // Full markdown body, loaded on demand
	Path        string `json:"-"`
}
