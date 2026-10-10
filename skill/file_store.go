package skill

import (
	"context"
	"os"
	"path/filepath"
)

// FileStore loads skills from a local directory.
type FileStore struct {
	Dir string
}

func (s *FileStore) LoadAll(_ context.Context) ([]Skill, error) {
	entries, err := os.ReadDir(s.Dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var skills []Skill
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		skillFile := filepath.Join(s.Dir, entry.Name(), "SKILL.md")
		data, err := os.ReadFile(skillFile)
		if err != nil {
			continue
		}

		fm, body := parseFrontmatter(string(data))
		name := fm.name
		if name == "" {
			name = entry.Name()
		}

		skills = append(skills, Skill{
			Name:        name,
			Description: fm.description,
			Content:     body,
			Path:        skillFile,
			Runs:        fm.runs,
		})
	}

	return skills, nil
}
