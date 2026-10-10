package main

import (
	"testing"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/skill"
)

// The skills come from SKILLS_DIR or SKILLS_REPO, never both; a mode's own
// directory only when neither is set.
func TestSkillSource(t *testing.T) {
	if _, _, err := skillSource(&config.Config{SkillsRepo: "https://example.com/s.git", SkillsDir: "/app/skills"}, "c", ""); err == nil {
		t.Error("both: accepted")
	}
	s, source, err := skillSource(&config.Config{SkillsDir: "/app/skills"}, "c", "./skills")
	if fs, ok := s.(*skill.FileStore); err != nil || !ok || fs.Dir != "/app/skills" || source != "/app/skills" {
		t.Errorf("SKILLS_DIR: %#v %q %v", s, source, err)
	}
	s, source, err = skillSource(&config.Config{SkillsRepo: "https://example.com/s.git", SkillsBranch: "main"}, "c", "./skills")
	if gs, ok := s.(*skill.GitStore); err != nil || !ok || gs.RepoURL != "https://example.com/s.git" || source != "https://example.com/s.git@main" {
		t.Errorf("SKILLS_REPO: %#v %q %v", s, source, err)
	}
	s, source, err = skillSource(&config.Config{}, "c", "./skills")
	if fs, ok := s.(*skill.FileStore); err != nil || !ok || fs.Dir != "./skills" || source != "./skills" {
		t.Errorf("dev's default: %#v %q %v", s, source, err)
	}
	if s, _, err := skillSource(&config.Config{}, "c", ""); err != nil || s != nil {
		t.Errorf("none: %#v %v", s, err)
	}
}
