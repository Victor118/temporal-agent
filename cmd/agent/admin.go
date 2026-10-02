package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/skill"
)

// serverSkills loads the skills the back-office shows: from the skills repo if
// one is configured, reloaded when skills_version moves, as the workers do.
// Loading runs in the background so an unreachable repo never delays the API.
// Without a repo the server holds no skill, and source is "".
func serverSkills(ctx context.Context, cfg *config.Config, st skillsVersionSource) (get func() []skill.Skill, source string) {
	var current atomic.Pointer[[]skill.Skill]
	get = func() []skill.Skill {
		if p := current.Load(); p != nil {
			return *p
		}
		return nil
	}
	if cfg.SkillsRepo == "" {
		return get, ""
	}

	skillStore := &skill.GitStore{
		RepoURL:  cfg.SkillsRepo,
		Branch:   cfg.SkillsBranch,
		CacheDir: filepath.Join(os.TempDir(), "temporal-agent-skills-server"),
	}
	load := func() {
		loadCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		skills, err := skillStore.LoadAll(loadCtx)
		if err != nil {
			log.Printf("Warning: failed to load skills from repo: %v", err)
			return
		}
		current.Store(&skills)
		log.Printf("Server loaded %d skills from %s", len(skills), cfg.SkillsRepo)
	}
	go func() {
		load()
		watchSkillsVersionDB(ctx, st, 30*time.Second, load)
	}()
	return get, cfg.SkillsRepo + "@" + cfg.SkillsBranch
}
