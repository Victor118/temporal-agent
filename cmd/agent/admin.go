package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/skill"
)

// skillSource is where a process loads its skills from: SKILLS_DIR, a
// directory read as it is (no secret: the coding containers' source), or
// SKILLS_REPO, a git repository cloned into cache, a directory of the
// process's own; never both, which stops the process rather than pick one.
// dir is the directory used when neither is set ("" = none: nil, "").
// source says where they come from, for the log and the back-office.
func skillSource(cfg *config.Config, cache, dir string) (store skill.Store, source string, err error) {
	switch {
	case cfg.SkillsRepo != "" && cfg.SkillsDir != "":
		return nil, "", fmt.Errorf("SKILLS_REPO and SKILLS_DIR are both set: the skills come from one of them, unset the other")
	case cfg.SkillsDir != "":
		dir = cfg.SkillsDir
	case cfg.SkillsRepo != "":
		return &skill.GitStore{RepoURL: cfg.SkillsRepo, Branch: cfg.SkillsBranch, CacheDir: filepath.Join(os.TempDir(), cache)},
			cfg.SkillsRepo + "@" + cfg.SkillsBranch, nil
	}
	if dir == "" {
		return nil, "", nil
	}
	return &skill.FileStore{Dir: dir}, dir, nil
}

// serverSkills loads the skills the back-office shows, from src (nil = none),
// reloaded when skills_version moves, as the workers do. Loading runs in the
// background so an unreachable repo never delays the API.
func serverSkills(ctx context.Context, src skill.Store, source string, st skillsVersionSource) (get func() []skill.Skill) {
	var current atomic.Pointer[[]skill.Skill]
	get = func() []skill.Skill {
		if p := current.Load(); p != nil {
			return *p
		}
		return nil
	}
	if src == nil {
		return get
	}
	load := func() {
		loadCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		skills, err := src.LoadAll(loadCtx)
		if err != nil {
			log.Printf("Warning: failed to load skills from %s: %v", source, err)
			return
		}
		current.Store(&skills)
		log.Printf("Server loaded %d skills from %s", len(skills), source)
	}
	go func() {
		load()
		watchSkillsVersionDB(ctx, st, 30*time.Second, load)
	}()
	return get
}
