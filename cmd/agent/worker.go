package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/skill"
)

var workerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Start the Temporal worker (no HTTP server)",
	Run:   runWorker,
}

// runWorker serves Temporal only. Its notifications go to the server's
// internal API, and its skills come from the skills repository, reloaded when
// skills_version moves.
func runWorker(cmd *cobra.Command, args []string) {
	cfg := config.Load()

	st := openStore(cfg)
	defer st.Close()

	temporalClient := dialTemporal(cfg)
	defer temporalClient.Close()

	// Notification bridge: POST to server's internal endpoint (SSE requires HTTP)
	notifier := activity.NewHTTPNotifier(cfg.NotifyURL, cfg.InternalAPIKey)
	if cfg.InternalAPIKey == "" {
		log.Println("Warning: INTERNAL_API_KEY is not set, the server will refuse this worker's notifications")
	}

	opts := workerOptions{hub: notifier}
	if cfg.SkillsRepo != "" {
		opts.skills = &skill.GitStore{
			RepoURL:  cfg.SkillsRepo,
			Branch:   cfg.SkillsBranch,
			CacheDir: filepath.Join(os.TempDir(), "temporal-agent-skills-worker"),
		}
		opts.watchSkills = true
	} else {
		log.Println("No skills repo configured (SKILLS_REPO), running without skills")
	}

	rt, err := newWorkerRuntime(cfg, st, temporalClient, opts)
	if err != nil {
		log.Fatalf("Worker: %v", err)
	}
	if err := rt.start(); err != nil {
		log.Fatalf("Worker failed: %v", err)
	}
	log.Printf("Worker running on queues %v", rt.queues)
	waitForSignal()
	log.Println("Shutting down workers...")
	rt.shutdown()
}

// activityQueueSource holds the activity → task queue mapping.
type activityQueueSource interface {
	GetActivityQueueMap(ctx context.Context) (map[string]string, error)
}

// skillsVersionSource holds the version that tells when to reload the skills.
type skillsVersionSource interface {
	GetSkillsVersion(ctx context.Context) (int64, error)
}

// loadActivityQueuesFromDB reads the activity → task queue mapping from PostgreSQL.
func loadActivityQueuesFromDB(st activityQueueSource) map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	m, err := st.GetActivityQueueMap(ctx)
	if err != nil {
		log.Printf("Warning: failed to load activity queue mapping from DB: %v", err)
		return nil
	}
	if len(m) > 0 {
		log.Printf("Loaded activity queue mapping from DB: %v", m)
	}
	return m
}

// pollActivityQueues periodically refreshes the activity → task queue mapping from DB.
func pollActivityQueues(ctx context.Context, st activityQueueSource, cfg *activity.WorkerConfig, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cfg.SetActivityQueues(loadActivityQueuesFromDB(st))
		}
	}
}

// watchSkillsVersionDB polls PostgreSQL for skills version changes.
func watchSkillsVersionDB(ctx context.Context, st skillsVersionSource, interval time.Duration, onReload func()) {
	var currentVersion int64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			v, err := st.GetSkillsVersion(ctx)
			if err != nil {
				log.Printf("Warning: failed to poll skills version from DB: %v", err)
				continue
			}
			if v > currentVersion {
				currentVersion = v
				log.Printf("Skills version changed to %d, reloading...", v)
				onReload()
			}
		}
	}
}
