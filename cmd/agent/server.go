package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/web"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the HTTP API server (no Temporal worker)",
	Run:   runServer,
}

func runServer(cmd *cobra.Command, args []string) {
	cfg := config.Load()

	// Temporal client (for starting/signaling/querying workflows)
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHost,
		Namespace: cfg.TemporalNamespace,
	})
	if err != nil {
		log.Fatalf("Failed to connect to Temporal: %v", err)
	}
	defer temporalClient.Close()

	// SSE hub
	hub := sse.NewHub()

	// Skills store
	var skillStore skill.Store
	if cfg.SkillsRepo != "" {
		skillStore = &skill.GitStore{
			RepoURL:  cfg.SkillsRepo,
			Branch:   cfg.SkillsBranch,
			CacheDir: filepath.Join(os.TempDir(), "temporal-agent-skills"),
		}
	}

	// Handler
	h := &handler{
		temporalClient: temporalClient,
		hub:            hub,
		cfg:            cfg,
		skillStore:     skillStore,
	}

	// Load initial skills (for version tracking — prompts are built by workers)
	if skillStore != nil {
		if err := h.reloadSkills(); err != nil {
			log.Printf("Warning: failed to load skills from repo: %v", err)
		}
	} else {
		log.Println("No skills repo configured (SKILLS_REPO), running without skills")
	}

	// Public API
	publicRouter := chi.NewRouter()
	publicRouter.Use(middleware.Logger)
	publicRouter.Use(middleware.Recoverer)
	publicRouter.Use(corsMiddleware)

	publicRouter.Get("/", web.HandleIndex)
	publicRouter.Post("/sessions", h.createSession)
	publicRouter.Post("/sessions/{id}/messages", h.sendMessage)
	publicRouter.Get("/sessions/{id}/state", h.getState)
	publicRouter.Get("/sessions/{id}/stream", h.stream)
	publicRouter.Post("/sessions/{id}/answer", h.answerQuestion)
	publicRouter.Post("/webhooks/skills", h.handleSkillsWebhook)

	// Internal API (receives notifications from workers + skills version)
	internalRouter := chi.NewRouter()
	internalRouter.Post("/internal/notify", handleInternalNotify(hub))
	internalRouter.Get("/internal/skills/version", h.handleSkillsVersion)
	internalRouter.Post("/internal/workers/register", h.handleRegisterWorker)
	internalRouter.Get("/internal/agents", h.handleGetAgents)

	publicSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: publicRouter}
	internalSrv := &http.Server{Addr: cfg.InternalAddr, Handler: internalRouter}

	// Start both servers
	go func() {
		log.Printf("Internal API listening on %s", cfg.InternalAddr)
		if err := internalSrv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Internal server error: %v", err)
		}
	}()

	// Graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down server...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		publicSrv.Shutdown(ctx)
		internalSrv.Shutdown(ctx)
	}()

	log.Printf("Public API listening on %s", cfg.HTTPAddr)
	if err := publicSrv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
}
