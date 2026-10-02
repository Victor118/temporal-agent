package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/sse"
)

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the HTTP API server (no Temporal worker)",
	Run:   runServer,
}

func runServer(cmd *cobra.Command, args []string) {
	cfg := config.Load()

	// Temporal client (for starting/signaling/querying workflows)
	temporalClient := dialTemporal(cfg)
	defer temporalClient.Close()

	// SSE hub
	hub := sse.NewHub()

	st := openStore(cfg)
	defer st.Close()

	// Seed agents missing from the DB (the DB is the source of truth)
	if err := seedAgents(st, cfg.AgentsFile); err != nil {
		log.Fatalf("Failed to seed agents: %v", err)
	}

	// Back-office skills: the server and the workers reload the repo when
	// skills_version moves.
	skills, skillsSource := serverSkills(context.Background(), cfg, st)
	handler := newHTTPHandler(cfg, st, temporalClient, hub, httpOptions{
		skills:           skills,
		skillsSource:     skillsSource,
		skillsReloadable: cfg.SkillsRepo != "",
	})

	// Internal API (receives SSE notifications from workers)
	internalRouter := chi.NewRouter()
	internalRouter.Post("/internal/notify", handleInternalNotify(hub, cfg.InternalAPIKey))
	if cfg.InternalAPIKey == "" {
		log.Println("Warning: INTERNAL_API_KEY is not set, the internal API refuses every worker notification")
	}

	publicSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: handler}
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
		waitForSignal()
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
