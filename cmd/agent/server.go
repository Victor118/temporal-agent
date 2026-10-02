package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/web/admin"
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

	// Store
	st, err := store.NewPostgresStore(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to init store: %v", err)
	}
	defer st.Close()

	// Seed agents missing from the DB (the DB is the source of truth)
	if err := seedAgents(st, cfg.AgentsFile); err != nil {
		log.Fatalf("Failed to seed agents: %v", err)
	}

	// Back-office
	skills, skillsSource := serverSkills(context.Background(), cfg, st)
	authSvc := &auth.Service{Store: st}
	adminUI := admin.New(admin.Config{
		Auth:         authSvc,
		Store:        st,
		Temporal:     temporalClient,
		Skills:       skills,
		SkillsSource: skillsSource,
		// The server and the workers reload the repo when skills_version moves.
		SkillsReloadable: cfg.SkillsRepo != "",
		DefaultAgentID:   cfg.DefaultAgentID,
		WorkflowQueue:    cfg.WorkflowQueue,
	})

	// Handler
	h := &handler{
		auth:           authSvc,
		temporalClient: temporalClient,
		hub:            hub,
		cfg:            cfg,
		store:          st,
	}

	// Internal API (receives SSE notifications from workers)
	internalRouter := chi.NewRouter()
	internalRouter.Post("/internal/notify", handleInternalNotify(hub))

	publicSrv := &http.Server{Addr: cfg.HTTPAddr, Handler: publicRouter(h, adminUI)}
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
