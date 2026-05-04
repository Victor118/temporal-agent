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
	"github.com/go-chi/chi/v5/middleware"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
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

	// Store
	st, err := store.NewPostgresStore(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to init store: %v", err)
	}
	defer st.Close()

	// Handler
	h := &handler{
		temporalClient: temporalClient,
		hub:            hub,
		cfg:            cfg,
		store:          st,
	}

	// Public API
	publicRouter := chi.NewRouter()
	publicRouter.Use(middleware.Logger)
	publicRouter.Use(middleware.Recoverer)
	publicRouter.Use(corsMiddleware)

	// Unauthenticated routes
	publicRouter.Get("/", web.HandleIndex)
	publicRouter.Post("/auth/login", h.login)
	publicRouter.Post("/auth/logout", h.logout)
	publicRouter.Post("/webhooks/skills", h.handleSkillsWebhook)
	publicRouter.Post("/webhooks/telegram", h.handleTelegramWebhook)

	// Authenticated routes
	publicRouter.Group(func(r chi.Router) {
		r.Use(authMiddleware(cfg))

		r.Get("/auth/check", h.checkAuth)
		r.Post("/sessions", h.createSession)
		r.Get("/users/{userID}/sessions", h.listSessions)
		r.Get("/users/{userID}/notifications", h.getNotifications)
		r.Get("/users/{userID}/notifications/stream", h.streamNotifications)
		r.Delete("/users/{userID}/notifications/{notifID}", h.deleteNotification)
		r.Delete("/users/{userID}/notifications", h.deleteAllNotifications)
		r.Post("/sessions/{id}/messages", h.sendMessage)
		r.Post("/sessions/{id}/cancel", h.cancelAgent)
		r.Delete("/sessions/{id}", h.deleteSession)
		r.Get("/sessions/{id}/state", h.getState)
		r.Get("/sessions/{id}/history", h.getHistory)
		r.Get("/sessions/{id}/stream", h.stream)
		r.Post("/sessions/{id}/answer", h.answerQuestion)

		// Admin routes
		r.Get("/admin/queues", h.listKnownQueues)
		r.Get("/admin/activity-queues", h.listActivityQueues)
		r.Put("/admin/activity-queues", h.setActivityQueue)
		r.Delete("/admin/activity-queues/{activityName}", h.deleteActivityQueue)
	})

	// Internal API (receives SSE notifications from workers)
	internalRouter := chi.NewRouter()
	internalRouter.Post("/internal/notify", handleInternalNotify(hub))

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
