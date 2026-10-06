package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/sse"
)

var devCmd = &cobra.Command{
	Use:   "dev",
	Short: "Start both server and worker in a single process (for development)",
	Run:   runDev,
}

// runDev is a server and a worker in one process: the workers publish to the
// server's own SSE hub, and the skills come from ./skills.
func runDev(cmd *cobra.Command, args []string) {
	cfg := config.Load()

	st := openStore(cfg)
	defer st.Close()

	// Agents — dev mode seeds the DB like the server, then reads the catalog
	// (agents + tools) from it
	if err := seedAgents(st, cfg.AgentsFile); err != nil {
		log.Fatalf("Failed to seed agents: %v", err)
	}

	temporalClient := dialTemporal(cfg)
	defer temporalClient.Close()

	// SSE hub (in-memory, shared between server and worker)
	hub := sse.NewHub()

	// Skills — dev mode loads from the local filesystem, and the back-office
	// shows the same ones.
	skillStore := &skill.FileStore{Dir: "./skills"}
	// The machines' gateway is in this process: directives reach it in
	// memory, like the notifications reach the hub.
	machines := newGateway(cfg, st, temporalClient, hub)
	opts := workerOptions{web: activity.HubNotifier{Hub: hub}, skills: skillStore, machines: st}
	if machines != nil {
		opts.handoff = machines
	}
	rt, err := newWorkerRuntime(cfg, st, temporalClient, opts)
	if err != nil {
		log.Fatalf("Worker: %v", err)
	}
	if err := rt.start(); err != nil {
		log.Fatalf("Worker failed: %v", err)
	}
	srv := newHTTPServer(cfg.HTTPAddr, newHTTPHandler(cfg, st, temporalClient, hub, httpOptions{
		skills:       func() []skill.Skill { return rt.skills },
		skillsSource: skillStore.Dir,
		machines:     machines,
	}))

	go func() {
		waitForSignal()
		log.Println("Shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		if machines != nil {
			machines.Close()
		}
		rt.shutdown()
	}()

	log.Printf("Dev mode: API on %s, workers on queues %v", cfg.HTTPAddr, rt.queues)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("HTTP server error: %v", err)
	}
}
