package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/skill"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/tool"
	"github.com/victor/temporal-agent/workflow"
)

// AgentInfo describes a specialized agent available on a task queue.
type AgentInfo struct {
	TaskQueue string   `json:"task_queue"`
	Skills    []string `json:"skills"`
}

type handler struct {
	temporalClient client.Client
	hub            *sse.Hub
	cfg            *config.Config

	// Skills hot-reload
	skillStore    skill.Store
	skillsMu      sync.RWMutex
	skills        []skill.Skill
	skillsVersion atomic.Int64
	registry      *tool.Registry

	// Agent catalog — aggregated from worker registrations
	catalogMu sync.RWMutex
	catalog   map[string]AgentInfo // task queue → agent info
}

type createSessionRequest struct {
	SystemPrompt string `json:"system_prompt,omitempty"`
	Model        string `json:"model,omitempty"`
}

type createSessionResponse struct {
	SessionID string `json:"session_id"`
}

func (h *handler) createSession(w http.ResponseWriter, r *http.Request) {
	var req createSessionRequest
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}

	sessionID := newUUID()
	model := req.Model
	if model == "" {
		model = h.cfg.LLMModel
	}

	_, err := h.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        "session-" + sessionID,
		TaskQueue: h.cfg.PrimaryTaskQueue(),
	}, workflow.SessionWorkflow, workflow.SessionWorkflowInput{
		SessionID:    sessionID,
		SystemPrompt: req.SystemPrompt, // Optional override; empty = load from task queue skills
		Model:        model,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create session: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(createSessionResponse{SessionID: sessionID})
}

type sendMessageRequest struct {
	Content string `json:"content"`
}

func (h *handler) sendMessage(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")

	var req sendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	err := h.temporalClient.SignalWorkflow(r.Context(), "session-"+sessionID, "", workflow.SignalUserMessage, req.Content)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to send message: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (h *handler) getState(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")

	resp, err := h.temporalClient.QueryWorkflow(r.Context(), "session-"+sessionID, "", workflow.QuerySessionState)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to query state: %v", err), http.StatusInternalServerError)
		return
	}

	var state workflow.SessionState
	if err := resp.Get(&state); err != nil {
		http.Error(w, fmt.Sprintf("Failed to decode state: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(state)
}

func (h *handler) stream(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := h.hub.Subscribe(sessionID)
	defer h.hub.Unsubscribe(sessionID, ch)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, string(event.Data))
			flusher.Flush()
		}
	}
}

type answerRequest struct {
	WorkflowID string `json:"workflow_id"`
	Answer     string `json:"answer"`
}

func (h *handler) answerQuestion(w http.ResponseWriter, r *http.Request) {
	var req answerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.WorkflowID == "" || req.Answer == "" {
		http.Error(w, "workflow_id and answer are required", http.StatusBadRequest)
		return
	}

	err := h.temporalClient.SignalWorkflow(r.Context(), req.WorkflowID, "", workflow.SignalUserAnswer, req.Answer)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to send answer: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleRegisterWorker registers a worker's task queue and skills in the catalog.
func (h *handler) handleRegisterWorker(w http.ResponseWriter, r *http.Request) {
	var req AgentInfo
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.TaskQueue == "" {
		http.Error(w, "task_queue is required", http.StatusBadRequest)
		return
	}

	h.catalogMu.Lock()
	if h.catalog == nil {
		h.catalog = make(map[string]AgentInfo)
	}
	h.catalog[req.TaskQueue] = req
	h.catalogMu.Unlock()

	log.Printf("Worker registered: queue %q with skills %v", req.TaskQueue, req.Skills)
	w.WriteHeader(http.StatusNoContent)
}

// handleGetAgents returns the full catalog of available specialized agents.
func (h *handler) handleGetAgents(w http.ResponseWriter, r *http.Request) {
	h.catalogMu.RLock()
	agents := make([]AgentInfo, 0, len(h.catalog))
	for _, a := range h.catalog {
		agents = append(agents, a)
	}
	h.catalogMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(agents)
}

// handleInternalNotify receives SSE events from workers and publishes them to the local hub.
func handleInternalNotify(hub *sse.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input activity.NotifyInput
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		hub.Publish(input.SessionID, input.Event)
		w.WriteHeader(http.StatusNoContent)
	}
}

// reloadSkills fetches skills from the store and increments the version counter.
func (h *handler) reloadSkills() error {
	if h.skillStore == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	skills, err := h.skillStore.LoadAll(ctx)
	if err != nil {
		return fmt.Errorf("reload skills: %w", err)
	}

	h.skillsMu.Lock()
	h.skills = skills
	h.skillsMu.Unlock()

	h.skillsVersion.Add(1)
	log.Printf("Skills reloaded: %d skills (version %d)", len(skills), h.skillsVersion.Load())
	return nil
}

// handleSkillsWebhook handles GitHub webhook pushes to reload skills.
// Verifies the X-Hub-Signature-256 header against the configured secret.
func (h *handler) handleSkillsWebhook(w http.ResponseWriter, r *http.Request) {
	if h.skillStore == nil {
		http.Error(w, "No skills repo configured", http.StatusNotFound)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	if secret := h.cfg.SkillsWebhookSecret; secret != "" {
		signature := r.Header.Get("X-Hub-Signature-256")
		if !verifyGitHubSignature(body, signature, secret) {
			http.Error(w, "Invalid signature", http.StatusUnauthorized)
			return
		}
	}

	if err := h.reloadSkills(); err != nil {
		log.Printf("Error reloading skills from webhook: %v", err)
		http.Error(w, "Failed to reload skills", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// verifyGitHubSignature checks the HMAC-SHA256 signature sent by GitHub.
// The header format is "sha256=<hex digest>".
func verifyGitHubSignature(payload []byte, signature, secret string) bool {
	if !strings.HasPrefix(signature, "sha256=") {
		return false
	}

	got, err := hex.DecodeString(signature[len("sha256="):])
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	expected := mac.Sum(nil)

	return hmac.Equal(got, expected)
}

// handleSkillsVersion returns the current skills version for worker polling.
func (h *handler) handleSkillsVersion(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{"version": h.skillsVersion.Load()})
}

// watchSkillsVersion polls the server for skill version changes and reloads when needed.
// Used by workers in production mode.
func watchSkillsVersion(ctx context.Context, serverURL string, interval time.Duration, onReload func()) {
	var currentVersion int64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			v, err := fetchSkillsVersion(ctx, serverURL)
			if err != nil {
				log.Printf("Warning: failed to poll skills version: %v", err)
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

func fetchSkillsVersion(ctx context.Context, serverURL string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", serverURL+"/internal/skills/version", nil)
	if err != nil {
		return 0, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result struct {
		Version int64 `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}
	return result.Version, nil
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
