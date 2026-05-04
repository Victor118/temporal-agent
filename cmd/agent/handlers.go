package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
	"github.com/victor/temporal-agent/workflow"
)

type handler struct {
	temporalClient client.Client
	hub            *sse.Hub
	cfg            *config.Config
	registry       *tool.Registry
	store          store.Store
}

// --- Auth ---

const sessionCookieName = "session_token"

// sessionStore holds active session tokens in memory.
// Tokens are lost on restart — users simply re-login.
var (
	sessions   = make(map[string]bool)
	sessionsMu sync.RWMutex
)

func generateToken() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

type loginRequest struct {
	APIKey string `json:"api_key"`
}

func (h *handler) login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.APIKey != h.cfg.APIKey {
		http.Error(w, "Invalid API key", http.StatusUnauthorized)
		return
	}

	token := generateToken()
	sessionsMu.Lock()
	sessions[token] = true
	sessionsMu.Unlock()

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   7 * 24 * 60 * 60, // 7 days
	})

	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) logout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(sessionCookieName)
	if err == nil {
		sessionsMu.Lock()
		delete(sessions, cookie.Value)
		sessionsMu.Unlock()
	}

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
	})

	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) checkAuth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// authMiddleware checks for a valid session cookie.
// Skips auth if API_KEY is not configured (dev mode).
func authMiddleware(cfg *config.Config) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// No API key configured → auth disabled
			if cfg.APIKey == "" {
				next.ServeHTTP(w, r)
				return
			}

			cookie, err := r.Cookie(sessionCookieName)
			if err != nil {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			sessionsMu.RLock()
			valid := sessions[cookie.Value]
			sessionsMu.RUnlock()

			if !valid {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

type createSessionRequest struct {
	UserID       string `json:"user_id,omitempty"`
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

	userID := req.UserID
	if userID == "" {
		userID = "anonymous"
	}

	sessionID := newUUID()
	model := req.Model
	if model == "" {
		model = h.cfg.LLMModel
	}

	taskQueue := h.cfg.PrimaryTaskQueue()

	_, err := h.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        "session-" + sessionID,
		TaskQueue: taskQueue,
	}, workflow.SessionWorkflow, workflow.SessionWorkflowInput{
		SessionID:    sessionID,
		UserID:       userID,
		SystemPrompt: req.SystemPrompt, // Optional override; empty = load from task queue skills
		Model:        model,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create session: %v", err), http.StatusInternalServerError)
		return
	}

	// Persist session metadata
	if err := h.store.CreateSession(r.Context(), store.Session{
		SessionID: sessionID,
		UserID:    userID,
		TaskQueue: taskQueue,
		Channel:   "web",
	}); err != nil {
		log.Printf("Warning: failed to persist session: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(createSessionResponse{SessionID: sessionID})
}

func (h *handler) listSessions(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "userID")

	sessions, err := h.store.ListSessionsByUser(r.Context(), userID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list sessions: %v", err), http.StatusInternalServerError)
		return
	}

	// Check workflow status for each session
	type sessionEntry struct {
		store.Session
		Active bool `json:"active"`
	}
	entries := make([]sessionEntry, 0, len(sessions))
	for _, s := range sessions {
		active := h.isSessionActive(r.Context(), s.SessionID)
		entries = append(entries, sessionEntry{Session: s, Active: active})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

func (h *handler) deleteSession(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")

	// Terminate any running Temporal workflows for this session
	workflowID := h.findActiveWorkflowID(r.Context(), sessionID)
	if workflowID != "" {
		_ = h.temporalClient.TerminateWorkflow(r.Context(), workflowID, "", "session deleted by user")
	}

	if err := h.store.DeleteSession(r.Context(), sessionID); err != nil {
		http.Error(w, fmt.Sprintf("Failed to delete session: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// isWorkflowRunning checks if a Temporal workflow is still running.
func (h *handler) isWorkflowRunning(ctx context.Context, workflowID string) bool {
	desc, err := h.temporalClient.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		return false
	}
	status := desc.WorkflowExecutionInfo.Status
	return status == 1 // WORKFLOW_EXECUTION_STATUS_RUNNING
}

// isSessionActive checks if any workflow for this session is still running.
// Handles both "session-{id}" and "session-{id}-{timestamp}" workflow IDs.
func (h *handler) isSessionActive(ctx context.Context, sessionID string) bool {
	// First check the original workflow ID
	if h.isWorkflowRunning(ctx, "session-"+sessionID) {
		return true
	}
	// Check for resumed workflows by listing with a query
	resp, err := h.temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: h.cfg.TemporalNamespace,
		Query:     fmt.Sprintf("WorkflowId STARTS_WITH 'session-%s' AND ExecutionStatus = 'Running'", sessionID),
		PageSize:  1,
	})
	if err != nil {
		return false
	}
	return len(resp.Executions) > 0
}

// findActiveWorkflowID returns the workflow ID for the running workflow of a session.
func (h *handler) findActiveWorkflowID(ctx context.Context, sessionID string) string {
	base := "session-" + sessionID
	if h.isWorkflowRunning(ctx, base) {
		return base
	}
	resp, err := h.temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: h.cfg.TemporalNamespace,
		Query:     fmt.Sprintf("WorkflowId STARTS_WITH 'session-%s' AND ExecutionStatus = 'Running'", sessionID),
		PageSize:  1,
	})
	if err != nil || len(resp.Executions) == 0 {
		return ""
	}
	return resp.Executions[0].Execution.WorkflowId
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

	// Find the active workflow for this session, or restart if none
	workflowID := h.findActiveWorkflowID(r.Context(), sessionID)
	if workflowID == "" {
		newWorkflowID := fmt.Sprintf("session-%s-%d", sessionID, time.Now().Unix())
		_, err := h.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
			ID:        newWorkflowID,
			TaskQueue: h.cfg.PrimaryTaskQueue(),
		}, workflow.SessionWorkflow, workflow.SessionWorkflowInput{
			SessionID: sessionID,
			UserID:    func() string { u, _ := h.store.GetSessionUser(r.Context(), sessionID); return u }(),
			Model:     h.cfg.LLMModel,
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to resume session: %v", err), http.StatusInternalServerError)
			return
		}
		workflowID = newWorkflowID
		log.Printf("Session %s resumed with workflow %s", sessionID, workflowID)
	}

	err := h.temporalClient.SignalWorkflow(r.Context(), workflowID, "", workflow.SignalUserMessage, req.Content)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to send message: %v", err), http.StatusInternalServerError)
		return
	}

	// Set session title from first message (only if title is still empty)
	go func() {
		title := req.Content
		if len(title) > 80 {
			title = title[:80] + "..."
		}
		h.store.UpdateSessionTitle(context.Background(), sessionID, title)
	}()

	w.WriteHeader(http.StatusAccepted)
}

func (h *handler) cancelAgent(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")

	workflowID := h.findActiveWorkflowID(r.Context(), sessionID)
	if workflowID == "" {
		http.Error(w, "No active session found", http.StatusNotFound)
		return
	}

	err := h.temporalClient.SignalWorkflow(r.Context(), workflowID, "", workflow.SignalCancelAgent, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to cancel agent: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (h *handler) getHistory(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")

	messages, err := h.store.LoadMessages(r.Context(), sessionID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to load messages: %v", err), http.StatusInternalServerError)
		return
	}

	// Convert store messages to a frontend-friendly format
	type historyEntry struct {
		Type      string      `json:"type"`                 // "message", "tool_calls"
		Role      string      `json:"role,omitempty"`       // "user", "assistant"
		Content   string      `json:"content,omitempty"`
		ToolCalls interface{} `json:"tool_calls,omitempty"`
	}

	var history []historyEntry
	for _, msg := range messages {
		// Content is stored as a JSON-encoded string (e.g. "\"hello\""), decode it
		content := msg.Content
		var decoded string
		if json.Unmarshal([]byte(content), &decoded) == nil {
			content = decoded
		}

		switch msg.Role {
		case store.RoleUser:
			history = append(history, historyEntry{Type: "message", Role: "user", Content: content})
		case store.RoleAssistant:
			if content != "" {
				history = append(history, historyEntry{Type: "message", Role: "assistant", Content: content})
			}
			if len(msg.ToolCalls) > 0 {
				type tc struct {
					Name  string          `json:"name"`
					Input json.RawMessage `json:"input"`
				}
				calls := make([]tc, len(msg.ToolCalls))
				for i, t := range msg.ToolCalls {
					calls[i] = tc{Name: t.Name, Input: t.Input}
				}
				history = append(history, historyEntry{Type: "tool_calls", ToolCalls: calls})
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(history)
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

// getNotifications returns the notification history for a user.
func (h *handler) getNotifications(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "userID")
	sessionID := fmt.Sprintf("notifications:%s", userID)

	messages, err := h.store.LoadMessagesWithID(r.Context(), sessionID)
	if err != nil {
		http.Error(w, "Failed to load notifications", http.StatusInternalServerError)
		return
	}

	type notification struct {
		ID      int64  `json:"id"`
		Content string `json:"content"`
	}
	var out []notification
	for _, msg := range messages {
		content := msg.Content
		var decoded string
		if json.Unmarshal([]byte(content), &decoded) == nil {
			content = decoded
		}
		out = append(out, notification{ID: msg.ID, Content: content})
	}
	if out == nil {
		out = []notification{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// deleteNotification deletes a single notification by ID.
func (h *handler) deleteNotification(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "userID")
	sessionID := fmt.Sprintf("notifications:%s", userID)

	idStr := chi.URLParam(r, "notifID")
	var id int64
	if _, err := fmt.Sscanf(idStr, "%d", &id); err != nil {
		http.Error(w, "Invalid notification ID", http.StatusBadRequest)
		return
	}

	if err := h.store.DeleteMessage(r.Context(), sessionID, id); err != nil {
		http.Error(w, "Failed to delete notification", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// deleteAllNotifications deletes all notifications for a user.
func (h *handler) deleteAllNotifications(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "userID")
	sessionID := fmt.Sprintf("notifications:%s", userID)

	if err := h.store.DeleteMessagesBySession(r.Context(), sessionID); err != nil {
		http.Error(w, "Failed to delete notifications", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// streamNotifications streams live notifications for a user via SSE.
func (h *handler) streamNotifications(w http.ResponseWriter, r *http.Request) {
	userID := chi.URLParam(r, "userID")
	sessionID := "notifications:" + userID

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

// handleSkillsWebhook handles GitHub webhook pushes to increment the skills version.
// Workers detect the change via DB polling and reload from git.
func (h *handler) handleSkillsWebhook(w http.ResponseWriter, r *http.Request) {
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

	newVersion, err := h.store.IncrementSkillsVersion(r.Context())
	if err != nil {
		log.Printf("Error incrementing skills version: %v", err)
		http.Error(w, "Failed to update skills version", http.StatusInternalServerError)
		return
	}

	log.Printf("Skills webhook: version incremented to %d", newVersion)
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

// Admin: list known task queues (from agent catalog)

func (h *handler) listKnownQueues(w http.ResponseWriter, r *http.Request) {
	agents, err := h.store.ListAgents(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list agents: %v", err), http.StatusInternalServerError)
		return
	}
	queues := make([]string, len(agents))
	for i, a := range agents {
		queues[i] = a.TaskQueue
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(queues)
}

// Admin: activity queue mapping

func (h *handler) listActivityQueues(w http.ResponseWriter, r *http.Request) {
	entries, err := h.store.ListActivityQueues(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list activity queues: %v", err), http.StatusInternalServerError)
		return
	}
	if entries == nil {
		entries = []store.ActivityQueueEntry{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

func (h *handler) setActivityQueue(w http.ResponseWriter, r *http.Request) {
	var req store.ActivityQueueEntry
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.ActivityName == "" || req.TaskQueue == "" {
		http.Error(w, "activity_name and task_queue are required", http.StatusBadRequest)
		return
	}
	if err := h.store.SetActivityQueue(r.Context(), req.ActivityName, req.TaskQueue); err != nil {
		http.Error(w, fmt.Sprintf("Failed to set activity queue: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) deleteActivityQueue(w http.ResponseWriter, r *http.Request) {
	activityName := chi.URLParam(r, "activityName")
	if err := h.store.DeleteActivityQueue(r.Context(), activityName); err != nil {
		http.Error(w, fmt.Sprintf("Failed to delete activity queue: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
