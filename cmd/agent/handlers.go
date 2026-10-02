package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"slices"
	"strings"

	"github.com/go-chi/chi/v5"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/config"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
	"github.com/victor/temporal-agent/workflow"
)

type handler struct {
	auth           *auth.Service
	temporalClient client.Client
	hub            *sse.Hub
	cfg            *config.Config
	registry       *tool.Registry
	store          store.Store
}

// --- Auth ---

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (h *handler) login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	token, u, err := h.auth.Login(r.Context(), auth.ClientAddr(r), req.Email, req.Password)
	if errors.Is(err, auth.ErrInvalidCredentials) {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if errors.Is(err, auth.ErrTooManyAttempts) {
		http.Error(w, err.Error(), http.StatusTooManyRequests)
		return
	}
	if err != nil {
		log.Printf("login: %v", err)
		http.Error(w, "Internal error", http.StatusInternalServerError)
		return
	}
	auth.SetCookie(w, r, token)
	writeJSON(w, http.StatusOK, u)
}

func (h *handler) logout(w http.ResponseWriter, r *http.Request) {
	h.auth.Logout(r.Context(), r)
	auth.ClearCookie(w)
	w.WriteHeader(http.StatusNoContent)
}

// me returns the logged-in user.
func (h *handler) me(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, auth.UserFrom(r.Context()))
}

// requireMember lets through only the members of the session in the URL.
// A non-member gets 404, not 403: whether a session exists is not theirs to
// learn.
func (h *handler) requireMember(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok, err := h.store.IsSessionMember(r.Context(), chi.URLParam(r, "id"), auth.UserFrom(r.Context()).ID)
		if err != nil {
			log.Printf("membership check: %v", err)
			http.Error(w, "Internal error", http.StatusInternalServerError)
			return
		}
		if !ok {
			http.Error(w, "Session not found", http.StatusNotFound)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

type createSessionRequest struct {
	AgentID      string `json:"agent_id,omitempty"`
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
	sessionID, err := h.openSession(r.Context(), auth.UserFrom(r.Context()), req.AgentID, req.SystemPrompt, req.Model)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create session: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, createSessionResponse{SessionID: sessionID})
}

// listSessions returns the sessions the logged-in user is a member of.
func (h *handler) listSessions(w http.ResponseWriter, r *http.Request) {
	sessions, err := h.store.ListSessionsByUser(r.Context(), auth.UserFrom(r.Context()).ID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list sessions: %v", err), http.StatusInternalServerError)
		return
	}

	// Check workflow status for each session
	type sessionEntry struct {
		store.Session
		Members []store.SessionMember `json:"members"`
		Active  bool                  `json:"active"`
	}
	entries := make([]sessionEntry, 0, len(sessions))
	for _, s := range sessions {
		members, err := h.store.ListSessionMembers(r.Context(), s.SessionID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to list members: %v", err), http.StatusInternalServerError)
			return
		}
		active := h.isSessionActive(r.Context(), s.SessionID)
		entries = append(entries, sessionEntry{Session: s, Members: members, Active: active})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(entries)
}

// deleteSession deletes a session for every member. Only its creator may: the
// others leave it instead.
func (h *handler) deleteSession(w http.ResponseWriter, r *http.Request) {
	switch err := h.removeSession(r.Context(), chi.URLParam(r, "id"), auth.UserFrom(r.Context()).ID); {
	case errors.Is(err, errSessionGone):
		http.Error(w, "Session not found", http.StatusNotFound)
	case errors.Is(err, errNotCreator):
		http.Error(w, "Only the session's creator can delete it; leave it instead", http.StatusForbidden)
	case err != nil:
		http.Error(w, fmt.Sprintf("Failed to delete session: %v", err), http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
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
	query, err := runningSessionQuery(sessionID)
	if err != nil {
		return false
	}
	resp, err := h.temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: h.cfg.TemporalNamespace,
		Query:     query,
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
	query, err := runningSessionQuery(sessionID)
	if err != nil {
		return ""
	}
	resp, err := h.temporalClient.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: h.cfg.TemporalNamespace,
		Query:     query,
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

	// An empty message is stored as an empty user turn, which every later turn
	// then replays to the LLM — and the API rejects a user message with no
	// content, so the session is poisoned for good.
	if strings.TrimSpace(req.Content) == "" {
		http.Error(w, "Message content is required", http.StatusBadRequest)
		return
	}

	sess, err := h.store.GetSession(r.Context(), sessionID)
	if err != nil || sess == nil {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}
	// A fork takes messages once its summary is in: the summary must be the
	// fork's first message, and a turn started before would lack it.
	if sess.ForkedAtMessageID != 0 {
		if state, err := h.forkSummaryState(r.Context(), sess); err == nil && state == summaryPending {
			http.Error(w, "The summary of the parent session is still being written", http.StatusConflict)
			return
		}
	}

	called, err := h.deliverMessage(r.Context(), sess, auth.UserFrom(r.Context()), req.Content)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to send message: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]bool{"agent_called": called})
}

type agentModeRequest struct {
	Mode string `json:"mode"`
}

// setAgentMode sets when human messages call the session's agent. Any member
// may.
func (h *handler) setAgentMode(w http.ResponseWriter, r *http.Request) {
	var req agentModeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	switch req.Mode {
	case store.AgentModeAuto, store.AgentModeAlways, store.AgentModeMention:
	default:
		http.Error(w, "mode must be auto, always or mention", http.StatusBadRequest)
		return
	}
	if err := h.store.SetSessionAgentMode(r.Context(), chi.URLParam(r, "id"), req.Mode); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

	messages, err := h.store.LoadMessagesWithID(r.Context(), sessionID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to load messages: %v", err), http.StatusInternalServerError)
		return
	}

	// Convert store messages to a frontend-friendly format
	type historyEntry struct {
		ID        int64       `json:"id"`             // what a fork starts from
		Type      string      `json:"type"`           // "message", "tool_calls", "fork_summary"
		Role      string      `json:"role,omitempty"` // "user", "assistant"
		Content   string      `json:"content,omitempty"`
		UserID    string      `json:"user_id,omitempty"` // author of a user message
		Author    string      `json:"author,omitempty"`
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

		switch {
		case msg.Kind == store.KindForkSummary:
			history = append(history, historyEntry{ID: msg.ID, Type: "fork_summary", Content: content})
		case msg.Role == store.RoleUser:
			history = append(history, historyEntry{ID: msg.ID, Type: "message", Role: "user", Content: content, UserID: msg.UserID, Author: msg.Author})
		case msg.Role == store.RoleAssistant:
			if content != "" {
				history = append(history, historyEntry{ID: msg.ID, Type: "message", Role: "assistant", Content: content})
			}
			if len(msg.ToolCalls) > 0 {
				type tc struct {
					Name  string          `json:"name"`
					Input json.RawMessage `json:"input"`
				}
				calls := make([]tc, len(msg.ToolCalls))
				for i, t := range msg.ToolCalls {
					calls[i] = tc{Name: t.Name, Input: tool.DisplayInput(t.Name, t.Input)}
				}
				history = append(history, historyEntry{ID: msg.ID, Type: "tool_calls", ToolCalls: calls})
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

// answerQuestion answers an ask_user of the session in the URL, by any of its
// members.
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
	// Every workflow of a session, sub-agents' included, has an ID starting
	// with the session's: one from another session is refused, membership
	// was checked for this one only.
	if !strings.HasPrefix(req.WorkflowID, chi.URLParam(r, "id")+"-") {
		http.Error(w, "This question does not belong to this session", http.StatusForbidden)
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
	sessionID := "notifications:" + auth.UserFrom(r.Context()).ID

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
	sessionID := "notifications:" + auth.UserFrom(r.Context()).ID

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
	sessionID := "notifications:" + auth.UserFrom(r.Context()).ID

	if err := h.store.DeleteMessagesBySession(r.Context(), sessionID); err != nil {
		http.Error(w, "Failed to delete notifications", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// streamNotifications streams live notifications for a user via SSE.
func (h *handler) streamNotifications(w http.ResponseWriter, r *http.Request) {
	sessionID := "notifications:" + auth.UserFrom(r.Context()).ID

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

// maxNotifyBytes bounds a notification from a worker. An event carries one
// agent message at most, far below this.
const maxNotifyBytes = 4 << 20

// handleInternalNotify receives SSE events from workers and publishes them to
// the local hub. A worker proves itself with the shared INTERNAL_API_KEY: the
// endpoint can put any event in any session, a fake question or a fake answer
// of the agent included, so without a key configured it refuses everything.
func handleInternalNotify(hub *sse.Hub, apiKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !validBearer(r, apiKey) {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		var input activity.NotifyInput
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxNotifyBytes)).Decode(&input); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		hub.Publish(input.SessionID, input.Event)
		w.WriteHeader(http.StatusNoContent)
	}
}

// validBearer reports whether the request carries "Authorization: Bearer
// <key>". An empty key matches nothing.
func validBearer(r *http.Request, key string) bool {
	got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	return ok && secretEqual(got, key)
}

// secretEqual compares a presented secret with the expected one in constant
// time. An empty expected secret matches nothing: an unset secret closes the
// route rather than opening it.
func secretEqual(got, want string) bool {
	return want != "" && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// maxWebhookBytes bounds a webhook payload. GitHub caps its own at 25 MB, but
// a push event that matters here is a few kilobytes.
const maxWebhookBytes = 1 << 20

// handleSkillsWebhook handles GitHub webhook pushes to increment the skills version.
// Workers detect the change via DB polling and reload from git. Every request
// must be signed with SKILLS_WEBHOOK_SECRET: each one makes the server and
// every worker clone the skills repo again.
func (h *handler) handleSkillsWebhook(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookBytes))
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	if !verifyGitHubSignature(body, r.Header.Get("X-Hub-Signature-256"), h.cfg.SkillsWebhookSecret) {
		http.Error(w, "Invalid signature", http.StatusUnauthorized)
		return
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
	if secret == "" || !strings.HasPrefix(signature, "sha256=") {
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

// resolveAgentID returns requested if it is a known agent. With no request, it
// returns the configured default agent, or the first agent if that one is missing.
func (h *handler) resolveAgentID(ctx context.Context, requested string) (string, error) {
	if requested != "" {
		a, err := h.store.GetAgent(ctx, requested)
		if err != nil {
			return "", fmt.Errorf("load agent %q: %w", requested, err)
		}
		if a == nil {
			return "", fmt.Errorf("unknown agent_id %q", requested)
		}
		return a.ID, nil
	}

	agents, err := h.store.ListAgents(ctx)
	if err != nil {
		return "", fmt.Errorf("list agents: %w", err)
	}
	for _, a := range agents {
		if a.ID == h.cfg.DefaultAgentID {
			return a.ID, nil
		}
	}
	if len(agents) > 0 {
		return agents[0].ID, nil
	}
	return "", fmt.Errorf("no agents configured")
}

// Admin: list known task queues (workflow queue + queues serving tools)

func (h *handler) listKnownQueues(w http.ResponseWriter, r *http.Request) {
	tools, err := h.store.ListTools(r.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list tools: %v", err), http.StatusInternalServerError)
		return
	}
	queues := []string{h.cfg.WorkflowQueue}
	for _, t := range tools {
		if !slices.Contains(queues, t.TaskQueue) {
			queues = append(queues, t.TaskQueue)
		}
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

// --- Session members ---

func (h *handler) listMembers(w http.ResponseWriter, r *http.Request) {
	members, err := h.store.ListSessionMembers(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list members: %v", err), http.StatusInternalServerError)
		return
	}
	if members == nil {
		members = []store.SessionMember{}
	}
	writeJSON(w, http.StatusOK, members)
}

type addMemberRequest struct {
	Email string `json:"email"`
}

// addMember adds a user to the session, by email. Any member may.
func (h *handler) addMember(w http.ResponseWriter, r *http.Request) {
	var req addMemberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Email) == "" {
		http.Error(w, "email is required", http.StatusBadRequest)
		return
	}
	switch err := h.inviteByEmail(r.Context(), chi.URLParam(r, "id"), req.Email, auth.UserFrom(r.Context()).ID); {
	case errors.Is(err, errNoSuchUser):
		http.Error(w, "No active user with this email", http.StatusNotFound)
	case err != nil:
		http.Error(w, fmt.Sprintf("Failed to add member: %v", err), http.StatusInternalServerError)
	default:
		h.listMembers(w, r)
	}
}

// removeMember lets a member leave the session. Members cannot remove each
// other. When the last member leaves, the session goes: nobody could open it.
func (h *handler) removeMember(w http.ResponseWriter, r *http.Request) {
	sessionID, userID := chi.URLParam(r, "id"), chi.URLParam(r, "userID")
	if userID != auth.UserFrom(r.Context()).ID {
		http.Error(w, "A member can only remove themselves", http.StatusForbidden)
		return
	}
	if err := h.leave(r.Context(), sessionID, userID); err != nil {
		http.Error(w, fmt.Sprintf("Failed to leave: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// maxTitleRunes bounds a session title taken from its first message.
const maxTitleRunes = 80

// setTitleFrom titles a session after a message, if it has no title yet. The
// cut is in characters: cut in bytes, an accented letter can be split, and
// Postgres refuses the invalid UTF-8.
func (h *handler) setTitleFrom(sessionID, text string) {
	title := strings.TrimSpace(text)
	if r := []rune(title); len(r) > maxTitleRunes {
		title = string(r[:maxTitleRunes]) + "..."
	}
	if err := h.store.UpdateSessionTitle(context.Background(), sessionID, title); err != nil {
		log.Printf("Session %s: set title: %v", sessionID, err)
	}
}
