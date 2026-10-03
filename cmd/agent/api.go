package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/session"
	"github.com/victor/temporal-agent/sse"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// readStore is what the HTTP adapters read directly, to show it: the user's
// sessions, a transcript, members, forks, notifications. Anything that
// changes a session goes through session.Service.
type readStore interface {
	GetSession(ctx context.Context, sessionID string) (*store.Session, error)
	IsSessionMember(ctx context.Context, sessionID, userID string) (bool, error)
	ListSessionsByUser(ctx context.Context, userID string) ([]store.Session, error)
	ListSessionStats(ctx context.Context, userID string) (map[string]store.SessionStats, error)
	ListSessionMembers(ctx context.Context, sessionID string) ([]store.SessionMember, error)
	ListForks(ctx context.Context, sessionID, userID string) ([]store.Session, error)
	GetAgent(ctx context.Context, agentID string) (*store.Agent, error)
	LoadMessagesWithID(ctx context.Context, sessionID string) ([]store.MessageWithID, error)
	ListTools(ctx context.Context) ([]store.ToolRecord, error)
	DeleteMessage(ctx context.Context, sessionID string, id int64) error
	DeleteMessagesBySession(ctx context.Context, sessionID string) error
}

// api is the JSON API: it decodes a request, calls the session service or
// reads the store, and encodes the answer.
type api struct {
	auth     *auth.Service
	sessions *session.Service
	store    readStore
	hub      *sse.Hub
}

// notificationsOf is the pseudo-session holding a user's notifications.
func notificationsOf(userID string) string { return "notifications:" + userID }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// requireMember lets through only the members of the session in the URL.
// A non-member gets 404, not 403: whether a session exists is not theirs to
// learn.
func requireMember(sessions *session.Service) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ok, err := sessions.IsMember(r.Context(), chi.URLParam(r, "id"), auth.UserFrom(r.Context()).ID)
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
}

// --- Auth ---

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (a *api) login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	token, u, err := a.auth.Login(r.Context(), a.auth.ClientAddr(r), req.Email, req.Password)
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

func (a *api) logout(w http.ResponseWriter, r *http.Request) {
	a.auth.Logout(r.Context(), r)
	auth.ClearCookie(w)
	w.WriteHeader(http.StatusNoContent)
}

// me returns the logged-in user.
func (a *api) me(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, auth.UserFrom(r.Context()))
}

// --- Sessions ---

type createSessionRequest struct {
	AgentID      string `json:"agent_id,omitempty"`
	SystemPrompt string `json:"system_prompt,omitempty"`
	Model        string `json:"model,omitempty"`
}

type createSessionResponse struct {
	SessionID string `json:"session_id"`
}

func (a *api) createSession(w http.ResponseWriter, r *http.Request) {
	var req createSessionRequest
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}
	sessionID, err := a.sessions.Open(r.Context(), auth.UserFrom(r.Context()), session.OpenOptions{
		AgentID: req.AgentID, SystemPrompt: req.SystemPrompt, Model: req.Model,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create session: %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusCreated, createSessionResponse{SessionID: sessionID})
}

// listSessions returns the sessions the logged-in user is a member of.
func (a *api) listSessions(w http.ResponseWriter, r *http.Request) {
	sessions, err := a.store.ListSessionsByUser(r.Context(), auth.UserFrom(r.Context()).ID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list sessions: %v", err), http.StatusInternalServerError)
		return
	}

	type sessionEntry struct {
		store.Session
		Members []store.SessionMember `json:"members"`
		Active  bool                  `json:"active"`
	}
	entries := make([]sessionEntry, 0, len(sessions))
	for _, s := range sessions {
		members, err := a.store.ListSessionMembers(r.Context(), s.SessionID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to list members: %v", err), http.StatusInternalServerError)
			return
		}
		entries = append(entries, sessionEntry{Session: s, Members: members, Active: a.sessions.IsActive(r.Context(), s.SessionID)})
	}
	writeJSON(w, http.StatusOK, entries)
}

// deleteSession deletes a session for every member. Only its creator may: the
// others leave it instead.
func (a *api) deleteSession(w http.ResponseWriter, r *http.Request) {
	switch err := a.sessions.Delete(r.Context(), chi.URLParam(r, "id"), auth.UserFrom(r.Context()).ID); {
	case errors.Is(err, session.ErrNotFound):
		http.Error(w, "Session not found", http.StatusNotFound)
	case errors.Is(err, session.ErrNotCreator):
		http.Error(w, "Only the session's creator can delete it; leave it instead", http.StatusForbidden)
	case err != nil:
		http.Error(w, fmt.Sprintf("Failed to delete session: %v", err), http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

type sendMessageRequest struct {
	Content string `json:"content"`
}

func (a *api) sendMessage(w http.ResponseWriter, r *http.Request) {
	var req sendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Content) == "" {
		http.Error(w, "Message content is required", http.StatusBadRequest)
		return
	}
	sess, err := a.sessions.Get(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}
	called, err := a.sessions.Deliver(r.Context(), sess, auth.UserFrom(r.Context()), req.Content)
	switch {
	case errors.Is(err, session.ErrSummaryPending):
		http.Error(w, "The summary of the parent session is still being written", http.StatusConflict)
	case err != nil:
		http.Error(w, fmt.Sprintf("Failed to send message: %v", err), http.StatusInternalServerError)
	default:
		writeJSON(w, http.StatusAccepted, map[string]bool{"agent_called": called})
	}
}

type agentModeRequest struct {
	Mode string `json:"mode"`
}

// setAgentMode sets when human messages call the session's agent. Any member
// may.
func (a *api) setAgentMode(w http.ResponseWriter, r *http.Request) {
	var req agentModeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	switch err := a.sessions.SetAgentMode(r.Context(), chi.URLParam(r, "id"), req.Mode); {
	case errors.Is(err, session.ErrBadMode):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func (a *api) cancelAgent(w http.ResponseWriter, r *http.Request) {
	switch err := a.sessions.Cancel(r.Context(), chi.URLParam(r, "id")); {
	case errors.Is(err, session.ErrNoActiveSession):
		http.Error(w, "No active session found", http.StatusNotFound)
	case err != nil:
		http.Error(w, fmt.Sprintf("Failed to cancel agent: %v", err), http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusAccepted)
	}
}

func (a *api) getHistory(w http.ResponseWriter, r *http.Request) {
	messages, err := a.store.LoadMessagesWithID(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to load messages: %v", err), http.StatusInternalServerError)
		return
	}

	// Convert store messages to a frontend-friendly format
	type historyEntry struct {
		ID        int64       `json:"id"`             // what a fork starts from
		Type      string      `json:"type"`           // "message", "tool_calls", "fork_summary", "turn_error"
		Role      string      `json:"role,omitempty"` // "user", "assistant"
		Content   string      `json:"content,omitempty"`
		UserID    string      `json:"user_id,omitempty"` // author of a user message
		Author    string      `json:"author,omitempty"`
		ToolCalls interface{} `json:"tool_calls,omitempty"`
	}

	// Which inputs to hide comes from the published tools. Unreadable, every
	// input is hidden rather than a user's memory shown.
	var private tool.PrivateInputs = allPrivate{}
	if records, err := a.store.ListTools(r.Context()); err == nil {
		private = tool.PrivateSetOf(records)
	} else {
		log.Printf("history: list tools: %v", err)
	}

	var history []historyEntry
	for _, msg := range messages {
		content := decodeContent(msg.Content)
		switch {
		case msg.Kind == store.KindForkSummary:
			history = append(history, historyEntry{ID: msg.ID, Type: "fork_summary", Content: content})
		case msg.Kind == store.KindTurnError:
			// The system's words, not the agent's: an assistant role would pass
			// the error off as its answer.
			history = append(history, historyEntry{ID: msg.ID, Type: "turn_error", Content: content})
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
					calls[i] = tc{Name: t.Name, Input: tool.DisplayInput(private.PrivateInput(t.Name), t.Input)}
				}
				history = append(history, historyEntry{ID: msg.ID, Type: "tool_calls", ToolCalls: calls})
			}
		}
	}
	writeJSON(w, http.StatusOK, history)
}

func (a *api) getState(w http.ResponseWriter, r *http.Request) {
	state, err := a.sessions.State(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

// stream sends the session's events as they come.
func (a *api) stream(w http.ResponseWriter, r *http.Request) {
	a.streamTopic(w, r, chi.URLParam(r, "id"))
}

// streamTopic relays the hub's events for topic over SSE until the client
// goes away.
func (a *api) streamTopic(w http.ResponseWriter, r *http.Request, topic string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := a.hub.Subscribe(topic)
	defer a.hub.Unsubscribe(topic, ch)

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
func (a *api) answerQuestion(w http.ResponseWriter, r *http.Request) {
	var req answerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.WorkflowID == "" {
		http.Error(w, "workflow_id and answer are required", http.StatusBadRequest)
		return
	}
	switch err := a.sessions.Answer(r.Context(), chi.URLParam(r, "id"), req.WorkflowID, req.Answer); {
	case errors.Is(err, session.ErrEmptyAnswer):
		http.Error(w, "workflow_id and answer are required", http.StatusBadRequest)
	case errors.Is(err, session.ErrForeignQuestion):
		http.Error(w, "This question does not belong to this session", http.StatusForbidden)
	case err != nil:
		http.Error(w, fmt.Sprintf("Failed to %v", err), http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusAccepted)
	}
}

// --- Forks ---

type forkRequest struct {
	MessageID int64 `json:"message_id"`
}

// forkSession starts a new session from a message of this one.
func (a *api) forkSession(w http.ResponseWriter, r *http.Request) {
	var req forkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.MessageID <= 0 {
		http.Error(w, "message_id is required", http.StatusBadRequest)
		return
	}
	f, err := a.sessions.Fork(r.Context(), chi.URLParam(r, "id"), req.MessageID, auth.UserFrom(r.Context()))
	switch {
	case errors.Is(err, session.ErrNotFound):
		http.Error(w, "Session not found", http.StatusNotFound)
	case errors.Is(err, session.ErrBadForkPoint):
		http.Error(w, "No such message to fork from in this session", http.StatusBadRequest)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		writeJSON(w, http.StatusCreated, createSessionResponse{SessionID: f.SessionID})
	}
}

type sessionLink struct {
	SessionID string `json:"session_id"`
	Title     string `json:"title"`
}

type parentInfo struct {
	MessageID int64 `json:"message_id"`
	// Accessible: the user is a member of the parent. Otherwise its ID and
	// title are withheld: they are not the user's to see.
	Accessible bool `json:"accessible"`
	*sessionLink
}

type forkInfo struct {
	sessionLink
	ForkedAtMessageID int64     `json:"forked_at_message_id"`
	CreatedAt         time.Time `json:"created_at"`
}

type sessionInfo struct {
	store.Session
	Members []store.SessionMember `json:"members"`
	// Parent is set on a fork whose parent still exists.
	Parent *parentInfo `json:"parent,omitempty"`
	// Summary is the state of a fork's starting summary: ready, pending or
	// failed. Empty for a session that is not a fork.
	Summary session.SummaryState `json:"summary,omitempty"`
	// Forks are this session's forks the user is a member of.
	Forks []forkInfo `json:"forks"`
}

// getSessionInfo describes a session and its place in a fork tree, as far as
// the user may see it.
func (a *api) getSessionInfo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me := auth.UserFrom(ctx)
	sess, err := a.sessions.Get(ctx, chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}
	info := sessionInfo{Session: *sess, Forks: []forkInfo{}}
	if info.Members, err = a.store.ListSessionMembers(ctx, sess.SessionID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if sess.ForkedAtMessageID != 0 {
		if info.Summary, err = a.sessions.ForkSummaryState(ctx, sess); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if sess.ParentSessionID != "" {
		p := &parentInfo{MessageID: sess.ForkedAtMessageID}
		if ok, _ := a.store.IsSessionMember(ctx, sess.ParentSessionID, me.ID); ok {
			if parent, _ := a.store.GetSession(ctx, sess.ParentSessionID); parent != nil {
				p.Accessible = true
				p.sessionLink = &sessionLink{SessionID: parent.SessionID, Title: parent.Title}
			}
		}
		info.Parent = p
	}

	forks, err := a.store.ListForks(ctx, sess.SessionID, me.ID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, f := range forks {
		info.Forks = append(info.Forks, forkInfo{
			sessionLink:       sessionLink{SessionID: f.SessionID, Title: f.Title},
			ForkedAtMessageID: f.ForkedAtMessageID,
			CreatedAt:         f.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, info)
}

// --- Members ---

func (a *api) listMembers(w http.ResponseWriter, r *http.Request) {
	members, err := a.store.ListSessionMembers(r.Context(), chi.URLParam(r, "id"))
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
func (a *api) addMember(w http.ResponseWriter, r *http.Request) {
	var req addMemberRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Email) == "" {
		http.Error(w, "email is required", http.StatusBadRequest)
		return
	}
	switch err := a.sessions.Invite(r.Context(), chi.URLParam(r, "id"), req.Email, auth.UserFrom(r.Context()).ID); {
	case errors.Is(err, session.ErrNoSuchUser):
		http.Error(w, "No active user with this email", http.StatusNotFound)
	case err != nil:
		http.Error(w, fmt.Sprintf("Failed to add member: %v", err), http.StatusInternalServerError)
	default:
		a.listMembers(w, r)
	}
}

// removeMember lets a member leave the session. Members cannot remove each
// other. When the last member leaves, the session goes: nobody could open it.
func (a *api) removeMember(w http.ResponseWriter, r *http.Request) {
	sessionID, userID := chi.URLParam(r, "id"), chi.URLParam(r, "userID")
	if userID != auth.UserFrom(r.Context()).ID {
		http.Error(w, "A member can only remove themselves", http.StatusForbidden)
		return
	}
	if err := a.sessions.Leave(r.Context(), sessionID, userID); err != nil {
		http.Error(w, fmt.Sprintf("Failed to leave: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Notifications ---

// getNotifications returns the notification history for a user.
func (a *api) getNotifications(w http.ResponseWriter, r *http.Request) {
	messages, err := a.store.LoadMessagesWithID(r.Context(), notificationsOf(auth.UserFrom(r.Context()).ID))
	if err != nil {
		http.Error(w, "Failed to load notifications", http.StatusInternalServerError)
		return
	}

	type notification struct {
		ID      int64  `json:"id"`
		Content string `json:"content"`
	}
	out := []notification{}
	for _, msg := range messages {
		out = append(out, notification{ID: msg.ID, Content: decodeContent(msg.Content)})
	}
	writeJSON(w, http.StatusOK, out)
}

// deleteNotification deletes a single notification by ID.
func (a *api) deleteNotification(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "notifID"), 10, 64)
	if err != nil {
		http.Error(w, "Invalid notification ID", http.StatusBadRequest)
		return
	}
	if err := a.store.DeleteMessage(r.Context(), notificationsOf(auth.UserFrom(r.Context()).ID), id); err != nil {
		http.Error(w, "Failed to delete notification", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// deleteAllNotifications deletes all notifications for a user.
func (a *api) deleteAllNotifications(w http.ResponseWriter, r *http.Request) {
	if err := a.store.DeleteMessagesBySession(r.Context(), notificationsOf(auth.UserFrom(r.Context()).ID)); err != nil {
		http.Error(w, "Failed to delete notifications", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// streamNotifications streams live notifications for a user via SSE.
func (a *api) streamNotifications(w http.ResponseWriter, r *http.Request) {
	a.streamTopic(w, r, notificationsOf(auth.UserFrom(r.Context()).ID))
}

// allPrivate hides every tool input.
type allPrivate struct{}

func (allPrivate) PrivateInput(string) bool { return true }

// decodeContent returns a stored message's text: it is kept as a JSON string.
func decodeContent(content string) string {
	var s string
	if json.Unmarshal([]byte(content), &s) == nil {
		return s
	}
	return content
}
