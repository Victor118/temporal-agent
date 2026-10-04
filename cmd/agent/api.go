package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/victor/temporal-agent/activity"
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
	SessionMembership(ctx context.Context, sessionID, userID string) (members int, isMember bool, err error)
	ListSessionsByUser(ctx context.Context, userID string) ([]store.Session, error)
	ListSessionStats(ctx context.Context, userID string) (map[string]store.SessionStats, error)
	ListSessionMembers(ctx context.Context, sessionID string) ([]store.SessionMember, error)
	ListForks(ctx context.Context, sessionID, userID string) ([]store.Session, error)
	ListAgents(ctx context.Context) ([]store.Agent, error)
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

// sseKeepAlive is how often an SSE stream with nothing to say sends a
// comment: a proxy closes a connection silent for long (often 60 s), and
// each reconnection reloads the thread.
const sseKeepAlive = 30 * time.Second

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
	AgentID string `json:"agent_id,omitempty"`
}

type createSessionResponse struct {
	SessionID string `json:"session_id"`
}

func (a *api) createSession(w http.ResponseWriter, r *http.Request) {
	var req createSessionRequest
	if r.Body != nil {
		json.NewDecoder(r.Body).Decode(&req)
	}
	sessionID, err := a.sessions.Open(r.Context(), auth.UserFrom(r.Context()), session.OpenOptions{AgentID: req.AgentID})
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
	}
	entries := make([]sessionEntry, 0, len(sessions))
	for _, s := range sessions {
		members, err := a.store.ListSessionMembers(r.Context(), s.SessionID)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to list members: %v", err), http.StatusInternalServerError)
			return
		}
		entries = append(entries, sessionEntry{Session: s, Members: members})
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
	case errors.Is(err, session.ErrQueueFull):
		http.Error(w, err.Error(), http.StatusTooManyRequests)
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

// cancelAgent stops the turns running that the user may stop: all of them
// for the session's creator, those answering their messages for another
// member.
func (a *api) cancelAgent(w http.ResponseWriter, r *http.Request) {
	sess, err := a.sessions.Get(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}
	writeStopped(w, a.sessions.Cancel(r.Context(), sess, auth.UserFrom(r.Context())))
}

type stopRequest struct {
	// Turn is the turn to stop (a turn key, as the state names it); empty,
	// the one running.
	Turn string `json:"turn"`
}

// stopParticipant stops a participant's turn: by the author of the message
// it answers, or the session's creator. Its body is optional.
func (a *api) stopParticipant(w http.ResponseWriter, r *http.Request) {
	var req stopRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	sess, err := a.sessions.Get(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}
	writeStopped(w, a.sessions.StopTurn(r.Context(), sess, chi.URLParam(r, "agent"), req.Turn, auth.UserFrom(r.Context())))
}

// clearParticipant stops a participant's turn and drops its waiting
// messages: by the session's creator alone.
func (a *api) clearParticipant(w http.ResponseWriter, r *http.Request) {
	sess, err := a.sessions.Get(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}
	writeStopped(w, a.sessions.Clear(r.Context(), sess, chi.URLParam(r, "agent"), auth.UserFrom(r.Context())))
}

// writeStopped answers a stop: 202 once sent, 403 to a member who may not,
// 409 when the turn aimed at is over, 404 when nothing runs.
func writeStopped(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, session.ErrStopNotAllowed), errors.Is(err, session.ErrClearNotAllowed):
		http.Error(w, err.Error(), http.StatusForbidden)
	case errors.Is(err, session.ErrTurnOver):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, session.ErrNothingToStop):
		http.Error(w, err.Error(), http.StatusNotFound)
	case err != nil:
		http.Error(w, fmt.Sprintf("Failed to stop: %v", err), http.StatusInternalServerError)
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
		ID        int64          `json:"id"`             // what a fork starts from
		Type      string         `json:"type"`           // "message", "tool_calls", "fork_summary", "fork_report", "turn_error"
		Role      string         `json:"role,omitempty"` // "user", "assistant"
		Content   string         `json:"content,omitempty"`
		UserID    string         `json:"user_id,omitempty"` // author of a user message, sender of a report
		Author    string         `json:"author,omitempty"`
		Fork      *store.ForkRef `json:"fork,omitempty"` // the fork a report comes from
		ToolCalls interface{}    `json:"tool_calls,omitempty"`
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
		case msg.Kind == store.KindForkReport:
			history = append(history, historyEntry{ID: msg.ID, Type: "fork_report", Content: content, UserID: msg.UserID, Author: msg.Author, Fork: msg.Fork})
		case msg.Kind == store.KindTurnEnd:
			// A turn's end shows only when it says why the turn failed: the
			// system's words, not the agent's, which an assistant role would
			// pass off as its answer.
			if reason := store.TurnEndError(msg.Message); reason != "" {
				history = append(history, historyEntry{ID: msg.ID, Type: "turn_error", Content: reason})
			}
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

// getState answers where the session's participants stand: each one
// running, as its state query says.
func (a *api) getState(w http.ResponseWriter, r *http.Request) {
	state, err := a.sessions.State(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to %v", err), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, state)
}

// stream sends the session's events as they come, while its user is a
// member.
func (a *api) stream(w http.ResponseWriter, r *http.Request) {
	sessionID := chi.URLParam(r, "id")
	relaySSE(w, r, a.hub, sseKeepAlive, stillMember(r, a.sessions, sessionID), sessionID)
}

// stillMember is the check that keeps a session's stream open: its user is
// still a member. When the store cannot tell, the stream goes on: the next
// check will.
func stillMember(r *http.Request, sessions *session.Service, sessionID string) func() bool {
	userID := auth.UserFrom(r.Context()).ID
	return func() bool {
		if r.Context().Err() != nil { // the client is gone: the stream ends anyway
			return true
		}
		ok, err := sessions.IsMember(r.Context(), sessionID, userID)
		if err != nil {
			log.Printf("stream of session %s: membership check: %v", sessionID, err)
			return true
		}
		return ok
	}
}

// relaySSE relays hub's events for topics over SSE, on one stream, until the
// client goes away, with a comment every keepAlive while nothing else is
// sent. Each event goes with its ID: a client that reconnects is first sent
// the events it missed, or a reload event when the hub no longer has them.
//
// alive, when not nil, is checked at every keep-alive and on every
// session.EventMemberLeft, replayed or live: once false, the stream ends (a
// member removed from the session hears no more of it), without that event
// but with a last session.EventSessionGone.
func relaySSE(w http.ResponseWriter, r *http.Request, hub *sse.Hub, keepAlive time.Duration, alive func() bool, topics ...string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	sub := hub.Subscribe(lastEventID(r), topics...)
	defer hub.Unsubscribe(sub)
	if sub.Stale {
		writeSSE(w, sse.Event{ID: sub.At, SSEEvent: activity.SSEEvent{Type: sse.EventReload, Data: []byte("{}")}})
	}
	gone := func() bool {
		if alive == nil || alive() {
			return false
		}
		writeGone(w)
		flusher.Flush()
		return true
	}
	// Checked once subscribed: a removal committed before the subscription is
	// seen here, one after it arrives live. With nothing to replay (no last
	// ID, a stale one), a check made before subscribing would leave a window
	// until the next keep-alive.
	if gone() {
		return
	}
	for _, event := range sub.Missed {
		if event.Type == session.EventMemberLeft && gone() {
			return
		}
		writeSSE(w, event)
	}
	flusher.Flush()

	ping := time.NewTicker(keepAlive)
	defer ping.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ping.C:
			if gone() {
				return
			}
			// A comment line: EventSource ignores it.
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case event, ok := <-sub.C:
			if !ok { // too slow: dropped, the client reconnects and catches up
				return
			}
			if event.Type == session.EventMemberLeft && gone() {
				return
			}
			writeSSE(w, event)
			flusher.Flush()
		}
	}
}

// lastEventID is the ID of the last event a reconnecting client got:
// EventSource sends it in a header when it reconnects on its own; the page
// passes it in the URL, where it starts from the position it was rendered
// at, and where the htmx extension's reconnection (a new EventSource, which
// sends no header) finds it. The header, when there is one, is the latest.
func lastEventID(r *http.Request) string {
	if id := r.Header.Get("Last-Event-ID"); id != "" {
		return id
	}
	return r.URL.Query().Get("last_event_id")
}

// writeSSE writes an event, with its ID when it has one: an empty "id:"
// line would reset the client's last event ID.
func writeSSE(w io.Writer, event sse.Event) {
	if event.ID != "" {
		fmt.Fprintf(w, "id: %s\n", event.ID)
	}
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Type, event.Data)
}

// writeGone tells a stream's user they are no member of its session.
func writeGone(w io.Writer) {
	writeSSE(w, sse.Event{SSEEvent: activity.SSEEvent{Type: session.EventSessionGone, Data: []byte("{}")}})
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
	MessageID int64  `json:"message_id"`
	Purpose   string `json:"purpose,omitempty"` // what the fork is for; optional
}

// forkSession starts a new session from a message of this one.
func (a *api) forkSession(w http.ResponseWriter, r *http.Request) {
	var req forkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.MessageID <= 0 {
		http.Error(w, "message_id is required", http.StatusBadRequest)
		return
	}
	f, err := a.sessions.Fork(r.Context(), chi.URLParam(r, "id"), req.MessageID, req.Purpose, auth.UserFrom(r.Context()))
	switch {
	case errors.Is(err, session.ErrNotFound):
		http.Error(w, "Session not found", http.StatusNotFound)
	case errors.Is(err, session.ErrBadForkPoint):
		http.Error(w, "No such message to fork from in this session", http.StatusBadRequest)
	case errors.Is(err, session.ErrPurposeTooLong):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		writeJSON(w, http.StatusCreated, createSessionResponse{SessionID: f.SessionID})
	}
}

// reportToParent starts the report of this fork to its parent, sent by the
// user: it arrives in the parent as their message, and calls no agent.
// Accepted at once; the parent's members hear of it when it is posted.
func (a *api) reportToParent(w http.ResponseWriter, r *http.Request) {
	switch err := a.sessions.ReportToParent(r.Context(), chi.URLParam(r, "id"), auth.UserFrom(r.Context())); {
	case errors.Is(err, session.ErrNotFound):
		http.Error(w, "Session not found", http.StatusNotFound)
	case errors.Is(err, session.ErrNotParentMember):
		http.Error(w, err.Error(), http.StatusForbidden)
	case errors.Is(err, session.ErrNotAFork), errors.Is(err, session.ErrNoParent),
		errors.Is(err, session.ErrSummaryPending), errors.Is(err, session.ErrNothingToReport):
		http.Error(w, err.Error(), http.StatusConflict)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusAccepted)
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
	// Report is where a fork stands with its reports to its parent, for the
	// user. Empty for a session that is not a fork.
	Report *reportInfo `json:"report,omitempty"`
}

type reportInfo struct {
	CanReport bool `json:"can_report"`
	// Refused: why the user cannot report at all (parent deleted, not a
	// member of it).
	Refused        string `json:"refused,omitempty"`
	SummaryPending bool   `json:"summary_pending,omitempty"`
	// AgentWorking: the fork's agent is on a turn. A report covers what is
	// written so far; the rest goes into the next one.
	AgentWorking bool `json:"agent_working,omitempty"`
	Pending      bool `json:"pending,omitempty"`
	Failed       bool `json:"failed,omitempty"`
	NothingNew   bool `json:"nothing_new,omitempty"`
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
		msgs, err := a.store.LoadMessagesWithID(ctx, sess.SessionID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		info.Summary = a.sessions.ForkSummaryState(ctx, sess)
		st, err := a.sessions.ReportState(ctx, sess, msgs, me)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		info.Report = &reportInfo{CanReport: st.CanReport(), SummaryPending: st.SummaryPending, AgentWorking: st.AgentWorking,
			Pending: st.Pending, Failed: st.Failed, NothingNew: st.NothingNew}
		if st.Refused != nil {
			info.Report.Refused = st.Refused.Error()
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
	relaySSE(w, r, a.hub, sseKeepAlive, nil, notificationsOf(auth.UserFrom(r.Context()).ID))
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
