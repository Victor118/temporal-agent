package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

type forkRequest struct {
	MessageID int64 `json:"message_id"`
}

// forkSession starts a new session from a message of this one. The fork gets
// the same agent, the forking user as its only member, and, as its first
// message, a summary of this session up to that message. The summary is
// written by a workflow: the fork exists at once, and takes messages once the
// summary is in.
func (h *handler) forkSession(w http.ResponseWriter, r *http.Request) {
	var req forkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.MessageID <= 0 {
		http.Error(w, "message_id is required", http.StatusBadRequest)
		return
	}
	f, err := h.fork(r.Context(), chi.URLParam(r, "id"), req.MessageID, auth.UserFrom(r.Context()))
	switch {
	case errors.Is(err, errSessionGone):
		http.Error(w, "Session not found", http.StatusNotFound)
	case errors.Is(err, errBadForkPoint):
		http.Error(w, "No such message to fork from in this session", http.StatusBadRequest)
	case err != nil:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	default:
		writeJSON(w, http.StatusCreated, createSessionResponse{SessionID: f.SessionID})
	}
}

func forkable(m store.Message) bool {
	return m.ToolResult == nil && (m.Role == store.RoleUser || m.Role == store.RoleAssistant)
}

func forkTitle(parentTitle string) string {
	if parentTitle == "" {
		return "Fork"
	}
	return "Fork : " + parentTitle
}

// Summary states of a fork.
const (
	summaryReady   = "ready"
	summaryPending = "pending"
	summaryFailed  = "failed"
)

// forkSummaryState tells where a fork's summary stands: written, still being
// written, or failed (the workflow is over and no summary came out of it).
func (h *handler) forkSummaryState(ctx context.Context, sess *store.Session) (string, error) {
	msgs, err := h.store.LoadMessages(ctx, sess.SessionID)
	if err != nil {
		return "", err
	}
	if len(msgs) > 0 && msgs[0].Kind == store.KindForkSummary {
		return summaryReady, nil
	}
	if h.isWorkflowRunning(ctx, workflow.ForkWorkflowID(sess.SessionID)) {
		return summaryPending, nil
	}
	return summaryFailed, nil
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
	Summary string `json:"summary,omitempty"`
	// Forks are this session's forks the user is a member of.
	Forks []forkInfo `json:"forks"`
}

// getSessionInfo describes a session and its place in a fork tree, as far as
// the user may see it.
func (h *handler) getSessionInfo(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	me := auth.UserFrom(ctx)
	sess, err := h.store.GetSession(ctx, chi.URLParam(r, "id"))
	if err != nil || sess == nil {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}
	info := sessionInfo{Session: *sess, Forks: []forkInfo{}}
	if info.Members, err = h.store.ListSessionMembers(ctx, sess.SessionID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if sess.ForkedAtMessageID != 0 {
		if info.Summary, err = h.forkSummaryState(ctx, sess); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if sess.ParentSessionID != "" {
		p := &parentInfo{MessageID: sess.ForkedAtMessageID}
		if ok, _ := h.store.IsSessionMember(ctx, sess.ParentSessionID, me.ID); ok {
			if parent, _ := h.store.GetSession(ctx, sess.ParentSessionID); parent != nil {
				p.Accessible = true
				p.sessionLink = &sessionLink{SessionID: parent.SessionID, Title: parent.Title}
			}
		}
		info.Parent = p
	}

	forks, err := h.store.ListForks(ctx, sess.SessionID, me.ID)
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
