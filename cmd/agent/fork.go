package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"go.temporal.io/sdk/client"

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
	parentID := chi.URLParam(r, "id")
	me := auth.UserFrom(r.Context())

	var req forkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.MessageID <= 0 {
		http.Error(w, "message_id is required", http.StatusBadRequest)
		return
	}
	parent, err := h.store.GetSession(r.Context(), parentID)
	if err != nil || parent == nil {
		http.Error(w, "Session not found", http.StatusNotFound)
		return
	}
	// The message must be one of this session's, and one a user can see: a
	// user or assistant message, not a tool result.
	msgs, err := h.store.LoadMessagesUpTo(r.Context(), parentID, req.MessageID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to load messages: %v", err), http.StatusInternalServerError)
		return
	}
	if len(msgs) == 0 || msgs[len(msgs)-1].ID != req.MessageID || !forkable(msgs[len(msgs)-1].Message) {
		http.Error(w, "No such message to fork from in this session", http.StatusBadRequest)
		return
	}

	agentID, err := h.resolveAgentID(r.Context(), parent.AgentID)
	if err != nil {
		agentID, err = h.resolveAgentID(r.Context(), "") // the parent's agent is gone
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fork := store.Session{
		SessionID:         newUUID(),
		CreatedBy:         me.ID,
		Title:             forkTitle(parent.Title),
		AgentID:           agentID,
		Channel:           "web",
		ParentSessionID:   parentID,
		ForkedAtMessageID: req.MessageID,
		ForkedBy:          me.ID,
	}
	if err := h.store.CreateSession(r.Context(), fork); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create the fork: %v", err), http.StatusInternalServerError)
		return
	}
	if _, err := h.temporalClient.ExecuteWorkflow(r.Context(), client.StartWorkflowOptions{
		ID:        workflow.ForkWorkflowID(fork.SessionID),
		TaskQueue: h.cfg.WorkflowQueue,
	}, workflow.ForkSessionWorkflow, workflow.ForkSessionInput{
		ForkSessionID:   fork.SessionID,
		ParentSessionID: parentID,
		UpToMessageID:   req.MessageID,
		Model:           h.cfg.SummaryModel,
	}); err != nil {
		// Without the workflow, the fork would wait for a summary forever.
		h.store.DeleteSession(context.Background(), fork.SessionID)
		http.Error(w, fmt.Sprintf("Failed to start the summary: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("Session %s forked at message %d into %s by %s", parentID, req.MessageID, fork.SessionID, me.ID)
	writeJSON(w, http.StatusCreated, createSessionResponse{SessionID: fork.SessionID})
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
