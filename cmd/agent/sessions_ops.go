package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// The operations on sessions, shared by the JSON API and the HTML interface:
// each interface only decodes its request and shapes its answer.

var (
	errNotCreator   = errors.New("only the session's creator can delete it; leave it instead")
	errNoSuchUser   = errors.New("no active user with this email")
	errBadForkPoint = errors.New("no such message to fork from in this session")
	errSessionGone  = errors.New("session not found")
)

// openSession creates a session for me, with its workflow, and returns its
// ID. agentID empty means the default agent.
func (h *handler) openSession(ctx context.Context, me *store.User, agentID, systemPrompt, model string) (string, error) {
	agentID, err := h.resolveAgentID(ctx, agentID)
	if err != nil {
		return "", err
	}
	sessionID := newUUID()
	if _, err := h.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "session-" + sessionID,
		TaskQueue: h.cfg.WorkflowQueue,
	}, workflow.SessionWorkflow, workflow.SessionWorkflowInput{
		SessionID:    sessionID,
		AgentID:      agentID,
		SystemPrompt: systemPrompt, // optional override; empty = built from the agent
		Model:        model,        // explicit choice only; empty = the worker's LLM_MODEL
	}); err != nil {
		return "", fmt.Errorf("start session: %w", err)
	}
	// Without the record nobody is a member, so nobody could open the session.
	if err := h.store.CreateSession(ctx, store.Session{SessionID: sessionID, CreatedBy: me.ID, AgentID: agentID, Channel: "web"}); err != nil {
		return "", fmt.Errorf("persist session: %w", err)
	}
	return sessionID, nil
}

// removeSession deletes a session for every member. Only its creator may.
func (h *handler) removeSession(ctx context.Context, sessionID, by string) error {
	sess, err := h.store.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}
	if sess == nil {
		return errSessionGone
	}
	if sess.CreatedBy != by {
		return errNotCreator
	}
	if wf := h.findActiveWorkflowID(ctx, sessionID); wf != "" {
		_ = h.temporalClient.TerminateWorkflow(ctx, wf, "", "session deleted by user")
	}
	return h.store.DeleteSession(ctx, sessionID)
}

// inviteByEmail adds a user to a session. Any member may invite.
func (h *handler) inviteByEmail(ctx context.Context, sessionID, email, by string) error {
	u, err := h.store.GetUserByEmail(ctx, email)
	if err != nil {
		return err
	}
	if u == nil || u.DisabledAt != nil {
		return errNoSuchUser
	}
	if err := h.store.AddSessionMember(ctx, sessionID, u.ID, by); err != nil {
		return err
	}
	log.Printf("Session %s: %s added %s", sessionID, by, u.ID)
	return nil
}

// leave takes a member out of a session. When the last member leaves, the
// session goes: nobody could open it any more.
func (h *handler) leave(ctx context.Context, sessionID, userID string) error {
	if err := h.store.RemoveSessionMember(ctx, sessionID, userID); err != nil {
		return err
	}
	members, err := h.store.ListSessionMembers(ctx, sessionID)
	if err == nil && len(members) == 0 {
		if wf := h.findActiveWorkflowID(ctx, sessionID); wf != "" {
			_ = h.temporalClient.TerminateWorkflow(ctx, wf, "", "last member left")
		}
		if err := h.store.DeleteSession(ctx, sessionID); err != nil {
			log.Printf("Session %s: delete after last member left: %v", sessionID, err)
		}
	}
	return nil
}

// fork starts a new session from a message of parentID. The fork gets the
// same agent, me as its only member, and, as its first message, a summary of
// the parent up to that message, written by a workflow: the fork exists at
// once, and takes messages once the summary is in.
func (h *handler) fork(ctx context.Context, parentID string, messageID int64, me *store.User) (*store.Session, error) {
	parent, err := h.store.GetSession(ctx, parentID)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		return nil, errSessionGone
	}
	// The message must be one of this session's, and one a user can see: a
	// user or assistant message, not a tool result.
	msgs, err := h.store.LoadMessagesUpTo(ctx, parentID, messageID)
	if err != nil {
		return nil, err
	}
	if messageID <= 0 || len(msgs) == 0 || msgs[len(msgs)-1].ID != messageID || !forkable(msgs[len(msgs)-1].Message) {
		return nil, errBadForkPoint
	}

	agentID, err := h.resolveAgentID(ctx, parent.AgentID)
	if err != nil {
		agentID, err = h.resolveAgentID(ctx, "") // the parent's agent is gone
	}
	if err != nil {
		return nil, err
	}
	f := store.Session{
		SessionID:         newUUID(),
		CreatedBy:         me.ID,
		Title:             forkTitle(parent.Title),
		AgentID:           agentID,
		Channel:           "web",
		ParentSessionID:   parentID,
		ForkedAtMessageID: messageID,
		ForkedBy:          me.ID,
	}
	if err := h.store.CreateSession(ctx, f); err != nil {
		return nil, fmt.Errorf("create the fork: %w", err)
	}
	if _, err := h.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflow.ForkWorkflowID(f.SessionID),
		TaskQueue: h.cfg.WorkflowQueue,
	}, workflow.ForkSessionWorkflow, workflow.ForkSessionInput{
		ForkSessionID:   f.SessionID,
		ParentSessionID: parentID,
		UpToMessageID:   messageID,
		Model:           h.cfg.SummaryModel,
	}); err != nil {
		// Without the workflow, the fork would wait for a summary forever.
		h.store.DeleteSession(context.Background(), f.SessionID)
		return nil, fmt.Errorf("start the summary: %w", err)
	}
	log.Printf("Session %s forked at message %d into %s by %s", parentID, messageID, f.SessionID, me.ID)
	return &f, nil
}
