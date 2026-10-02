package session

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// Fork starts a new session from a message of parentID. The fork gets the
// same agent, me as its only member, and, as its first message, a summary of
// the parent up to that message, written by a workflow: the fork exists at
// once, and takes messages once the summary is in.
func (s *Service) Fork(ctx context.Context, parentID string, messageID int64, me *store.User) (*store.Session, error) {
	defer s.statuses.invalidate()
	parent, err := s.Get(ctx, parentID)
	if err != nil {
		return nil, err
	}
	// The message must be one of this session's, and one a user can see: a
	// user or assistant message, not a tool result.
	msgs, err := s.store.LoadMessagesUpTo(ctx, parentID, messageID)
	if err != nil {
		return nil, err
	}
	if messageID <= 0 || len(msgs) == 0 || msgs[len(msgs)-1].ID != messageID || !forkable(msgs[len(msgs)-1].Message) {
		return nil, ErrBadForkPoint
	}

	agentID, err := s.agentOrDefault(ctx, parent.AgentID)
	if err != nil {
		return nil, err
	}
	f := store.Session{
		SessionID:         uuid.New().String(),
		CreatedBy:         me.ID,
		Title:             forkTitle(parent.Title),
		AgentID:           agentID,
		Channel:           ChannelWeb,
		ParentSessionID:   parentID,
		ForkedAtMessageID: messageID,
		ForkedBy:          me.ID,
	}
	if err := s.store.CreateSession(ctx, f); err != nil {
		return nil, fmt.Errorf("create the fork: %w", err)
	}
	if _, err := s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflow.ForkWorkflowID(f.SessionID),
		TaskQueue: s.cfg.WorkflowQueue,
	}, workflow.ForkSessionWorkflow, workflow.ForkSessionInput{
		ForkSessionID:   f.SessionID,
		ParentSessionID: parentID,
		UpToMessageID:   messageID,
		Model:           s.cfg.SummaryModel,
	}); err != nil {
		// Without the workflow, the fork would wait for a summary forever.
		s.store.DeleteSession(context.Background(), f.SessionID)
		return nil, fmt.Errorf("start the summary: %w", err)
	}
	log.Printf("Session %s forked at message %d into %s by %s", parentID, messageID, f.SessionID, me.ID)
	return &f, nil
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

// SummaryState is where a fork's starting summary stands.
type SummaryState string

const (
	SummaryReady   SummaryState = "ready"
	SummaryPending SummaryState = "pending"
	SummaryFailed  SummaryState = "failed" // the workflow is over and no summary came out of it
)

// ForkSummaryState tells where a fork's summary stands: written, still being
// written, or failed.
func (s *Service) ForkSummaryState(ctx context.Context, sess *store.Session) (SummaryState, error) {
	msgs, err := s.store.LoadMessages(ctx, sess.SessionID)
	if err != nil {
		return "", err
	}
	if len(msgs) > 0 && msgs[0].Kind == store.KindForkSummary {
		return SummaryReady, nil
	}
	if s.ForkRunning(ctx, sess.SessionID) {
		return SummaryPending, nil
	}
	return SummaryFailed, nil
}

// ForkRunning reports whether the workflow writing a fork's summary runs.
func (s *Service) ForkRunning(ctx context.Context, sessionID string) bool {
	return s.isWorkflowRunning(ctx, workflow.ForkWorkflowID(sessionID))
}
