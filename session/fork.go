package session

import (
	"context"
	"fmt"
	"log"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// MaxPurposeRunes bounds a fork's purpose: a goal in a sentence or a short
// paragraph, not a specification (that one is in the parent, and the summary
// quotes it).
const MaxPurposeRunes = 500

// Fork starts a new session from a message of parentID. The fork gets the
// same agent, me as its only member, and, as its first message, a summary of
// the parent up to that message, written by a workflow: the fork exists at
// once, and takes messages once the summary is in.
//
// purpose, optional, is what the fork is for: it titles the fork, and the
// summary keeps what matters for it.
func (s *Service) Fork(ctx context.Context, parentID string, messageID int64, purpose string, me *store.User) (*store.Session, error) {
	purpose = strings.TrimSpace(purpose)
	if utf8.RuneCountInString(purpose) > MaxPurposeRunes {
		return nil, ErrPurposeTooLong
	}
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
		Title:             forkTitle(parent.Title, purpose),
		AgentID:           agentID,
		Channel:           ChannelWeb,
		ParentSessionID:   parentID,
		ForkedAtMessageID: messageID,
		ForkedBy:          me.ID,
		ForkPurpose:       purpose,
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
		Purpose:         purpose,
		Model:           s.cfg.SummaryModel,
	}); err != nil {
		// Without the workflow, the fork would wait for a summary forever.
		s.store.DeleteSession(context.Background(), f.SessionID)
		return nil, fmt.Errorf("start the summary: %w", err)
	}
	log.Printf("Session %s forked at message %d into %s by %s", parentID, messageID, f.SessionID, me.ID)
	s.ringTrees(ctx, f.SessionID, me.ID)
	return &f, nil
}

func forkable(m store.Message) bool {
	return m.ToolResult == nil && (m.Role == store.RoleUser || m.Role == store.RoleAssistant)
}

// forkTitle is the purpose, the first line of it, when there is one: it
// tells the forks of one session apart. Otherwise the parent's title.
func forkTitle(parentTitle, purpose string) string {
	if purpose != "" {
		line, _, _ := strings.Cut(purpose, "\n")
		return titleFrom(line)
	}
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
// written, or failed. The fork's row says whether it is written
// (SummaryMessageID); only while it is not is its workflow read, as pages
// show it (ForkRunning).
func (s *Service) ForkSummaryState(ctx context.Context, fork *store.Session) SummaryState {
	if fork.SummaryMessageID != 0 {
		return SummaryReady
	}
	if s.ForkRunning(ctx, fork.SessionID) {
		return SummaryPending
	}
	return SummaryFailed
}

// ForkRunning reports whether the workflow writing a fork's summary runs, as
// pages show it (shownWorkflow).
func (s *Service) ForkRunning(ctx context.Context, sessionID string) bool {
	return s.shownWorkflow(ctx, workflow.ForkWorkflowID(sessionID)).status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
}
