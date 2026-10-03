package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// mentionPattern finds an @mention: @ at the start or after a non-word
// character (so an email address is not one), then a name.
var mentionPattern = regexp.MustCompile(`(?:^|[^\w@.])@([\w-]+)`)

// mentionsAgent reports whether text calls the session's agent: @agent, or
// the agent by its ID (@default). Case does not matter.
func mentionsAgent(text, agentID string) bool {
	for _, m := range mentionPattern.FindAllStringSubmatch(text, -1) {
		if name := m[1]; strings.EqualFold(name, "agent") || (agentID != "" && strings.EqualFold(name, agentID)) {
			return true
		}
	}
	return false
}

// callsAgent decides whether a human message starts an agent turn. Alone in a
// session, a user talks to the agent; once several share it, they talk to
// each other and call the agent with @agent. A session can force either.
func callsAgent(mode string, members int, text, agentID string) bool {
	switch mode {
	case store.AgentModeAlways:
		return true
	case store.AgentModeMention:
		return mentionsAgent(text, agentID)
	default: // auto
		return members <= 1 || mentionsAgent(text, agentID)
	}
}

// Deliver takes a human message into a session, from any channel: it stores
// it at once, shows it to the members, and starts an agent turn if the
// message calls the agent. The turn then loads the whole conversation, the
// messages the agent was not called on included. Reports whether it called.
//
// An empty message is refused: stored as an empty user turn, every later turn
// would replay it to the LLM, which rejects a user message with no content —
// the session would be poisoned for good. A fork takes messages once its
// summary is in: the summary must be its first message.
func (s *Service) Deliver(ctx context.Context, sess *store.Session, author *store.User, text string) (bool, error) {
	if strings.TrimSpace(text) == "" {
		return false, ErrEmptyMessage
	}
	if sess.ForkedAtMessageID != 0 {
		if state, err := s.ForkSummaryState(ctx, sess); err == nil && state == SummaryPending {
			return false, ErrSummaryPending
		}
	}

	content, _ := json.Marshal(text)
	stored := store.Message{Role: store.RoleUser, Content: string(content), UserID: author.ID, Author: author.Name()}
	if err := s.store.AppendMessage(ctx, sess.SessionID, "msg:"+uuid.New().String(), stored); err != nil {
		return false, fmt.Errorf("store message: %w", err)
	}
	go s.setTitleFrom(sess.SessionID, text)

	members, err := s.store.ListSessionMembers(ctx, sess.SessionID)
	if err != nil {
		return false, fmt.Errorf("list members: %w", err)
	}
	called := callsAgent(sess.AgentMode, len(members), text, sess.AgentID)
	msg := workflow.UserMessage{Text: text, UserID: author.ID, UserName: author.Name(), Stored: true}
	s.publishUserMessage(sess.SessionID, msg, called)
	if !called {
		return false, nil
	}

	defer s.statuses.invalidate()
	if err := s.signalSession(ctx, sess, msg); err != nil {
		return false, err
	}
	return true, nil
}

// signalSession hands a message to the session's workflow, starting a new run
// when the last one is over (an idle session times out). Signal and start are
// one call on the session's fixed ID: Temporal starts a run only if none is
// running, so two messages never start two runs, and a run ending while the
// message is sent cannot lose it.
func (s *Service) signalSession(ctx context.Context, sess *store.Session, msg workflow.UserMessage) error {
	legacy, err := s.legacyRunID(ctx, sess.SessionID)
	if err != nil {
		return fmt.Errorf("signal session: %w", err)
	}
	if legacy != "" {
		err := s.temporal.SignalWorkflow(ctx, legacy, "", workflow.SignalUserMessage, msg)
		var gone *serviceerror.NotFound
		if !errors.As(err, &gone) {
			if err != nil {
				return fmt.Errorf("signal session: %w", err)
			}
			return nil
		}
		// It ended since it was listed: start on the fixed ID.
	}
	// Resume with the session's agent, or the default one if it is gone.
	agentID, err := s.agentOrDefault(ctx, sess.AgentID)
	if err != nil {
		return err
	}
	id := sessionWorkflowID(sess.SessionID)
	if _, err := s.temporal.SignalWithStartWorkflow(ctx, id, workflow.SignalUserMessage, msg, client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: s.cfg.WorkflowQueue,
	}, workflow.SessionWorkflow, workflow.SessionWorkflowInput{
		SessionID: sess.SessionID,
		AgentID:   agentID,
		Channel:   sess.Channel,
		ChannelID: sess.ChannelID,
	}); err != nil {
		return fmt.Errorf("signal session: %w", err)
	}
	return nil
}

// publishUserMessage shows a user's message to the other members of the
// session, live, and whether it called the agent. The sender displays it
// already and skips its own.
func (s *Service) publishUserMessage(sessionID string, msg workflow.UserMessage, called bool) {
	data, _ := json.Marshal(map[string]any{
		"content":      msg.Text,
		"user_id":      msg.UserID,
		"author":       msg.UserName,
		"agent_called": called,
	})
	s.hub.Publish(sessionID, activity.SSEEvent{Type: "user_message", Data: data})
}

// maxTitleRunes bounds a session title taken from its first message.
const maxTitleRunes = 80

// setTitleFrom titles a session after a message, if it has no title yet. The
// cut is in characters: cut in bytes, an accented letter can be split, and
// Postgres refuses the invalid UTF-8.
func (s *Service) setTitleFrom(sessionID, text string) {
	title := strings.TrimSpace(text)
	if r := []rune(title); len(r) > maxTitleRunes {
		title = string(r[:maxTitleRunes]) + "..."
	}
	if err := s.store.UpdateSessionTitle(context.Background(), sessionID, title); err != nil {
		log.Printf("Session %s: set title: %v", sessionID, err)
	}
}
