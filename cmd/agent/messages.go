package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

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

// deliverMessage takes a human message into a session, from any channel: it
// stores it at once, shows it to the members, and starts an agent turn if the
// message calls the agent. The turn then loads the whole conversation, the
// messages the agent was not called on included. Reports whether it called.
func (h *handler) deliverMessage(ctx context.Context, sess *store.Session, author *store.User, text string) (bool, error) {
	defer h.statuses.invalidate() // the states shown next must not predate this
	content, _ := json.Marshal(text)
	stored := store.Message{Role: store.RoleUser, Content: string(content), UserID: author.ID, Author: author.Name()}
	if err := h.store.AppendMessage(ctx, sess.SessionID, "msg:"+newUUID(), stored); err != nil {
		return false, fmt.Errorf("store message: %w", err)
	}
	go h.setTitleFrom(sess.SessionID, text)

	members, err := h.store.ListSessionMembers(ctx, sess.SessionID)
	if err != nil {
		return false, fmt.Errorf("list members: %w", err)
	}
	called := callsAgent(sess.AgentMode, len(members), text, sess.AgentID)
	msg := workflow.UserMessage{Text: text, UserID: author.ID, UserName: author.Name(), Stored: true}
	h.publishUserMessage(sess.SessionID, msg, called)
	if !called {
		return false, nil
	}

	workflowID, err := h.ensureSessionWorkflow(ctx, sess)
	if err != nil {
		return false, err
	}
	if err := h.temporalClient.SignalWorkflow(ctx, workflowID, "", workflow.SignalUserMessage, msg); err != nil {
		return false, fmt.Errorf("signal session: %w", err)
	}
	return true, nil
}

// ensureSessionWorkflow returns the running workflow of a session, starting a
// new run when the last one is over (an idle session times out).
func (h *handler) ensureSessionWorkflow(ctx context.Context, sess *store.Session) (string, error) {
	if id := h.findActiveWorkflowID(ctx, sess.SessionID); id != "" {
		return id, nil
	}
	// Resume with the session's agent, or the default one if it is gone.
	agentID, err := h.resolveAgentID(ctx, sess.AgentID)
	if err != nil {
		agentID, err = h.resolveAgentID(ctx, "")
	}
	if err != nil {
		return "", err
	}
	id := fmt.Sprintf("session-%s-%d", sess.SessionID, time.Now().Unix())
	if _, err := h.temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: h.cfg.WorkflowQueue,
	}, workflow.SessionWorkflow, workflow.SessionWorkflowInput{
		SessionID: sess.SessionID,
		AgentID:   agentID,
		Channel:   sess.Channel,
		ChannelID: sess.ChannelID,
	}); err != nil {
		return "", fmt.Errorf("resume session: %w", err)
	}
	log.Printf("Session %s resumed with workflow %s", sess.SessionID, id)
	return id, nil
}

// publishUserMessage shows a user's message to the other members of the
// session, live, and whether it called the agent. The sender displays it
// already and skips its own.
func (h *handler) publishUserMessage(sessionID string, msg workflow.UserMessage, called bool) {
	data, _ := json.Marshal(map[string]any{
		"content":      msg.Text,
		"user_id":      msg.UserID,
		"author":       msg.UserName,
		"agent_called": called,
	})
	h.hub.Publish(sessionID, activity.SSEEvent{Type: "user_message", Data: data})
}
