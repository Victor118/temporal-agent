package session

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// mentionPattern finds an @mention: @ at the start or after a non-word
// character (so an email address is not one), then a name.
var mentionPattern = regexp.MustCompile(`(?:^|[^\w@.])@([\w-]+)`)

// maxAgentsPerMessage bounds the agents one message calls: each runs a full
// turn, one after another, while the session's next messages wait.
const maxAgentsPerMessage = 3

// mentionedAgents returns the agents text calls by their mentions, in the
// order they first appear, once each. Case does not matter, and a mention
// that names no agent (a member, say) is ignored. Beyond max agents, the
// mentions are dropped, and returned apart.
//
// Any agent of the installation may be called, for now: which agents a
// session may call is a later restriction.
func mentionedAgents(text string, agents []store.Agent, max int) (called []workflow.AddressedAgent, dropped []string) {
	// The store keeps a mention from being another agent's ID; were they to
	// meet anyway, the explicit mention wins over the ID standing in for one,
	// whatever the order of agents.
	byMention := make(map[string]store.Agent, len(agents))
	for _, a := range agents {
		key := strings.ToLower(a.MentionName())
		if prev, ok := byMention[key]; ok && prev.Mention != "" && a.Mention == "" {
			continue
		}
		byMention[key] = a
	}
	seen := map[string]bool{}
	for _, m := range mentionPattern.FindAllStringSubmatch(text, -1) {
		a, ok := byMention[strings.ToLower(m[1])]
		if !ok || seen[a.ID] {
			continue
		}
		seen[a.ID] = true
		if len(called) == max {
			dropped = append(dropped, a.MentionName())
			continue
		}
		called = append(called, workflow.AddressedAgent{ID: a.ID, Name: cmp.Or(a.Name, a.ID), Mention: a.MentionName()})
	}
	return called, dropped
}

// answered decides whether a human message starts agent turns. The agents it
// mentions answer, whatever the session's mode. A message that mentions none
// calls the session's agent, as the mode says: alone in a session, a user
// talks to the agent; once several share it, they talk to each other and
// call agents by their mentions. A session can force either.
func answered(mode string, members int, mentioned []workflow.AddressedAgent) bool {
	if len(mentioned) > 0 {
		return true
	}
	switch mode {
	case store.AgentModeAlways:
		return true
	case store.AgentModeMention:
		return false
	default: // auto
		return members <= 1
	}
}

// Deliver takes a human message into a session, from any channel: it stores
// it at once, shows it to the members, and starts the turns of the agents the
// message calls (answered). Each turn then loads the whole conversation, the
// messages no agent was called on included. Reports whether it called one.
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
	if err := s.store.AppendMessage(ctx, sess.SessionID, store.HumanMessageKey(uuid.New().String()), stored); err != nil {
		return false, fmt.Errorf("store message: %w", err)
	}
	go s.setTitleFrom(sess.SessionID, text)

	members, err := s.store.ListSessionMembers(ctx, sess.SessionID)
	if err != nil {
		return false, fmt.Errorf("list members: %w", err)
	}
	agents, err := s.store.ListAgents(ctx)
	if err != nil {
		return false, fmt.Errorf("list agents: %w", err)
	}
	mentioned, dropped := mentionedAgents(text, agents, maxAgentsPerMessage)
	if len(dropped) > 0 {
		log.Printf("Session %s: a message calls more than %d agents; not called: %s", sess.SessionID, maxAgentsPerMessage, strings.Join(dropped, ", "))
	}
	called := answered(sess.AgentMode, len(members), mentioned)
	// No agent mentioned: the session's agent answers (an empty list).
	msg := workflow.UserMessage{Text: text, UserID: author.ID, UserName: author.Name(), Stored: true, Agents: mentioned}
	s.publishUserMessage(sess.SessionID, msg, answering(called, mentioned, sess.AgentID, s.cfg.DefaultAgentID, agents))
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
// session, live, and which agents it called, by name (none: no agent
// answers). The sender displays it already and skips its own.
func (s *Service) publishUserMessage(sessionID string, msg workflow.UserMessage, agents []string) {
	data, _ := json.Marshal(map[string]any{
		"content":      msg.Text,
		"user_id":      msg.UserID,
		"author":       msg.UserName,
		"agent_called": len(agents) > 0,
		"agents":       agents,
	})
	s.hub.Publish(sessionID, activity.SSEEvent{Type: "user_message", Data: data})
}

// answering names the agents a message calls, in the order they answer: the
// ones it mentions, or the session's agent (the default one when it names
// none, or is gone). Nil when the message calls none.
func answering(called bool, mentioned []workflow.AddressedAgent, sessionAgent, defaultAgent string, agents []store.Agent) []string {
	if !called {
		return nil
	}
	names := make([]string, 0, max(len(mentioned), 1))
	for _, a := range mentioned {
		names = append(names, a.Name)
	}
	if len(names) > 0 {
		return names
	}
	for _, id := range []string{sessionAgent, defaultAgent} {
		for _, a := range agents {
			if id != "" && a.ID == id {
				return append(names, cmp.Or(a.Name, a.ID))
			}
		}
	}
	return append(names, cmp.Or(sessionAgent, defaultAgent))
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
