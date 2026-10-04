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
// turn on it, one after another (the relay), each holding its own
// participant's next messages meanwhile.
const maxAgentsPerMessage = 3

// MaxQueued bounds the messages waiting for a participant: past it, a
// message for that participant is refused, before it is stored. A relay is
// not counted: the server does not deliver it.
const MaxQueued = 5

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

// Deliver takes a human message into a session, from any channel, and
// reports whether it called an agent:
//  1. it resolves who answers (answered): the agents it mentions, in order,
//     or the session's agent (the default one if it is gone); this depends
//     on the text alone;
//  2. it refuses the message when the first of them has MaxQueued messages
//     waiting already, as far as the server knows (ErrQueueFull), without
//     storing it: refused after being stored, it would be in the
//     conversation, unanswered, and sent again;
//  3. it stores it, and shows it to the members;
//  4. it delivers it to the first agent's participant, by SignalWithStart:
//     started if it does not run, queued behind its current message if it
//     does. The participant relays it to the next agents.
//
// Each turn then loads the whole conversation, the messages no agent was
// called on included.
//
// An empty message is refused: stored as an empty user turn, every later turn
// would replay it to the LLM, which rejects a user message with no content —
// the session would be poisoned for good. A fork takes messages once its
// summary is in: the summary must be its first message.
func (s *Service) Deliver(ctx context.Context, sess *store.Session, author *store.User, text string) (bool, error) {
	if strings.TrimSpace(text) == "" {
		return false, ErrEmptyMessage
	}
	if sess.ForkedAtMessageID != 0 && s.ForkSummaryState(ctx, sess) == SummaryPending {
		return false, ErrSummaryPending
	}

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
	var who []workflow.AddressedAgent
	sessionAgent := ""
	if answered(sess.AgentMode, len(members), mentioned) {
		if sessionAgent, err = s.agentOrDefault(ctx, sess.AgentID); err != nil {
			return false, err
		}
		who = mentioned
		if len(who) == 0 {
			who = []workflow.AddressedAgent{addressed(sessionAgent, agents)}
		}
		if s.turns.queued(sess.SessionID, who[0].ID) >= MaxQueued {
			return false, ErrQueueFull
		}
	}

	content, _ := json.Marshal(text)
	stored := store.Message{Role: store.RoleUser, Content: string(content), UserID: author.ID, Author: author.Name()}
	id, err := s.store.AppendMessage(ctx, sess.SessionID, store.HumanMessageKey(uuid.New().String()), stored)
	if err != nil {
		return false, fmt.Errorf("store message: %w", err)
	}
	s.inBackground(func() { s.setTitleFrom(sess.SessionID, text) })
	s.publishUserMessage(sess.SessionID, text, author, who)
	if len(who) == 0 {
		return false, nil
	}

	defer s.statuses.invalidate()
	msg := workflow.ParticipantMessage{
		MessageID: id, UserID: author.ID, UserName: author.Name(),
		Next: who[1:],
		// On the channel, an answer that could be taken for another
		// agent's is signed.
		SignReply: len(who) > 1 || who[0].ID != sessionAgent,
		Channel:   sess.Channel, ChannelID: sess.ChannelID,
	}
	if len(who) > 1 {
		msg.Part = &workflow.Part{Agents: who, Quote: workflow.Quote(text)}
	}
	if err := s.deliver(ctx, sess, who[0].ID, msg); err != nil {
		return false, err
	}
	return true, nil
}

// addressed is the agent agentID as a message addresses it, named from
// agents when it is among them.
func addressed(agentID string, agents []store.Agent) workflow.AddressedAgent {
	for _, a := range agents {
		if a.ID == agentID {
			return workflow.AddressedAgent{ID: a.ID, Name: cmp.Or(a.Name, a.ID), Mention: a.MentionName()}
		}
	}
	return workflow.AddressedAgent{ID: agentID, Name: agentID, Mention: agentID}
}

// deliver hands a message to agentID's participant in the session: one
// SignalWithStart on its fixed ID, which starts it only if it does not run,
// so two messages never start two, and one ending as the message is sent
// cannot lose it (the Temporal server refuses to close it with a signal
// unhandled). The message is counted in the participant's queue until it
// says it started it.
func (s *Service) deliver(ctx context.Context, sess *store.Session, agentID string, msg workflow.ParticipantMessage) error {
	id := workflow.ParticipantWorkflowID(sess.SessionID, agentID)
	s.turns.expect(sess.SessionID, agentID, msg.MessageID)
	if _, err := s.temporal.SignalWithStartWorkflow(ctx, id, workflow.SignalMessage, msg, client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: s.cfg.WorkflowQueue,
	}, workflow.ParticipantWorkflow, workflow.ParticipantInput{
		SessionID: sess.SessionID,
		AgentID:   agentID,
		Channel:   sess.Channel,
		ChannelID: sess.ChannelID,
	}); err != nil {
		s.turns.unexpect(sess.SessionID, agentID, msg.MessageID)
		return fmt.Errorf("deliver to %s: %w", agentID, err)
	}
	return nil
}

// publishUserMessage shows a user's message to the other members of the
// session, live, and which agents it calls, by name (none: no agent
// answers), with, for each one answering a message already, that message
// (queued_behind, by agent ID): this one waits behind it. The sender
// displays it already and skips its own.
func (s *Service) publishUserMessage(sessionID, text string, author *store.User, who []workflow.AddressedAgent) {
	names := make([]string, len(who))
	behind := map[string]int64{}
	for i, a := range who {
		names[i] = cmp.Or(a.Name, a.ID)
		if current := s.QueuedBehind(sessionID, a.ID); current != 0 {
			behind[a.ID] = current
		}
	}
	data, _ := json.Marshal(map[string]any{
		"content":       text,
		"user_id":       author.ID,
		"author":        author.Name(),
		"agent_called":  len(who) > 0,
		"agents":        names,
		"queued_behind": behind,
	})
	s.hub.Publish(sessionID, activity.SSEEvent{Type: EventUserMessage, Data: data})
}

// maxTitleRunes bounds a session title taken from its first message.
const maxTitleRunes = 80

// setTitleFrom titles a session after a message, if it has no title yet. The
// cut is in characters: cut in bytes, an accented letter can be split, and
// Postgres refuses the invalid UTF-8.
func (s *Service) setTitleFrom(sessionID, text string) {
	if err := s.store.UpdateSessionTitle(context.Background(), sessionID, titleFrom(text)); err != nil {
		log.Printf("Session %s: set title: %v", sessionID, err)
	}
	// The trees show the message's session first, under its title.
	s.ringTrees(context.Background(), sessionID)
}

// titleFrom is text as a title: trimmed, and cut to maxTitleRunes.
func titleFrom(text string) string {
	title := strings.TrimSpace(text)
	if r := []rune(title); len(r) > maxTitleRunes {
		title = string(r[:maxTitleRunes]) + "..."
	}
	return title
}
