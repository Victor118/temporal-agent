package session

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"

	"github.com/victor/temporal-agent/store"
)

// OpenOptions describe a new session. Everything is optional.
type OpenOptions struct {
	AgentID   string // empty = the default agent
	Channel   string // where the user is reached; empty = the web
	ChannelID string // the user's address on that channel (a Telegram chat)
}

// Open creates a session for me, and returns its ID. Nothing runs for it
// until a message calls an agent.
func (s *Service) Open(ctx context.Context, me *store.User, o OpenOptions) (string, error) {
	agentID, err := s.resolveAgentID(ctx, o.AgentID)
	if err != nil {
		return "", err
	}
	channel := o.Channel
	if channel == "" {
		channel = ChannelWeb
	}
	sessionID := uuid.New().String()
	// Without the record nobody is a member, so nobody could open the session.
	if err := s.store.CreateSession(ctx, store.Session{
		SessionID: sessionID,
		CreatedBy: me.ID,
		AgentID:   agentID,
		Channel:   channel,
		ChannelID: o.ChannelID,
	}); err != nil {
		return "", fmt.Errorf("persist session: %w", err)
	}
	s.ringTrees(ctx, sessionID)
	return sessionID, nil
}

// OpenOnChannel returns the session a user talks to on a channel, opening
// one if they have none there yet.
func (s *Service) OpenOnChannel(ctx context.Context, user *store.User, channel, channelID string) (*store.Session, error) {
	sess, err := s.store.GetActiveSessionByChannel(ctx, user.ID, channel, channelID)
	if err != nil || sess != nil {
		return sess, err
	}
	id, err := s.Open(ctx, user, OpenOptions{Channel: channel, ChannelID: channelID})
	if err != nil {
		return nil, err
	}
	log.Printf("Session %s opened on %s for user %s", id, channel, user.ID)
	return s.Get(ctx, id)
}

// Get returns a session, or ErrNotFound.
func (s *Service) Get(ctx context.Context, sessionID string) (*store.Session, error) {
	sess, err := s.store.GetSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	if sess == nil {
		return nil, ErrNotFound
	}
	return sess, nil
}

// IsMember reports whether userID may use the session.
func (s *Service) IsMember(ctx context.Context, sessionID, userID string) (bool, error) {
	return s.store.IsSessionMember(ctx, sessionID, userID)
}

// Delete deletes a session for every member. Only its creator may.
func (s *Service) Delete(ctx context.Context, sessionID, by string) error {
	defer s.statuses.invalidate()
	sess, err := s.Get(ctx, sessionID)
	if err != nil {
		return err
	}
	if sess.CreatedBy != by {
		return ErrNotCreator
	}
	// Its participants and its background tasks end with it: a turn would
	// write into a session gone, a question wait for days, a task run for
	// nobody.
	s.terminateParticipants(ctx, sessionID, "session deleted by user")
	s.terminateTasks(ctx, sessionID, "session deleted by user")
	members, _ := s.store.ListSessionMembers(ctx, sessionID)
	if err := s.store.DeleteSession(ctx, sessionID); err != nil {
		return err
	}
	s.turns.forget(sessionID)
	ids := make([]string, len(members))
	for i, m := range members {
		ids[i] = m.UserID
	}
	s.publishMembersLeft(sessionID, ids...)
	s.ringTrees(ctx, sessionID, ids...) // the session is gone: it has no members to list
	return nil
}

// Invite adds a user to a session, by email. Any member may invite.
func (s *Service) Invite(ctx context.Context, sessionID, email, by string) error {
	u, err := s.store.GetUserByEmail(ctx, email)
	if err != nil {
		return err
	}
	if u == nil || u.DisabledAt != nil {
		return ErrNoSuchUser
	}
	if err := s.store.AddSessionMember(ctx, sessionID, u.ID, by); err != nil {
		return err
	}
	log.Printf("Session %s: %s added %s", sessionID, by, u.ID)
	s.ringTrees(ctx, sessionID)
	return nil
}

// Leave takes a member out of a session. When the last member leaves, the
// session goes: nobody could open it any more.
func (s *Service) Leave(ctx context.Context, sessionID, userID string) error {
	defer s.statuses.invalidate()
	if err := s.store.RemoveSessionMember(ctx, sessionID, userID); err != nil {
		return err
	}
	s.publishMembersLeft(sessionID, userID)
	defer s.ringTrees(ctx, sessionID, userID)
	members, err := s.store.ListSessionMembers(ctx, sessionID)
	if err == nil && len(members) == 0 {
		s.terminateParticipants(ctx, sessionID, "last member left")
		s.terminateTasks(ctx, sessionID, "last member left")
		if err := s.store.DeleteSession(ctx, sessionID); err != nil {
			log.Printf("Session %s: delete after last member left: %v", sessionID, err)
		}
		s.turns.forget(sessionID)
	}
	return nil
}

// SetAgentMode sets when human messages call the session's agent. Any member
// may.
func (s *Service) SetAgentMode(ctx context.Context, sessionID, mode string) error {
	switch mode {
	case store.AgentModeAuto, store.AgentModeAlways, store.AgentModeMention:
	default:
		return ErrBadMode
	}
	return s.store.SetSessionAgentMode(ctx, sessionID, mode)
}

// resolveAgentID returns requested if it is a known agent. With no request, it
// returns the configured default agent, or the first agent if that one is missing.
func (s *Service) resolveAgentID(ctx context.Context, requested string) (string, error) {
	if requested != "" {
		a, err := s.store.GetAgent(ctx, requested)
		if err != nil {
			return "", fmt.Errorf("load agent %q: %w", requested, err)
		}
		if a == nil {
			return "", fmt.Errorf("unknown agent_id %q", requested)
		}
		return a.ID, nil
	}

	agents, err := s.store.ListAgents(ctx)
	if err != nil {
		return "", fmt.Errorf("list agents: %w", err)
	}
	for _, a := range agents {
		if a.ID == s.cfg.DefaultAgentID {
			return a.ID, nil
		}
	}
	if len(agents) > 0 {
		return agents[0].ID, nil
	}
	return "", fmt.Errorf("no agents configured")
}

// agentOrDefault is the session's agent, or the default one if it is gone.
func (s *Service) agentOrDefault(ctx context.Context, agentID string) (string, error) {
	id, err := s.resolveAgentID(ctx, agentID)
	if err != nil {
		id, err = s.resolveAgentID(ctx, "")
	}
	return id, err
}
