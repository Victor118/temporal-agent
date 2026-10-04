package session

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// Participant is where a participant of a session stands: what the turn
// events told the server, or past them what its state query answers.
type Participant struct {
	// Participant is its name in the session: its agent's ID.
	Participant string
	AgentID     string
	// Name is its agent's name, as a turn event gave it; empty: the caller
	// names it.
	Name    string
	Working bool
	// Turn is the turn it is on, and UserID and UserName the author of the
	// message it answers, who may stop it; Since is when it started. Empty
	// when not known: a participant seen running, which nothing told more
	// of.
	Turn     string
	UserID   string
	UserName string
	Since    time.Time
	// Note is what its turn waits for (a coding run waiting for a free
	// worker); Waiting, a question of its turn waits for a member's answer.
	Note    string
	Waiting bool
	// Queued are the messages it has waiting: those this server delivered
	// and saw not started, or as its state query says.
	Queued int
	// Background are its background tasks, as its state query says; none
	// when the events told enough.
	Background []string
	// known: the turn events told of it.
	known bool
}

func (p Participant) working() Working {
	return Working{Participant: p.Participant, AgentID: p.AgentID, Name: p.Name, Note: p.Note}
}

// Participants are where the session's participants stand: those the turn
// events told of in the last day, working or not, and those the visibility
// queries see running. The events come first: they are on time, the
// queries lag. A participant running that no event told of (after the
// server restarted) answers its state query, all at once, each answer kept
// statusesTTL. In the order of their names.
func (s *Service) Participants(ctx context.Context, sessionID string) []Participant {
	v := s.statuses.get(ctx, s.loadVisible)
	ps := s.turns.snapshot(sessionID, v.runningIn(sessionID))
	var wg sync.WaitGroup
	for i := range ps {
		if ps[i].known {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := &ps[i]
			st, ok := s.statuses.participantState(ctx, workflow.ParticipantWorkflowID(sessionID, p.Participant), s.queryState)
			if !ok {
				return // running, as far as anyone knows
			}
			p.Working, p.Queued, p.Background = st.Current != nil, st.Queued, st.Background
			if c := st.Current; c != nil {
				p.Turn, p.UserID, p.UserName, p.Since = c.Turn, c.UserID, c.UserName, c.Since
			}
		}()
	}
	wg.Wait()
	asking := v.askingIn(sessionID)
	for i := range ps {
		ps[i].Waiting = ps[i].Working && slices.Contains(asking, ps[i].Participant)
	}
	return ps
}

// MayStop reports whether a user may stop a participant's turn: the author
// of the message it answers may, and the session's creator, any turn.
func MayStop(sess *store.Session, authorID, userID string) bool {
	return userID != "" && (sess.CreatedBy == userID || authorID == userID)
}

// MayClear reports whether a user may stop a participant and drop the
// messages it has waiting: the session's creator alone, since they may be
// other members'.
func MayClear(sess *store.Session, userID string) bool {
	return userID != "" && sess.CreatedBy == userID
}

// participantID is the workflow ID of a participant of the session, or
// false for a name no participant has: a name holds no ':', or it would
// name another workflow of the session.
func participantID(sessionID, participant string) (string, bool) {
	if participant == "" || strings.Contains(participant, ":") || checkSessionID(sessionID) != nil {
		return "", false
	}
	return workflow.ParticipantWorkflowID(sessionID, participant), true
}

// StopTurn stops a participant's turn, turn: the one the member saw. Only
// the author of the message it answers may, or the session's creator
// (MayStop). The turn is read now, from the participant itself: it may have
// changed since the member saw it. The stop names the turn, which the
// participant stops only if it still runs it: a turn started meanwhile,
// another member's maybe, goes on. With no turn named, the turn running is
// stopped, whichever it is; the creator, who may stop any, then needs no
// reading.
//
// ErrStopNotAllowed: the turn answers another member, and the user did not
// create the session. ErrTurnOver: the turn named is over. ErrNothingToStop:
// the participant runs no turn.
func (s *Service) StopTurn(ctx context.Context, sess *store.Session, participant, turn string, me *store.User) error {
	id, ok := participantID(sess.SessionID, participant)
	if !ok {
		return ErrNothingToStop
	}
	defer s.statuses.invalidate()
	stop := workflow.StopTurn{TurnKey: turn}
	over := ErrNothingToStop // no turn runs: the one named, if any, is over
	if turn != "" {
		over = ErrTurnOver
	}
	if turn != "" || sess.CreatedBy != me.ID {
		st, err := s.queryState(ctx, id)
		var gone *serviceerror.NotFound
		switch {
		case errors.As(err, &gone):
			return over
		case err != nil:
			return fmt.Errorf("read the turn of %s: %w", id, err)
		case st.Current == nil:
			return over
		case turn != "" && st.Current.Turn != turn:
			return ErrTurnOver
		case !MayStop(sess, st.Current.UserID, me.ID):
			return ErrStopNotAllowed
		}
		stop.TurnKey = st.Current.Turn
	}
	if err := s.signalParticipant(ctx, id, workflow.SignalStopTurn, stop); err != nil {
		if errors.Is(err, ErrNothingToStop) {
			return over
		}
		return err
	}
	return nil
}

// Clear stops a participant's turn and drops the messages it has waiting,
// each with an end saying so. The session's creator alone may (MayClear).
func (s *Service) Clear(ctx context.Context, sess *store.Session, participant string, me *store.User) error {
	if !MayClear(sess, me.ID) {
		return ErrClearNotAllowed
	}
	id, ok := participantID(sess.SessionID, participant)
	if !ok {
		return ErrNothingToStop
	}
	defer s.statuses.invalidate()
	return s.signalParticipant(ctx, id, workflow.SignalClear, nil)
}

// signalParticipant signals a participant; ErrNothingToStop when it does not
// run.
func (s *Service) signalParticipant(ctx context.Context, id, signal string, arg any) error {
	err := s.temporal.SignalWorkflow(ctx, id, "", signal, arg)
	var gone *serviceerror.NotFound
	switch {
	case errors.As(err, &gone):
		return ErrNothingToStop
	case err != nil:
		return fmt.Errorf("%s %s: %w", signal, id, err)
	}
	return nil
}

// Cancel stops the turns running in the session that the user may stop
// (MayStop): all of them for its creator, whichever turn each runs; for
// another member, those answering their messages, read now from each
// participant, all at once, each stop naming its turn. A participant that
// cannot be read is logged and left running, as if its turn were another
// member's. It stops turns, not the messages waiting.
//
// An error only when no stop went: the stops that went are what the user
// asked for, as far as could be done. ErrStopNotAllowed: turns run, none of
// them the user's (or readable). ErrNothingToStop: none runs.
func (s *Service) Cancel(ctx context.Context, sess *store.Session, me *store.User) error {
	defer s.statuses.invalidate()
	ids, err := s.participants(ctx, sess.SessionID)
	if err != nil {
		return fmt.Errorf("find the participants: %w", err)
	}
	stops := make([]*workflow.StopTurn, len(ids))
	others := 0
	var errs []error
	if sess.CreatedBy == me.ID {
		for i := range ids {
			stops[i] = &workflow.StopTurn{}
		}
	} else {
		states, qerrs := s.queryStates(ctx, ids)
		for i, st := range states {
			var gone *serviceerror.NotFound
			switch {
			case errors.As(qerrs[i], &gone), qerrs[i] == nil && st.Current == nil:
			case qerrs[i] != nil:
				log.Printf("Session %s: read the turn of %s: %v", sess.SessionID, ids[i], qerrs[i])
				errs = append(errs, fmt.Errorf("read the turn of %s: %w", ids[i], qerrs[i]))
				others++
			case MayStop(sess, st.Current.UserID, me.ID):
				stops[i] = &workflow.StopTurn{TurnKey: st.Current.Turn}
			default:
				others++
			}
		}
	}
	stopped := 0
	for i, stop := range stops {
		if stop == nil {
			continue
		}
		switch err := s.signalParticipant(ctx, ids[i], workflow.SignalStopTurn, *stop); {
		case err == nil:
			stopped++
		case !errors.Is(err, ErrNothingToStop): // ended meanwhile: nothing to stop
			errs = append(errs, err)
		}
	}
	switch {
	case stopped > 0:
		if len(errs) > 0 {
			log.Printf("Session %s: %d turns stopped, not all: %v", sess.SessionID, stopped, errors.Join(errs...))
		}
		return nil
	case len(errs) > 0:
		return errors.Join(errs...)
	case others > 0:
		return ErrStopNotAllowed
	}
	return ErrNothingToStop
}
