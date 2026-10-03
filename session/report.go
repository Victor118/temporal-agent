package session

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// Why a member cannot report a fork to its parent.
var (
	ErrNotAFork        = errors.New("this session is not a fork")
	ErrNoParent        = errors.New("the fork's parent session was deleted")
	ErrNotParentMember = errors.New("only a member of the parent session can report to it")
	ErrNothingToReport = errors.New("nothing new to report since the last report")
)

// reportFailureShown is how long a failed report is shown as such. A click
// tries it again (and clears it); without one, it does not stay forever.
const reportFailureShown = 15 * time.Minute

// ReportState is where a fork stands with its reports to its parent, for one
// of its members.
type ReportState struct {
	// Refused says why this member cannot report at all (ErrNoParent,
	// ErrNotParentMember); nil when they can.
	Refused error
	// Why the report cannot be sent now: the fork's summary is still being
	// written, a report is being written, or there is nothing new since the
	// last one.
	SummaryPending bool
	Pending        bool
	NothingNew     bool
	// AgentWorking: the fork's agent is on a turn, running or waiting for an
	// answer. A report may still be sent: it covers what is written so far,
	// and the rest goes into the next one.
	AgentWorking bool
	// Failed: the last attempt at this report failed, less than
	// reportFailureShown ago. Another may be made.
	Failed bool

	ParentSessionID string
	// The latest report: its message in the parent, and when; zero for none.
	LastReportID   int64
	LastReportedAt *time.Time
}

// CanReport reports whether the member can send a report now.
func (r ReportState) CanReport() bool {
	return r.Refused == nil && !r.SummaryPending && !r.Pending && !r.NothingNew
}

// ReportState tells where a fork stands with its reports, for me. msgs are
// the fork's messages, all of them, as the caller loaded them: the range of
// the next report is read from them. Not a fork:
// ErrNotAFork. The workflows' states are read as pages show them, a few
// seconds old at most (shownWorkflow, Statuses).
func (s *Service) ReportState(ctx context.Context, fork *store.Session, msgs []store.MessageWithID, me *store.User) (ReportState, error) {
	st := ReportState{ParentSessionID: fork.ParentSessionID, LastReportID: fork.LastReportID, LastReportedAt: fork.LastReportedAt}
	switch err := s.reportRefusal(ctx, fork, me); {
	case errors.Is(err, ErrNoParent), errors.Is(err, ErrNotParentMember):
		st.Refused = err
		return st, nil
	case err != nil:
		return st, err
	}
	st.SummaryPending = s.ForkSummaryState(ctx, fork) == SummaryPending
	st.AgentWorking = s.agentWorking(ctx, fork.SessionID)
	_, _, ok := reportRange(fork, msgs)
	st.NothingNew = !ok
	w := s.shownWorkflow(ctx, workflow.ReportWorkflowID(fork.SessionID, fork.LastReportedMessageID))
	switch w.status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		st.Pending = true
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		st.Failed = s.statuses.clock().Sub(w.closed) < reportFailureShown
	}
	return st, nil
}

// ReportToParent starts the report of a fork to its parent, sent by me: a
// summary of the fork's messages since its last report, posted into the parent
// as my message. Only a member of both may (the routes check the fork, this
// the parent); not while the fork's summary is written, nor with nothing new.
//
// While the fork's agent is on a turn, the report covers what the turn wrote
// so far (reportRange), and the next report starts after it: a turn suspended
// on a question (ask_user) as well as one running.
//
// The workflow's ID is the report's (workflow.ReportWorkflowID): while it
// runs, a second call starts nothing; once it failed, a new call tries again.
func (s *Service) ReportToParent(ctx context.Context, forkID string, me *store.User) error {
	fork, err := s.Get(ctx, forkID)
	if err != nil {
		return err
	}
	if err := s.reportRefusal(ctx, fork, me); err != nil {
		return err
	}
	if s.ForkSummaryState(ctx, fork) == SummaryPending {
		return ErrSummaryPending
	}
	msgs, err := s.store.LoadMessagesUpTo(ctx, fork.SessionID, 0)
	if err != nil {
		return err
	}
	from, upTo, ok := reportRange(fork, msgs)
	if !ok {
		return ErrNothingToReport
	}

	defer s.statuses.invalidate()
	if _, err := s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        workflow.ReportWorkflowID(fork.SessionID, from),
		TaskQueue: s.cfg.WorkflowQueue,
		// A report posted moves the fork's last reported message, and the
		// next report gets another ID: only a failed one is tried again.
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}, workflow.ReportToParentWorkflow, workflow.ReportToParentInput{
		ForkSessionID:   fork.SessionID,
		ParentSessionID: fork.ParentSessionID,
		ForkTitle:       cmp.Or(fork.Title, "Fork"),
		Purpose:         fork.ForkPurpose,
		From:            from,
		UpTo:            upTo,
		ReporterID:      me.ID,
		ReporterName:    me.Name(),
		Model:           s.cfg.SummaryModel,
	}); err != nil {
		return fmt.Errorf("start the report: %w", err)
	}
	log.Printf("Session %s: %s reports messages %d to %d to %s", fork.SessionID, me.ID, from+1, upTo, fork.ParentSessionID)
	return nil
}

// reportRefusal says why me cannot report fork to its parent at all: it is
// not a fork (ErrNotAFork), its parent is gone (ErrNoParent), or me is no
// member of it (ErrNotParentMember). Nil: they can.
func (s *Service) reportRefusal(ctx context.Context, fork *store.Session, me *store.User) error {
	switch {
	case fork.ForkedAtMessageID == 0:
		return ErrNotAFork
	case fork.ParentSessionID == "":
		return ErrNoParent
	}
	member, err := s.store.IsSessionMember(ctx, fork.ParentSessionID, me.ID)
	if err != nil {
		return err
	}
	if !member {
		return ErrNotParentMember
	}
	return nil
}

// agentWorking reports whether the fork's agent is on a turn, running or
// waiting for a member's answer (ask_user), as the pages show it (the session
// states are a few seconds old at most): the member is told a report would
// stop where the turn stands. The workflow writing the fork's summary shows
// as working too: it is not a turn.
func (s *Service) agentWorking(ctx context.Context, forkID string) bool {
	switch s.Statuses(ctx)[forkID] {
	case StatusWaiting:
		return true
	case StatusWorking:
		return !s.ForkRunning(ctx, forkID)
	}
	return false
}

// reportRange is the range of the fork's next report: after its last reported
// message, up to its last message. While a turn runs, a report covers what it
// wrote so far. It stops before a call of the latest turn still waiting for
// its results (pendingCall): the turns store a call with its results, in one
// write, so this does not happen today, and the cut guards that invariant: a
// report ending between a call and its result would leave the result to the
// next one. False when nothing in the range is reportable
// (activity.Reportable, the summary's own rule).
func reportRange(fork *store.Session, msgs []store.MessageWithID) (from, upTo int64, ok bool) {
	from = fork.LastReportedMessageID
	for _, m := range msgs[:pendingCall(msgs)] {
		upTo = m.ID
		if m.ID > from && activity.Reportable(m.Message) {
			ok = true
		}
	}
	return from, upTo, ok
}

// pendingCall is the index in msgs of the first assistant message of the
// latest turn with a tool call no result answers yet: the turn is running
// that call. len(msgs) when there is none. A call left unanswered by an
// earlier turn is not waited for: that turn is over.
func pendingCall(msgs []store.MessageWithID) int {
	latest := ""
	answered := map[string]bool{}
	for _, m := range msgs {
		if turn, ok := store.TurnOf(m.Key); ok {
			latest = turn
		}
		if m.ToolResult != nil {
			answered[m.ToolResult.ToolCallID] = true
		}
	}
	for i, m := range msgs {
		if turn, ok := store.TurnOf(m.Key); !ok || turn != latest {
			continue
		}
		for _, tc := range m.ToolCalls {
			if !answered[tc.ID] {
				return i
			}
		}
	}
	return len(msgs)
}
