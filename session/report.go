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

// ReportState is where a fork stands with its reports to its parent, for one
// of its members.
type ReportState struct {
	// Refused says why this member cannot report at all (ErrNoParent,
	// ErrNotParentMember); nil when they can.
	Refused error
	// Why the report cannot be sent now: the fork's summary is still being
	// written, a report is, or there is nothing new since the last one.
	SummaryPending bool
	Pending        bool
	NothingNew     bool
	// Failed: the last attempt at this report failed. Another may be made.
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

// ReportState tells where a fork stands with its reports, for me. Not a fork:
// ErrNotAFork.
func (s *Service) ReportState(ctx context.Context, fork *store.Session, me *store.User) (ReportState, error) {
	st := ReportState{ParentSessionID: fork.ParentSessionID, LastReportID: fork.LastReportID, LastReportedAt: fork.LastReportedAt}
	switch err := s.reportRefusal(ctx, fork, me); {
	case errors.Is(err, ErrNoParent), errors.Is(err, ErrNotParentMember):
		st.Refused = err
		return st, nil
	case err != nil:
		return st, err
	}
	msgs, err := s.store.LoadMessagesUpTo(ctx, fork.SessionID, 0)
	if err != nil {
		return st, err
	}
	st.SummaryPending = s.summaryPending(ctx, fork, msgs)
	_, _, ok := reportRange(fork, msgs)
	st.NothingNew = !ok
	switch s.workflowStatus(ctx, workflow.ReportWorkflowID(fork.SessionID, fork.LastReportedMessageID)) {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		st.Pending = true
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		st.Failed = true
	}
	return st, nil
}

// ReportToParent starts the report of a fork to its parent, sent by me: a
// summary of the fork's messages since its last report, posted into the parent
// as my message. Only a member of both may (the routes check the fork, this
// the parent); not while the fork's summary is written, nor with nothing new.
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
	msgs, err := s.store.LoadMessagesUpTo(ctx, fork.SessionID, 0)
	if err != nil {
		return err
	}
	if s.summaryPending(ctx, fork, msgs) {
		return ErrSummaryPending
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

// summaryPending reports whether the fork's summary, its first message, is
// still being written.
func (s *Service) summaryPending(ctx context.Context, fork *store.Session, msgs []store.MessageWithID) bool {
	if len(msgs) > 0 && msgs[0].Kind == store.KindForkSummary {
		return false
	}
	return s.ForkRunning(ctx, fork.SessionID)
}

// reportRange is the range of the fork's next report: after its last reported
// message, up to its last message. False when there is nothing in it to
// report: no message, or only the summary and failed turns.
func reportRange(fork *store.Session, msgs []store.MessageWithID) (from, upTo int64, ok bool) {
	from = fork.LastReportedMessageID
	for _, m := range msgs {
		upTo = max(upTo, m.ID)
		if m.ID > from && m.Kind != store.KindForkSummary && m.Kind != store.KindTurnError {
			ok = true
		}
	}
	return from, upTo, ok
}
