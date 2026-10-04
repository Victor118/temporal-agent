package session

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/victor/temporal-agent/workflow"
)

// Temporal's visibility queries are text, and a session ID goes into them
// between quotes. The routes check membership first, which a forged ID never
// passes, but that protection is indirect: the per-session queries accept a
// session ID only in its canonical UUID form, which holds no quote, whatever
// the caller checked before. No other value is put in a query: a session's
// workflows are found by its ID, followed by ':' (workflow.SessionOf).

// checkSessionID refuses anything but a canonical UUID.
func checkSessionID(sessionID string) error {
	if id, err := uuid.Parse(sessionID); err != nil || id.String() != sessionID {
		return fmt.Errorf("invalid session ID %q", sessionID)
	}
	return nil
}

// runningQuery finds the running workflows of a type in a session: theirs,
// their agents' and their sub-agents' alike, whose IDs all start with the
// session's.
func runningQuery(workflowType, sessionID string) (string, error) {
	if err := checkSessionID(sessionID); err != nil {
		return "", err
	}
	return fmt.Sprintf("WorkflowType = '%s' AND ExecutionStatus = 'Running' AND WorkflowId STARTS_WITH '%s:'", workflowType, sessionID), nil
}

// pendingQuestionsQuery finds the questions waiting in a session.
func pendingQuestionsQuery(sessionID string) (string, error) {
	return runningQuery("AskUserWorkflow", sessionID)
}

// listRunning lists the running workflows of a type in a session, oldest
// first.
func (s *Service) listRunning(ctx context.Context, workflowType, sessionID string) ([]*workflowpb.WorkflowExecutionInfo, error) {
	query, err := runningQuery(workflowType, sessionID)
	if err != nil {
		return nil, err
	}
	resp, err := s.temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.cfg.Namespace,
		Query:     query,
		PageSize:  100,
	})
	if err != nil {
		return nil, err
	}
	runs := slices.Clone(resp.Executions)
	// Ordered here: the SQL visibility stores take no ORDER BY.
	slices.SortStableFunc(runs, func(a, b *workflowpb.WorkflowExecutionInfo) int {
		return a.GetStartTime().AsTime().Compare(b.GetStartTime().AsTime())
	})
	return runs, nil
}

// participants are the workflow IDs of the session's participants running:
// those the visibility queries list, and those the turn events say work,
// which the queries may not list yet.
func (s *Service) participants(ctx context.Context, sessionID string) ([]string, error) {
	runs, err := s.listRunning(ctx, "ParticipantWorkflow", sessionID)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, r := range runs {
		ids = append(ids, r.Execution.WorkflowId)
	}
	for _, w := range s.turns.working(sessionID, nil) {
		if id := workflow.ParticipantWorkflowID(sessionID, w.Participant); !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// workflowStatus is the status of a workflow's latest run, as Temporal has
// it now; unspecified when it knows of none, or cannot tell.
func (s *Service) workflowStatus(ctx context.Context, workflowID string) enumspb.WorkflowExecutionStatus {
	return s.describeWorkflow(ctx, workflowID).status
}

// isWorkflowRunning checks if a Temporal workflow is still running.
func (s *Service) isWorkflowRunning(ctx context.Context, workflowID string) bool {
	return s.workflowStatus(ctx, workflowID) == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// describeWorkflow reads where a workflow's latest run stands.
func (s *Service) describeWorkflow(ctx context.Context, workflowID string) workflowState {
	desc, err := s.temporal.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil || desc.WorkflowExecutionInfo == nil {
		return workflowState{status: enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED}
	}
	info := desc.WorkflowExecutionInfo
	w := workflowState{status: info.Status}
	if info.CloseTime != nil {
		w.closed = info.CloseTime.AsTime()
	}
	return w
}

// shownWorkflow is where a workflow stands, for a page: read at most once per
// statusesTTL, like the session states, and dropped with them after an
// action. An action that needs the state now reads workflowStatus.
func (s *Service) shownWorkflow(ctx context.Context, workflowID string) workflowState {
	return s.statuses.workflow(ctx, workflowID, s.describeWorkflow)
}

// Cancel stops the turns running in the session: every participant at work
// is sent stop-turn, which stops its turn, not the messages it has waiting.
func (s *Service) Cancel(ctx context.Context, sessionID string) error {
	defer s.statuses.invalidate()
	ids, err := s.participants(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("find the participants: %w", err)
	}
	stopped := 0
	var errs []error
	for _, id := range ids {
		err := s.temporal.SignalWorkflow(ctx, id, "", workflow.SignalStopTurn, nil)
		var gone *serviceerror.NotFound
		switch {
		case err == nil:
			stopped++
		case !errors.As(err, &gone): // ended meanwhile: nothing to stop
			errs = append(errs, fmt.Errorf("stop %s: %w", id, err))
		}
	}
	if stopped == 0 && len(errs) == 0 {
		return ErrNothingToStop
	}
	return errors.Join(errs...)
}

// terminateParticipants ends the session's participants, and their turns
// with them: the session is gone. A participant started a moment ago may not
// be listed yet and survive: its next turn finds the session gone.
func (s *Service) terminateParticipants(ctx context.Context, sessionID, reason string) {
	ids, err := s.participants(ctx, sessionID)
	if err != nil {
		log.Printf("Session %s: find the participants to end: %v", sessionID, err)
		return
	}
	for _, id := range ids {
		if err := s.temporal.TerminateWorkflow(ctx, id, "", reason); err != nil {
			var gone *serviceerror.NotFound
			if !errors.As(err, &gone) {
				log.Printf("Session %s: end participant %s: %v", sessionID, id, err)
			}
		}
	}
}

// Answer answers a question of the session, by any of its members. Every
// workflow of a session, sub-agents' included, has an ID starting with the
// session's: a question of another session is refused, membership was checked
// for this one only.
func (s *Service) Answer(ctx context.Context, sessionID, workflowID, answer string) error {
	if strings.TrimSpace(answer) == "" {
		return ErrEmptyAnswer
	}
	if sid, ok := workflow.SessionOf(workflowID); !ok || sid != sessionID {
		return ErrForeignQuestion
	}
	defer s.statuses.invalidate()
	if err := s.temporal.SignalWorkflow(ctx, workflowID, "", workflow.SignalUserAnswer, answer); err != nil {
		return fmt.Errorf("send answer: %w", err)
	}
	return nil
}

// AnswerPending answers the oldest question waiting in a session, if there
// is one, and reports whether there was: a channel without buttons
// (Telegram) takes the next message as the answer. The question may come
// from any participant, or a sub-agent of one: the query finds them by
// type.
func (s *Service) AnswerPending(ctx context.Context, sessionID, answer string) bool {
	runs, err := s.listRunning(ctx, "AskUserWorkflow", sessionID)
	if err != nil || len(runs) == 0 {
		return false
	}

	defer s.statuses.invalidate()
	askWfID := runs[0].Execution.WorkflowId
	if err := s.temporal.SignalWorkflow(ctx, askWfID, "", workflow.SignalUserAnswer, answer); err != nil {
		log.Printf("Session %s: failed to signal ask_user workflow %s: %v", sessionID, askWfID, err)
		return false
	}
	log.Printf("Session %s: routed answer to ask_user workflow %s", sessionID, askWfID)
	return true
}

// Question is a question an agent of the session waits on.
type Question struct {
	WorkflowID string
	Text       string
	AgentChain []string
}

// PendingQuestions returns the questions the session's agents wait on,
// oldest first.
func (s *Service) PendingQuestions(ctx context.Context, sessionID string) []Question {
	runs, err := s.listRunning(ctx, "AskUserWorkflow", sessionID)
	if err != nil {
		if checkSessionID(sessionID) == nil {
			log.Printf("Session %s: list questions: %v", sessionID, err)
		}
		return nil
	}
	var out []Question
	for _, e := range runs {
		id := e.Execution.WorkflowId
		v, err := s.temporal.QueryWorkflow(ctx, id, "", workflow.QueryQuestion)
		if err != nil {
			log.Printf("Session %s: question %s: %v", sessionID, id, err)
			continue
		}
		var q workflow.PendingQuestion
		if err := v.Get(&q); err != nil {
			continue
		}
		out = append(out, Question{WorkflowID: id, Text: q.Question, AgentChain: q.AgentChain})
	}
	return out
}

// ParticipantState is where a participant of the session stands, as its
// state query answers.
type ParticipantState struct {
	AgentID    string `json:"agent_id"`
	WorkflowID string `json:"workflow_id"`
	workflow.ParticipantState
}

// State is where the session's participants stand: each one running, as it
// answers its state query. A participant that ended meanwhile is left out.
func (s *Service) State(ctx context.Context, sessionID string) ([]ParticipantState, error) {
	ids, err := s.participants(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("find the participants: %w", err)
	}
	out := []ParticipantState{}
	for _, id := range ids {
		resp, err := s.temporal.QueryWorkflow(ctx, id, "", workflow.QueryState)
		if err != nil {
			log.Printf("Session %s: state of %s: %v", sessionID, id, err)
			continue
		}
		st := ParticipantState{WorkflowID: id}
		st.AgentID, _ = workflow.ParticipantOf(id)
		if err := resp.Get(&st.ParticipantState); err != nil {
			return nil, fmt.Errorf("decode the state of %s: %w", id, err)
		}
		out = append(out, st)
	}
	slices.SortFunc(out, func(a, b ParticipantState) int { return cmp.Compare(a.AgentID, b.AgentID) })
	return out, nil
}
