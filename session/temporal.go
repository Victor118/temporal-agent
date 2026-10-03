package session

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"github.com/victor/temporal-agent/workflow"
)

// Temporal's visibility queries are text, and a session ID goes into them
// between quotes. The routes check membership first, which a forged ID never
// passes, but that protection is indirect: the per-session queries accept a
// session ID only in its canonical UUID form, which holds no quote, whatever
// the caller checked before.

// checkSessionID refuses anything but a canonical UUID.
func checkSessionID(sessionID string) error {
	if id, err := uuid.Parse(sessionID); err != nil || id.String() != sessionID {
		return fmt.Errorf("invalid session ID %q", sessionID)
	}
	return nil
}

// sessionWorkflowID is the ID of every run of a session's workflow: opened,
// resumed after it timed out, or continued as new.
func sessionWorkflowID(sessionID string) string { return "session-" + sessionID }

// runningSessionQuery finds the session's own workflow, on its fixed ID or
// resumed under the former scheme ("session-<id>-<unix time>").
func runningSessionQuery(sessionID string) (string, error) {
	if err := checkSessionID(sessionID); err != nil {
		return "", err
	}
	return fmt.Sprintf("WorkflowId STARTS_WITH 'session-%s' AND ExecutionStatus = 'Running'", sessionID), nil
}

// legacyRunQuery finds a run of the session resumed under the former scheme,
// "session-<id>-<unix time>", which ends after 30 minutes idle.
func legacyRunQuery(sessionID string) (string, error) {
	if err := checkSessionID(sessionID); err != nil {
		return "", err
	}
	return fmt.Sprintf("WorkflowType = 'SessionWorkflow' AND ExecutionStatus = 'Running' AND WorkflowId STARTS_WITH 'session-%s-'", sessionID), nil
}

// pendingQuestionsQuery finds the questions waiting in a session, its agent's
// and its sub-agents' alike: their IDs all start with the session's.
func pendingQuestionsQuery(sessionID string) (string, error) {
	if err := checkSessionID(sessionID); err != nil {
		return "", err
	}
	return fmt.Sprintf("WorkflowType = 'AskUserWorkflow' AND ExecutionStatus = 'Running' AND WorkflowId STARTS_WITH '%s-'", sessionID), nil
}

// isWorkflowRunning checks if a Temporal workflow is still running.
func (s *Service) isWorkflowRunning(ctx context.Context, workflowID string) bool {
	desc, err := s.temporal.DescribeWorkflowExecution(ctx, workflowID, "")
	if err != nil {
		return false
	}
	return desc.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// activeWorkflowID returns the ID of the session's running workflow, or "".
// Handles both "session-{id}" and "session-{id}-{timestamp}" workflow IDs.
func (s *Service) activeWorkflowID(ctx context.Context, sessionID string) string {
	base := sessionWorkflowID(sessionID)
	if s.isWorkflowRunning(ctx, base) {
		return base
	}
	query, err := runningSessionQuery(sessionID)
	if err != nil {
		return ""
	}
	resp, err := s.temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.cfg.Namespace,
		Query:     query,
		PageSize:  1,
	})
	if err != nil || len(resp.Executions) == 0 {
		return ""
	}
	return resp.Executions[0].Execution.WorkflowId
}

// legacyRunID returns the session's run resumed under the former scheme if it
// still runs, or "": until it times out, a message goes to it rather than
// starting a second run on the fixed ID. Only the runs started before the
// fixed ID was adopted have such an ID. A visibility error is returned, not
// taken for "none": a run may be there, and starting on the fixed ID would
// make a second one.
//
// The fallback (legacyRunQuery, legacyRunID, its branch in signalSession) can
// go once no "session-<id>-<unix>" run can still be running: 30 minutes idle
// after the fixed ID was deployed.
func (s *Service) legacyRunID(ctx context.Context, sessionID string) (string, error) {
	query, err := legacyRunQuery(sessionID)
	if err != nil {
		// Not a UUID: no run was ever resumed under it, every session
		// created had a UUID, before the fixed ID as after.
		return "", nil
	}
	resp, err := s.temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.cfg.Namespace,
		Query:     query,
		PageSize:  1,
	})
	if err != nil {
		return "", fmt.Errorf("list legacy runs: %w", err)
	}
	if len(resp.Executions) == 0 {
		return "", nil
	}
	return resp.Executions[0].Execution.WorkflowId, nil
}

// signalIfRunning signals a workflow and reports whether it took the signal:
// one that has ended is no error, only not there. Any other failure is
// returned, the signal possibly not sent.
func (s *Service) signalIfRunning(ctx context.Context, workflowID, signal string, arg any) (bool, error) {
	err := s.temporal.SignalWorkflow(ctx, workflowID, "", signal, arg)
	var gone *serviceerror.NotFound
	if errors.As(err, &gone) {
		return false, nil
	}
	return err == nil, err
}

// IsActive reports whether the session's workflow runs.
func (s *Service) IsActive(ctx context.Context, sessionID string) bool {
	return s.activeWorkflowID(ctx, sessionID) != ""
}

// Cancel interrupts the agent's turn, if one runs.
func (s *Service) Cancel(ctx context.Context, sessionID string) error {
	defer s.statuses.invalidate()
	workflowID := s.activeWorkflowID(ctx, sessionID)
	if workflowID == "" {
		return ErrNoActiveSession
	}
	if err := s.temporal.SignalWorkflow(ctx, workflowID, "", workflow.SignalCancelAgent, nil); err != nil {
		return fmt.Errorf("cancel agent: %w", err)
	}
	return nil
}

// Answer answers a question of the session, by any of its members. Every
// workflow of a session, sub-agents' included, has an ID starting with the
// session's: a question of another session is refused, membership was checked
// for this one only.
func (s *Service) Answer(ctx context.Context, sessionID, workflowID, answer string) error {
	if strings.TrimSpace(answer) == "" {
		return ErrEmptyAnswer
	}
	if !strings.HasPrefix(workflowID, sessionID+"-") {
		return ErrForeignQuestion
	}
	defer s.statuses.invalidate()
	if err := s.temporal.SignalWorkflow(ctx, workflowID, "", workflow.SignalUserAnswer, answer); err != nil {
		return fmt.Errorf("send answer: %w", err)
	}
	return nil
}

// AnswerPending answers the question waiting in a session, if there is one,
// and reports whether there was: a channel without buttons (Telegram) takes
// the next message as the answer. The question may come from a sub-agent,
// whose ask_user is "<session>-tool-agent_x-<call>-tool-ask_user-…": the
// query finds them by type.
func (s *Service) AnswerPending(ctx context.Context, sessionID, answer string) bool {
	query, err := pendingQuestionsQuery(sessionID)
	if err != nil {
		return false
	}
	resp, err := s.temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.cfg.Namespace,
		Query:     query,
		PageSize:  1,
	})
	if err != nil || len(resp.Executions) == 0 {
		return false
	}

	defer s.statuses.invalidate()
	askWfID := resp.Executions[0].Execution.WorkflowId
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

// PendingQuestions returns the questions the session's agents wait on.
func (s *Service) PendingQuestions(ctx context.Context, sessionID string) []Question {
	query, err := pendingQuestionsQuery(sessionID)
	if err != nil {
		return nil
	}
	resp, err := s.temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.cfg.Namespace,
		Query:     query,
		PageSize:  50,
	})
	if err != nil {
		log.Printf("Session %s: list questions: %v", sessionID, err)
		return nil
	}
	var out []Question
	for _, e := range resp.Executions {
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

// State queries the state of the session's running workflow, or of its last
// run, which still answers once completed.
func (s *Service) State(ctx context.Context, sessionID string) (workflow.SessionState, error) {
	var state workflow.SessionState
	id := s.activeWorkflowID(ctx, sessionID)
	if id == "" {
		id = sessionWorkflowID(sessionID)
	}
	resp, err := s.temporal.QueryWorkflow(ctx, id, "", workflow.QuerySessionState)
	if err != nil {
		return state, fmt.Errorf("query state: %w", err)
	}
	if err := resp.Get(&state); err != nil {
		return state, fmt.Errorf("decode state: %w", err)
	}
	return state, nil
}
