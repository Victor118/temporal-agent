package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// The background tasks of a session's agents (docs/design/async-tasks.md):
// their rows say what runs, for whom, since when; Temporal runs them. The
// server shows them in the Agents panel, stops them, ends them with their
// session, and ends those whose workflow closed without saying how (the
// sweep).

var (
	// ErrNoSuchTask: no such background task in this session.
	ErrNoSuchTask = errors.New("no such background task in this session")
	// ErrTaskOver: the background task has ended.
	ErrTaskOver = errors.New("the background task has ended")
)

// Task is a background task running, as the Agents panel shows it under
// its agent.
type Task struct {
	ID      string
	Tool    string
	Summary string
	// UserID and UserName asked for it: they may stop it, with the
	// session's creator.
	UserID   string
	UserName string
	Since    time.Time
	// Note is what it waits for (a worker, a machine), as its last notice
	// said; Waiting, a question of it waits for a member's answer.
	Note    string
	Waiting bool
}

// tasksOf are the background tasks running in a session, by participant,
// as the panel shows them: their note and their question.
func (s *Service) tasksOf(ctx context.Context, sessionID string, v *visible) map[string][]Task {
	rows, err := s.store.ListRunningTasks(ctx, sessionID, "")
	if err != nil {
		log.Printf("Session %s: list the background tasks: %v", sessionID, err)
		return nil
	}
	asking := v.taskAskingIn(sessionID)
	out := map[string][]Task{}
	for _, t := range rows {
		out[t.Participant] = append(out[t.Participant], Task{
			ID: t.ID, Tool: t.Tool, Summary: t.Summary, UserID: t.UserID, UserName: t.UserName, Since: t.StartedAt,
			Note: s.turns.taskNote(sessionID, t.ID), Waiting: slices.Contains(asking, t.ID),
		})
	}
	return out
}

// StopTask stops a background task of the session: the user who asked for
// it may, and the session's creator (MayStop). Its workflow is cancelled,
// which cancels its tool, and posts a message saying who stopped it, which
// wakes nobody.
//
// ErrNoSuchTask: not one of the session's. ErrTaskOver: it ended.
// ErrStopNotAllowed: another member's, and the user did not create the
// session.
func (s *Service) StopTask(ctx context.Context, sess *store.Session, taskID string, me *store.User) error {
	t, err := s.store.GetTask(ctx, taskID)
	switch {
	case err != nil:
		return fmt.Errorf("read the task: %w", err)
	case t == nil || t.SessionID != sess.SessionID:
		return ErrNoSuchTask
	case t.State != store.BackgroundRunning:
		return ErrTaskOver
	case !MayStop(sess, t.UserID, me.ID):
		return ErrStopNotAllowed
	}
	defer s.statuses.invalidate()
	if err := s.store.SetTaskCancelledBy(ctx, taskID, me.Name()); err != nil {
		if errors.Is(err, store.ErrTaskOver) || errors.Is(err, store.ErrTaskNotFound) {
			return ErrTaskOver
		}
		return fmt.Errorf("record who stops the task: %w", err)
	}
	err = s.temporal.CancelWorkflow(ctx, taskID, "")
	var gone *serviceerror.NotFound
	switch {
	case errors.As(err, &gone):
		return ErrTaskOver // closed: the sweep ends its row
	case err != nil:
		return fmt.Errorf("cancel the task: %w", err)
	}
	log.Printf("Session %s: background task %s stopped by %s", sess.SessionID, taskID, me.ID)
	return nil
}

// terminateTasks ends the session's background tasks, their tools with
// them (a coding run frees its machine): the session goes. Those the
// visibility lists, and those its store says run: the visibility lags.
// Called before the session is deleted, as for its participants: a task
// ending meanwhile may still wake a participant, whose next turn finds the
// session gone.
func (s *Service) terminateTasks(ctx context.Context, sessionID, reason string) {
	var ids []string
	if runs, err := s.listRunning(ctx, "BackgroundTaskWorkflow", sessionID); err != nil {
		log.Printf("Session %s: find the background tasks to end: %v", sessionID, err)
	} else {
		for _, r := range runs {
			ids = append(ids, r.Execution.WorkflowId)
		}
	}
	if rows, err := s.store.ListRunningTasks(ctx, sessionID, ""); err != nil {
		log.Printf("Session %s: list the background tasks to end: %v", sessionID, err)
	} else {
		for _, t := range rows {
			if !slices.Contains(ids, t.ID) {
				ids = append(ids, t.ID)
			}
		}
	}
	for _, id := range ids {
		if err := s.temporal.TerminateWorkflow(ctx, id, "", reason); err != nil {
			var gone *serviceerror.NotFound
			if !errors.As(err, &gone) {
				log.Printf("Session %s: end background task %s: %v", sessionID, id, err)
			}
		}
	}
}

// Sweep timing: a task the sweep looks at has run this long at least, so
// that a workflow just started, which the visibility may not list yet, is
// left alone.
const (
	taskSweepEvery = time.Minute
	taskSweepAge   = 5 * time.Minute
	taskSweepLoad  = 30 * time.Second
)

// sweptReason is the end of a task whose workflow closed without saying
// how it ended.
const sweptReason = "The task stopped without a result: its workflow ended without posting one (terminated, out of time, or its end could not be written)."

// RunTaskSweep sweeps the background tasks every minute until ctx ends.
func (s *Service) RunTaskSweep(ctx context.Context) {
	tick := time.NewTicker(taskSweepEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			sctx, cancel := context.WithTimeout(ctx, taskSweepLoad)
			s.SweepTasks(sctx)
			cancel()
		}
	}
}

// SweepTasks ends the background tasks whose workflow closed without
// ending them: terminated by an admin, out of time, its end not written.
// Without it, such a task would count for ever against its participant's
// cap, and in its prompt. The tasks running for taskSweepAge at least, in
// the store, are crossed with the task workflows the visibility lists
// running; one it does not list is described, and ended if closed or
// unknown: failed, its message posted, its participant woken. A task its
// workflow ends meanwhile is not ended twice (store.EndTask: the first
// writer wins).
func (s *Service) SweepTasks(ctx context.Context) {
	rows, err := s.store.ListTasksRunningSince(ctx, time.Now().Add(-taskSweepAge))
	if err != nil {
		log.Printf("Background tasks sweep: list: %v", err)
		return
	}
	if len(rows) == 0 {
		return
	}
	resp, err := s.temporal.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.cfg.Namespace,
		Query:     "WorkflowType = 'BackgroundTaskWorkflow' AND ExecutionStatus = 'Running'",
		PageSize:  1000,
	})
	if err != nil {
		log.Printf("Background tasks sweep: list the running: %v", err)
		return
	}
	running := map[string]bool{}
	for _, e := range resp.Executions {
		running[e.Execution.WorkflowId] = true
	}
	for _, t := range rows {
		if running[t.ID] || !s.taskClosed(ctx, t.ID) {
			continue
		}
		s.endSwept(ctx, t)
	}
}

// taskClosed reports whether a task's workflow is closed, or unknown to
// Temporal: Temporal said so. In doubt (it does not answer), false.
func (s *Service) taskClosed(ctx context.Context, id string) bool {
	desc, err := s.temporal.DescribeWorkflowExecution(ctx, id, "")
	var gone *serviceerror.NotFound
	switch {
	case errors.As(err, &gone):
		return true
	case err != nil || desc.WorkflowExecutionInfo == nil:
		if err != nil {
			log.Printf("Background tasks sweep: describe %s: %v", id, err)
		}
		return false
	}
	return desc.WorkflowExecutionInfo.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// endSwept ends a task the sweep found closed, and wakes its participant,
// unless its workflow ended it meanwhile.
func (s *Service) endSwept(ctx context.Context, t store.BackgroundTask) {
	end, err := s.store.EndTask(ctx, t.ID, store.TaskEndedBySweep, store.BackgroundFailed, func(t store.BackgroundTask) store.Message {
		return activity.TaskResultMessage(t, sweptReason, nil)
	})
	switch {
	case err != nil:
		log.Printf("Background tasks sweep: end %s: %v", t.ID, err)
		return
	case end.Gone || !end.Mine:
		return
	}
	log.Printf("Background tasks sweep: task %s closed without ending: ended as failed", t.ID)
	data, _ := json.Marshal(map[string]string{"type": workflow.EventTaskResult, "task": t.ID, "agent_id": t.Participant, "message_id": fmt.Sprint(end.MessageID)})
	s.hub.Publish(t.SessionID, activity.SSEEvent{Type: workflow.EventTaskResult, Data: data})
	sess, err := s.store.GetSession(ctx, t.SessionID)
	if err != nil || sess == nil {
		return
	}
	msg := workflow.ParticipantMessage{
		MessageID: end.MessageID, UserID: t.UserID, UserName: t.UserName,
		SignReply: activity.TaskSignsReply(*sess, t.Participant), Channel: t.Channel, ChannelID: t.ChannelID,
	}
	id := workflow.ParticipantWorkflowID(t.SessionID, t.Participant)
	if _, err := s.temporal.SignalWithStartWorkflow(ctx, id, workflow.SignalMessage, msg, client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: s.cfg.WorkflowQueue,
	}, workflow.ParticipantWorkflow, workflow.ParticipantInput{
		SessionID: t.SessionID, AgentID: t.Participant, Channel: sess.Channel, ChannelID: sess.ChannelID,
	}); err != nil {
		// Its message gets an end, so that no late delivery answers it.
		log.Printf("Background tasks sweep: wake %s for %s: %v", id, t.ID, err)
		reason := fmt.Sprintf("la fin de la tâche n'a pas pu réveiller @%s : %v", t.Participant, err)
		if _, err := s.store.AppendMessage(ctx, t.SessionID, store.TurnEndKey(store.TurnKey(end.MessageID, t.Participant)), store.TurnEnd(t.Participant, reason)); err != nil {
			log.Printf("Background tasks sweep: end the turn of %s: %v", t.ID, err)
		}
	}
}
