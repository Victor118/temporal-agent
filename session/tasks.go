package session

import (
	"cmp"
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
	// ErrStopPending: the stop is recorded, its workflow not told yet:
	// the sweep tells it within a minute or so.
	ErrStopPending = errors.New("the stop is recorded; the task will be told shortly")
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
// session. ErrStopPending: recorded, but its workflow could not be told
// now; the sweep tells it.
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
	if err := s.store.SetTaskCancelledBy(ctx, taskID, cmp.Or(me.Name(), me.ID)); err != nil { // never empty: empty is no stop
		if errors.Is(err, store.ErrTaskOver) || errors.Is(err, store.ErrTaskNotFound) {
			return ErrTaskOver
		}
		return fmt.Errorf("record who stops the task: %w", err)
	}
	err = s.temporal.CancelWorkflow(ctx, taskID, "")
	var gone *serviceerror.NotFound
	switch {
	case errors.As(err, &gone):
		return ErrTaskOver // closed: the sweep ends its row, cancelled
	case err != nil:
		log.Printf("Session %s: cancel %s (the sweep sends it again): %v", sess.SessionID, taskID, err)
		return ErrStopPending
	}
	if err := s.store.SetTaskCancelSent(ctx, taskID); err != nil {
		log.Printf("Session %s: record the cancel of %s (the sweep sends it again): %v", sess.SessionID, taskID, err)
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

// Sweep timing. A task the sweep may end has run taskSweepAge at least:
// its workflow, just started, may not be listed yet. One its workflow
// Temporal knows not is ended only past taskStartGrace: the turn records
// it before starting the workflow, and that start may wait for a worker.
// An ended task's wake is the sweep's again past taskWakeGrace (its own
// workflow wakes it at once, and retries a minute), and given up past
// taskWakeGiveUp, its turn ended saying so.
const (
	taskSweepEvery = time.Minute
	taskSweepAge   = 5 * time.Minute
	taskStartGrace = 15 * time.Minute
	taskWakeGrace  = 5 * time.Minute
	taskWakeGiveUp = 30 * time.Minute
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

// SweepTasks catches up with what the background tasks left undone:
//   - a stop recorded whose workflow was not told (CancelWorkflow failed):
//     told again;
//   - a task whose workflow closed without ending it (terminated by an
//     admin, out of time, its end not written): ended, failed (cancelled
//     if a member stopped it), its message posted, its participant woken
//     unless cancelled. Without it, such a task would count for ever
//     against its participant's cap, and in its prompt. The tasks running
//     for taskSweepAge at least, in the store, are crossed with the task
//     workflows the visibility lists running; one it does not list is
//     described, and ended if closed, or unknown to Temporal past
//     taskStartGrace. A task its workflow ends meanwhile is not ended
//     twice (store.EndTask: the first writer wins);
//   - an ended task whose participant was never woken (the waker stopped
//     between the end and the signal): woken again, CheckTurn
//     deduplicating.
func (s *Service) SweepTasks(ctx context.Context) {
	s.resendCancels(ctx)
	s.endClosedTasks(ctx)
	late, err := s.store.ListTasksToWake(ctx, time.Now().Add(-taskWakeGrace))
	if err != nil {
		log.Printf("Background tasks sweep: list those to wake: %v", err)
		return
	}
	for _, t := range late {
		s.wakeTask(ctx, t)
	}
}

// resendCancels tells again the workflows of the tasks a member stopped
// that were not told.
func (s *Service) resendCancels(ctx context.Context) {
	stopped, err := s.store.ListTasksToCancel(ctx)
	if err != nil {
		log.Printf("Background tasks sweep: list those stopped: %v", err)
		return
	}
	for _, t := range stopped {
		err := s.temporal.CancelWorkflow(ctx, t.ID, "")
		var gone *serviceerror.NotFound
		switch {
		case errors.As(err, &gone): // closed: ended below, cancelled
		case err != nil:
			log.Printf("Background tasks sweep: cancel %s: %v", t.ID, err)
		default:
			if err := s.store.SetTaskCancelSent(ctx, t.ID); err != nil {
				log.Printf("Background tasks sweep: record the cancel of %s: %v", t.ID, err)
			}
		}
	}
}

// endClosedTasks ends the tasks whose workflow closed without ending them.
func (s *Service) endClosedTasks(ctx context.Context) {
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
		if running[t.ID] || !s.taskClosed(ctx, t) {
			continue
		}
		s.endSwept(ctx, t)
	}
}

// taskClosed reports whether a task's workflow is closed: Temporal says so,
// or knows it not and the task was recorded long enough ago for its start
// to have happened. In doubt (Temporal does not answer), false.
func (s *Service) taskClosed(ctx context.Context, t store.BackgroundTask) bool {
	desc, err := s.temporal.DescribeWorkflowExecution(ctx, t.ID, "")
	var gone *serviceerror.NotFound
	switch {
	case errors.As(err, &gone):
		return time.Since(t.StartedAt) > taskStartGrace
	case err != nil || desc.WorkflowExecutionInfo == nil:
		if err != nil {
			log.Printf("Background tasks sweep: describe %s: %v", t.ID, err)
		}
		return false
	}
	return desc.WorkflowExecutionInfo.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
}

// endSwept ends a task the sweep found closed, and wakes its participant,
// unless a member stopped it (it ends cancelled, waking nobody) or its
// workflow ended it meanwhile.
func (s *Service) endSwept(ctx context.Context, t store.BackgroundTask) {
	state, content := store.BackgroundFailed, sweptReason
	if t.CancelledBy != "" {
		state, content = store.BackgroundCancelled, ""
	}
	end, err := s.store.EndTask(ctx, t.ID, store.TaskEndedBySweep, state, func(t store.BackgroundTask) store.Message {
		return activity.TaskResultMessage(t, content, nil)
	})
	switch {
	case err != nil:
		log.Printf("Background tasks sweep: end %s: %v", t.ID, err)
		return
	case end.Gone || !end.Mine:
		return
	}
	log.Printf("Background tasks sweep: task %s closed without ending: ended %s", t.ID, state)
	data, _ := json.Marshal(map[string]string{"type": workflow.EventTaskResult, "task": t.ID, "agent_id": t.Participant, "message_id": fmt.Sprint(end.MessageID)})
	s.hub.Publish(t.SessionID, activity.SSEEvent{Type: workflow.EventTaskResult, Data: data})
	if state != store.BackgroundCancelled {
		s.wakeTask(ctx, end.Task)
	}
}

// wakeTask delivers an ended task's message to its participant, as its
// workflow does: the session read first (gone: nobody to wake), a
// SignalWithStart, its wake then recorded. A failure is tried again at the
// next sweep, and given up past taskWakeGiveUp: its message gets an end
// under the turn that would have answered it, so that no late delivery
// answers it, and the launching turn's channel is told.
func (s *Service) wakeTask(ctx context.Context, t store.BackgroundTask) {
	sess, err := s.store.GetSession(ctx, t.SessionID)
	if err != nil || sess == nil {
		if err != nil {
			log.Printf("Background tasks sweep: read the session of %s: %v", t.ID, err)
		}
		return
	}
	sign := activity.TaskSignsReply(*sess, t.Participant)
	msg := workflow.ParticipantMessage{
		MessageID: t.ResultMessageID, UserID: t.UserID, UserName: t.UserName,
		SignReply: sign, Channel: t.Channel, ChannelID: t.ChannelID,
	}
	id := workflow.ParticipantWorkflowID(t.SessionID, t.Participant)
	_, err = s.temporal.SignalWithStartWorkflow(ctx, id, workflow.SignalMessage, msg, client.StartWorkflowOptions{
		ID:        id,
		TaskQueue: s.cfg.WorkflowQueue,
	}, workflow.ParticipantWorkflow, workflow.ParticipantInput{
		SessionID: t.SessionID, AgentID: t.Participant, Channel: sess.Channel, ChannelID: sess.ChannelID,
	})
	if err != nil {
		log.Printf("Background tasks sweep: wake %s for %s: %v", id, t.ID, err)
		if t.EndedAt == nil || time.Since(*t.EndedAt) < taskWakeGiveUp {
			return // the next sweep tries again
		}
		reason := fmt.Sprintf("la fin de la tâche n'a pas pu réveiller @%s : %v", t.Participant, err)
		if _, err := s.store.AppendMessage(ctx, t.SessionID, store.TurnEndKey(store.TurnKey(t.ResultMessageID, t.Participant)), store.TurnEnd(t.Participant, reason)); err != nil {
			log.Printf("Background tasks sweep: end the turn of %s: %v", t.ID, err)
			return
		}
		signer := ""
		if sign {
			signer = t.Participant
		}
		s.tell(ctx, t.SessionID, t.Channel, t.ChannelID, signer, "Error processing message: "+reason)
	}
	if err := s.store.SetTaskWoken(ctx, t.ID); err != nil {
		log.Printf("Background tasks sweep: record the wake of %s: %v", t.ID, err)
	}
}

// tell sends an agent's word to a session's channel, as a turn's answer
// goes (activity.EventMessage), by the channel's notifier
// (Config.Channels); the web by the hub when it has none.
func (s *Service) tell(ctx context.Context, sessionID, channel, channelID, signer, text string) {
	payload := map[string]string{"type": activity.EventMessage, "content": text}
	if signer != "" {
		payload["agent"] = signer
	}
	data, _ := json.Marshal(payload)
	ev := activity.SSEEvent{Type: activity.EventMessage, Data: data}
	n, ok := s.cfg.Channels[cmp.Or(channel, activity.ChannelWeb)]
	if !ok {
		if channel != "" && channel != activity.ChannelWeb {
			log.Printf("Session %s: no notifier for channel %s", sessionID, channel)
			return
		}
		s.hub.Publish(sessionID, ev)
		return
	}
	if err := n.Notify(ctx, activity.Notification{SessionID: sessionID, ChannelID: channelID, Event: ev}); err != nil {
		log.Printf("Session %s: tell %s: %v", sessionID, channel, err)
	}
}
