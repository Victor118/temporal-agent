package workflow

import (
	"encoding/json"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// Background tasks (docs/design/async-tasks.md): a session turn's model may
// launch a workflow tool's call without waiting for it. The turn starts a
// BackgroundTaskWorkflow, "<turn>:bg:<call>" (store.BackgroundTaskID),
// abandoned when it closes, and goes on; the task runs the tool, posts its
// result into the session, and wakes its participant with it, as a member's
// message does.

// Events of a background task, on the web: one started, one ended (its
// message posted). Data: {"task", "agent_id"}, and "message_id" at its end.
const (
	EventTaskStarted = "task_started"
	EventTaskResult  = "task_result"
)

// maxTaskResultBytes bounds the result a task passes on to be posted: its
// file (activity.MaxTaskMessageBytes goes in the message), and the
// activity's input, under Temporal's payload limit.
const maxTaskResultBytes = 1 << 20

// BackgroundTaskInput is a background task: the tool call it runs, as the
// turn built it, and the participant its end wakes.
type BackgroundTaskInput struct {
	SessionID string `json:"session_id"`
	// AgentID is the participant whose turn launched it.
	AgentID string `json:"agent_id"`
	Tool    string `json:"tool"`
	// The tool's child workflow: its type, ID ("<task>:tool:<tool>:<call>"),
	// queue and input, the call's context in it (tool.CallContext).
	Workflow  string          `json:"workflow"`
	ChildID   string          `json:"child_id"`
	TaskQueue string          `json:"task_queue"`
	Input     json.RawMessage `json:"input"`
	// SubAgent: the child is an AgentWorkflow, whose result is read by type
	// (subAgentContent).
	SubAgent bool `json:"sub_agent,omitempty"`
}

// BackgroundTaskWorkflow runs a background task's tool as an ordinary child,
// waiting for it, then ends the task: its message posted (PostTaskResult),
// then its participant woken by it (a Relay that reads the session first).
// Cancelled (a member stopped it), its tool is cancelled and its message,
// which says so, wakes nobody: it is posted from a disconnected context, a
// cancelled one scheduling no activity. A task whose end could not be
// written fails: the sweep ends it.
func BackgroundTaskWorkflow(ctx workflow.Context, in BackgroundTaskInput) error {
	logger := workflow.GetLogger(ctx)
	taskID := workflow.GetInfo(ctx).WorkflowExecution.ID

	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: in.ChildID, TaskQueue: in.TaskQueue})
	var raw json.RawMessage
	err := workflow.ExecuteChildWorkflow(childCtx, in.Workflow, in.Input).Get(ctx, &raw)

	// The tool has ended. Cancelled is a tool that ended on the stop: in
	// error, or a sub-agent that returned what it had, cancelled. A result
	// that came with the stop, in the same workflow task, is a result,
	// never thrown away. Whatever comes now changes nothing: the end is
	// posted, told and woken from a disconnected context, a cancellation
	// arriving meanwhile would abandon an activity half way, its message
	// written and nobody woken.
	stopped := ctx.Err() != nil
	cancelled := (err != nil && (stopped || temporal.IsCanceledError(err))) || (err == nil && stopped && in.SubAgent && subAgentCancelled(raw))
	ctx, cancel := workflow.NewDisconnectedContext(ctx)
	defer cancel()

	end := activity.PostTaskResultInput{TaskID: taskID, State: store.BackgroundDone}
	isError := false
	switch {
	case cancelled:
		end.State = store.BackgroundCancelled
	case err != nil:
		end.Content, isError = "The task failed: "+childFailure(err), true
	case in.SubAgent:
		end.Content, isError = subAgentContent(raw)
	default:
		end.Content, isError = tool.DecodeResult(raw)
	}
	if isError {
		end.State = store.BackgroundFailed
	}
	end.Content = truncateTo(end.Content, maxTaskResultBytes)

	var taskAct *activity.TaskActivities
	var out activity.PostTaskResultOutput
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, storeStepOptions), taskAct.PostTaskResult, end).Get(ctx, &out); err != nil {
		logger.Error("Background task's end not written: the sweep will end it", "task", taskID, "error", err)
		return err
	}
	if out.Gone {
		logger.Info("Background task ended in a session deleted", "task", taskID)
		return nil
	}
	if out.File != nil {
		notifyFiles(ctx, tool.TurnRef{SessionID: out.SessionID, TurnKey: out.TurnKey}, in.AgentID, []tool.FileRef{*out.File})
	}
	if out.MessageID != 0 {
		notifySession(ctx, in.SessionID, EventTaskResult, map[string]string{
			"task": taskID, "agent_id": in.AgentID, "message_id": fmt.Sprint(out.MessageID),
		})
	}
	if out.Wake {
		wakeParticipant(ctx, in.AgentID, out)
		// Done with, delivered or given up: the sweep wakes again a task
		// whose wake was never said done.
		if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, storeStepOptions), taskAct.TaskWoken, taskID).Get(ctx, nil); err != nil {
			logger.Warn("Background task's wake not recorded: the sweep will wake again", "task", taskID, "error", err)
		}
	}
	return nil
}

// wakeParticipant delivers a task's end message to its participant, as a
// member's message is delivered: a SignalWithStart, by the relay's code,
// the session read just before. Past the relay's attempts, the message
// gets an end under the turn that would have answered it, so that a late
// delivery is not answered, and the channel is told.
func wakeParticipant(ctx workflow.Context, agentID string, out activity.PostTaskResultOutput) {
	msg := ParticipantMessage{
		MessageID: out.MessageID, UserID: out.UserID, UserName: out.UserName,
		SignReply: out.SignReply, Channel: out.Channel, ChannelID: out.ChannelID,
	}
	message, _ := json.Marshal(msg)
	start, _ := json.Marshal(ParticipantInput{SessionID: out.SessionID, AgentID: agentID, Channel: out.SessionChannel, ChannelID: out.SessionChannelID})
	var relayAct *activity.RelayActivities
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, relayOptions), relayAct.Relay, activity.RelayInput{
		WorkflowID:   ParticipantWorkflowID(out.SessionID, agentID),
		WorkflowType: "ParticipantWorkflow",
		TaskQueue:    workflow.GetInfo(ctx).TaskQueueName,
		Signal:       SignalMessage,
		Message:      message,
		Start:        start,
		SessionID:    out.SessionID,
	}).Get(ctx, nil)
	if err == nil {
		return
	}
	reason := fmt.Sprintf("la fin de la tâche n'a pas pu réveiller @%s : %s", agentID, failureText(err))
	workflow.GetLogger(ctx).Error("Background task's end not delivered", "agent", agentID, "message_id", out.MessageID, "error", err)
	var turnAct *activity.TurnActivities
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, endTurnOptions), turnAct.EndTurn, activity.EndTurnInput{
		SessionID: out.SessionID, TurnKey: store.TurnKey(out.MessageID, agentID), Message: store.TurnEnd(agentID, reason),
	}).Get(ctx, nil); err != nil {
		workflow.GetLogger(ctx).Error("Turn end not written", "agent", agentID, "message_id", out.MessageID, "error", err)
	}
	signer := ""
	if out.SignReply {
		signer = agentID
	}
	notifyResponse(ctx, out.SessionID, out.Channel, out.ChannelID, signer, "Error processing message: "+reason)
}

// subAgentCancelled reports whether a sub-agent's result is that of a run
// cancelled (cancelledOutput).
func subAgentCancelled(raw json.RawMessage) bool {
	var out AgentWorkflowOutput
	return json.Unmarshal(raw, &out) == nil && out.Cancelled
}

// childFailure is why a child workflow failed, for the model and the
// members: its application error's message, without Temporal's envelope.
func childFailure(err error) string {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		return appErr.Message()
	}
	var timeout *temporal.TimeoutError
	if errors.As(err, &timeout) {
		return "it ran out of time"
	}
	var terminated *temporal.TerminatedError
	if errors.As(err, &terminated) {
		return "it was terminated"
	}
	return err.Error()
}

// backgroundStart is a background task the turn launches: what it records,
// the workflow it starts, and the context it is started on (launch).
type backgroundStart struct {
	task  store.BackgroundTask
	input BackgroundTaskInput
	ctx   workflow.Context
}

// newBackgroundStart is the background task a turn's call launches:
// childWorkflow and childInput are the tool's child as buildChildInput made
// it, under childID.
func newBackgroundStart(input AgentWorkflowInput, tc provider.ToolCallInfo, toolInput json.RawMessage, cc tool.CallContext,
	taskID, childID, queue string, childWorkflow, childInput any, subAgent bool) (backgroundStart, error) {
	name, ok := childWorkflow.(string)
	if !ok {
		name = "AgentWorkflow" // a sub-agent: the only workflow given by its function
	}
	encoded, err := json.Marshal(childInput)
	if err != nil {
		return backgroundStart{}, fmt.Errorf("encode the task's input: %w", err)
	}
	return backgroundStart{
		task: store.BackgroundTask{
			ID: taskID, SessionID: input.SessionID, Participant: input.AgentID,
			UserID: input.UserID, UserName: input.UserName, Tool: tc.Name, Summary: activity.TaskSummary(toolInput),
			TurnKey: input.TurnKey, CallID: cc.CallID, Channel: input.Channel, ChannelID: input.ChannelID,
		},
		input: BackgroundTaskInput{
			SessionID: input.SessionID, AgentID: input.AgentID, Tool: tc.Name,
			Workflow: name, ChildID: childID, TaskQueue: queue, Input: encoded, SubAgent: subAgent,
		},
	}, nil
}

// launch records the task, then starts its workflow, abandoned by the turn,
// from a context disconnected from the turn's: a stop of the turn (its
// context cancelled) would otherwise ask Temporal to cancel the child as
// well, which ABANDON does not prevent, only the close of the parent. The
// record too: a task recorded always gets its workflow, a stop of the turn
// meanwhile notwithstanding. The future settles once it started. A refusal
// (the cap) is for the model.
func (b *backgroundStart) launch(ctx workflow.Context) (future workflow.Future, refused string) {
	// Never cancelled: the task is cancelled on its own (StopTask), and the
	// turn closing abandons it.
	b.ctx, _ = workflow.NewDisconnectedContext(ctx)
	var taskAct *activity.TaskActivities
	var reg activity.RegisterTaskOutput
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(b.ctx, storeStepOptions), taskAct.RegisterTask,
		activity.RegisterTaskInput{Task: b.task}).Get(b.ctx, &reg); err != nil {
		return nil, "The background task could not be recorded: " + failureText(err)
	}
	if reg.Refused != "" {
		return nil, reg.Refused
	}
	child := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(b.ctx, workflow.ChildWorkflowOptions{
		WorkflowID: b.task.ID,
		// It runs where the participants run, and outlives the turn: the
		// default policy would terminate it as soon as the turn closes.
		TaskQueue:         workflow.GetInfo(ctx).TaskQueueName,
		ParentClosePolicy: enumspb.PARENT_CLOSE_POLICY_ABANDON,
	}), BackgroundTaskWorkflow, b.input)
	// What is awaited is its start: a turn that closed before it would
	// leave it never started.
	return child.GetChildWorkflowExecution(), ""
}

// started waits for the start of a task launched, on the context it was
// started on (a stop of the turn meanwhile must not read as a failed start,
// and drop a task that runs), and says what the model reads of it, or why
// it did not start: the row is then dropped.
func (b *backgroundStart) started(ctx workflow.Context, future workflow.Future) (string, bool) {
	if err := future.Get(b.ctx, nil); err != nil {
		var taskAct *activity.TaskActivities
		if derr := workflow.ExecuteActivity(workflow.WithActivityOptions(b.ctx, storeStepOptions), taskAct.DropTask, b.task.ID).Get(b.ctx, nil); derr != nil {
			workflow.GetLogger(ctx).Error("A background task that did not start was not dropped: the sweep will end it", "task", b.task.ID, "error", derr)
		}
		return "The background task could not start: " + failureText(err), true
	}
	notifySession(b.ctx, b.task.SessionID, EventTaskStarted, map[string]string{"task": b.task.ID, "agent_id": b.task.Participant})
	return fmt.Sprintf("Background task started, ID %s. Its result will reach you in a message when it ends, in a new turn: "+
		"do not wait for it, nor start it again. Tell the user it runs.", b.task.ID), false
}
