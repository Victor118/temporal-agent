package workflow

import (
	"reflect"
	"strconv"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"
)

// A SessionWorkflow still open when a worker is deployed is replayed by the
// new code, from a history the old code wrote. Writing the error of a turn
// that produced nothing adds a PersistContext: an old history, where the
// failure was only notified, must replay without it, and a new one with it.
func TestSessionWorkflow_ReplaysAFailedTurnOfEitherVersion(t *testing.T) {
	for _, tc := range []struct {
		name string
		// after adds what the turn's end recorded, and returns the task that
		// went back to waiting for a message.
		after func(h *replayHistory, task int64) int64
	}{
		{"before the change", func(h *replayHistory, task int64) int64 {
			return h.activity(task, "NotifyStep")
		}},
		{"after the change", func(h *replayHistory, task int64) int64 {
			h.versionMarker(task, turnErrorChangeID, 1)
			task = h.activity(task, "PersistContext")
			return h.activity(task, "NotifyStep")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &replayHistory{t: t}
			task := h.started(SessionWorkflowInput{SessionID: "s1", AgentID: "default"})
			// The idle wait's timer; a message does not cancel it.
			h.timer(task)
			h.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
				SignalName: SignalUserMessage, Input: h.payloads(UserMessage{Text: "analyse", UserID: "victor", Stored: true}),
			}})
			task = h.workflowTask()
			child := &commonpb.WorkflowExecution{WorkflowId: "s1-turn-1", RunId: "child-run"}
			agentType := &commonpb.WorkflowType{Name: "AgentWorkflow"}
			initiated := h.add(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
				WorkflowId: child.WorkflowId, WorkflowType: agentType, WorkflowTaskCompletedEventId: task,
			}})
			childStarted := h.add(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, &historypb.HistoryEvent_ChildWorkflowExecutionStartedEventAttributes{ChildWorkflowExecutionStartedEventAttributes: &historypb.ChildWorkflowExecutionStartedEventAttributes{
				InitiatedEventId: initiated, WorkflowExecution: child, WorkflowType: agentType,
			}})
			h.workflowTask()
			// The first LLM call failed: the turn has no message of its own.
			h.add(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED, &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
				InitiatedEventId: initiated, StartedEventId: childStarted, WorkflowExecution: child, WorkflowType: agentType,
				Result: h.payloads(AgentWorkflowOutput{Error: "call LLM: credit balance is too low"}),
			}})
			h.timer(tc.after(h, h.workflowTask()))

			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflow(SessionWorkflow)
			if err := replayer.ReplayWorkflowHistory(nil, &historypb.History{Events: h.events}); err != nil {
				t.Fatalf("replay: %v", err)
			}
		})
	}
}

// replayHistory builds a workflow history event by event, the IDs following
// the order the events are added in. A timer or an activity is named after
// the event it is recorded as, as the SDK names its commands.
type replayHistory struct {
	t      *testing.T
	events []*historypb.HistoryEvent
}

// add appends an event and returns its ID. attrs is one of the generated
// HistoryEvent_*EventAttributes wrappers, whose interface is unexported.
func (h *replayHistory) add(eventType enumspb.EventType, attrs interface{}) int64 {
	e := &historypb.HistoryEvent{EventId: int64(len(h.events) + 1), EventType: eventType}
	reflect.ValueOf(e).Elem().FieldByName("Attributes").Set(reflect.ValueOf(attrs))
	h.events = append(h.events, e)
	return e.EventId
}

// timer adds a timer started by task: the session waiting for a message.
func (h *replayHistory) timer(task int64) int64 {
	return h.add(enumspb.EVENT_TYPE_TIMER_STARTED, &historypb.HistoryEvent_TimerStartedEventAttributes{TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{
		TimerId: strconv.Itoa(len(h.events) + 1), WorkflowTaskCompletedEventId: task,
	}})
}

// payloads encodes v as the SDK would.
func (h *replayHistory) payloads(v interface{}) *commonpb.Payloads {
	p, err := converter.GetDefaultDataConverter().ToPayloads(v)
	if err != nil {
		h.t.Fatal(err)
	}
	return p
}

// started opens the history with the workflow's start and first task, and
// returns that task's completion.
func (h *replayHistory) started(input SessionWorkflowInput) int64 {
	h.add(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
		WorkflowType: &commonpb.WorkflowType{Name: "SessionWorkflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "agent"},
		Input:        h.payloads(input),
		Attempt:      1,
	}})
	return h.workflowTask()
}

// workflowTask adds a scheduled, started and completed workflow task, and
// returns its completion: the commands it made follow it.
func (h *replayHistory) workflowTask() int64 {
	scheduled := h.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
		TaskQueue: &taskqueuepb.TaskQueue{Name: "agent"}, Attempt: 1,
	}})
	started := h.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{
		ScheduledEventId: scheduled,
	}})
	return h.add(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
		ScheduledEventId: scheduled, StartedEventId: started,
	}})
}

// activity adds an activity scheduled by task, run and completed, then the
// workflow task that receives its result, whose completion it returns.
func (h *replayHistory) activity(task int64, name string) int64 {
	scheduled := h.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
		ActivityId: strconv.Itoa(len(h.events) + 1), ActivityType: &commonpb.ActivityType{Name: name},
		TaskQueue: &taskqueuepb.TaskQueue{Name: "agent"}, WorkflowTaskCompletedEventId: task,
	}})
	started := h.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
		ScheduledEventId: scheduled, Attempt: 1,
	}})
	h.add(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
		ScheduledEventId: scheduled, StartedEventId: started,
	}})
	return h.workflowTask()
}

// versionMarker adds what workflow.GetVersion records the first time it runs:
// the marker, then the search attribute naming the change.
func (h *replayHistory) versionMarker(task int64, changeID string, version sdkworkflow.Version) {
	h.add(enumspb.EVENT_TYPE_MARKER_RECORDED, &historypb.HistoryEvent_MarkerRecordedEventAttributes{MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
		MarkerName: "Version",
		Details: map[string]*commonpb.Payloads{
			"change-id": h.payloads(changeID),
			"version":   h.payloads(version),
		},
		WorkflowTaskCompletedEventId: task,
	}})
	h.add(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, &historypb.HistoryEvent_UpsertWorkflowSearchAttributesEventAttributes{UpsertWorkflowSearchAttributesEventAttributes: &historypb.UpsertWorkflowSearchAttributesEventAttributes{
		WorkflowTaskCompletedEventId: task,
		SearchAttributes: &commonpb.SearchAttributes{IndexedFields: map[string]*commonpb.Payload{
			"TemporalChangeVersion": h.payloads([]string{changeID + "-1"}).Payloads[0],
		}},
	}})
}
