package workflow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// scheduleStepOptions bound the steps that follow a scheduled run: its
// delivery, the end or the pause of its schedule.
var scheduleStepOptions = workflow.ActivityOptions{
	StartToCloseTimeout: 30 * time.Second,
	RetryPolicy: &temporal.RetryPolicy{
		MaximumAttempts: 3,
	},
}

// pauseRefused pauses a recurring task its agent refused, and reports
// whether it is paused: if not, the next run tries again, and tells again.
func pauseRefused(ctx workflow.Context, input tool.ScheduledAgentInput, reason string) bool {
	var schedAct *activity.ScheduleActivities
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, scheduleStepOptions), schedAct.PauseSchedule, activity.PauseScheduleInput{
		ScheduleID: input.ScheduleID,
		Reason:     "agent not open to its owner: " + reason,
	}).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Error("Schedule not paused", "schedule_id", input.ScheduleID, "error", err)
		return false
	}
	return true
}

// ScheduledAgentWorkflow runs an agent loop from a scheduled trigger.
// It executes the prompt, then delivers the result via the configured channel.
func ScheduledAgentWorkflow(ctx workflow.Context, input tool.ScheduledAgentInput) error {
	logger := workflow.GetLogger(ctx)
	logger.Info("Scheduled agent starting", "schedule_id", input.ScheduleID)

	// Identifies this run of the schedule: a cron fires repeatedly under the
	// same ScheduleID, so the run timestamp separates the runs while keeping
	// retries of one run idempotent.
	runUnixMilli := workflow.Now(ctx).UnixMilli()

	// Run the agent loop as a child workflow
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("%s-agent-%d", input.ScheduleID, runUnixMilli),
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	})

	var result AgentWorkflowOutput
	err := workflow.ExecuteChildWorkflow(childCtx, AgentWorkflow, AgentWorkflowInput{
		SessionID:   input.ScheduleID,
		AgentID:     input.AgentID,
		UserMessage: input.Prompt,
		Model:       "", // Uses default from LLM provider
		// The task runs for the user who scheduled it: the scheduling tools
		// act only for an identified user, and the answer is theirs, so is
		// the memory it is written with.
		UserID:         input.UserID,
		LoadUserMemory: true,
	}).Get(ctx, &result)

	response, failed := result.Response, true
	switch {
	case err == nil && result.ErrorType == activity.AgentNotOpen && input.Cron != "":
		// A recurring task whose agent is closed to its owner would fail at
		// every run, and say so every time: paused, and its owner told once.
		paused := pauseRefused(ctx, input, result.Error)
		response = fmt.Sprintf("Scheduled task failed: %s", result.Error)
		if paused {
			response = fmt.Sprintf("Tâche planifiée mise en pause : %s Elle ne reprendra pas d'elle-même ; une fois l'accès donné, annule-la et planifie-la à nouveau.", result.Error)
		}
		logger.Error("Scheduled agent refused", "schedule_id", input.ScheduleID, "error", result.Error, "paused", paused)
	case err != nil:
		response = fmt.Sprintf("Scheduled task failed: %s", err.Error())
		logger.Error("Scheduled agent failed", "schedule_id", input.ScheduleID, "error", err)
	case result.Error != "":
		response = fmt.Sprintf("Scheduled task failed: %s", result.Error)
		logger.Error("Scheduled agent failed", "schedule_id", input.ScheduleID, "error", result.Error)
	default:
		failed = false
	}

	// Deliver the result
	deliverCtx := workflow.WithActivityOptions(ctx, scheduleStepOptions)

	var deliverAct *activity.DeliveryActivities
	deliverErr := workflow.ExecuteActivity(deliverCtx, deliverAct.DeliverResult, activity.DeliverInput{
		UserID:       input.UserID,
		Content:      response,
		ScheduleID:   input.ScheduleID,
		RunUnixMilli: runUnixMilli,
	}).Get(ctx, nil)
	if deliverErr != nil {
		logger.Error("Failed to deliver result", "schedule_id", input.ScheduleID, "error", deliverErr)
	}

	// A one-shot schedule has fired its only action: delete it and close its
	// task log even when the delivery failed, since keeping them would only
	// list a task that will never run again. The log says whether the result
	// reached the user: a failed task is not "completed". Recurring tasks
	// keep their schedule until cancel_schedule.
	if input.Cron == "" {
		status := store.TaskCompleted
		if failed || deliverErr != nil {
			status = store.TaskFailed
		}
		var schedAct *activity.ScheduleActivities
		if err := workflow.ExecuteActivity(deliverCtx, schedAct.DeleteSchedule, activity.DeleteScheduleInput{
			ScheduleID: input.ScheduleID,
			Status:     status,
		}).Get(ctx, nil); err != nil {
			logger.Error("Failed to delete the schedule", "schedule_id", input.ScheduleID, "error", err)
		}
	}

	if deliverErr != nil {
		return fmt.Errorf("deliver result: %w", deliverErr)
	}
	return nil
}
