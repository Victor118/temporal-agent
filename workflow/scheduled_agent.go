package workflow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/tool"
)

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

	response := result.Response
	switch {
	case err != nil:
		response = fmt.Sprintf("Scheduled task failed: %s", err.Error())
		logger.Error("Scheduled agent failed", "schedule_id", input.ScheduleID, "error", err)
	case result.Error != "":
		response = fmt.Sprintf("Scheduled task failed: %s", result.Error)
		logger.Error("Scheduled agent failed", "schedule_id", input.ScheduleID, "error", result.Error)
	}

	// Deliver the result
	deliverCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

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
	// list a task that will never run again. Recurring tasks keep their
	// schedule until cancel_schedule.
	if input.Cron == "" {
		var schedAct *activity.ScheduleActivities
		if err := workflow.ExecuteActivity(deliverCtx, schedAct.DeleteSchedule, activity.DeleteScheduleInput{
			ScheduleID: input.ScheduleID,
		}).Get(ctx, nil); err != nil {
			logger.Error("Failed to delete the schedule", "schedule_id", input.ScheduleID, "error", err)
		}
	}

	if deliverErr != nil {
		return fmt.Errorf("deliver result: %w", deliverErr)
	}
	return nil
}
