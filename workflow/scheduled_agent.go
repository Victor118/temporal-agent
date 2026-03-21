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
	logger.Info("Scheduled agent starting", "schedule_id", input.ScheduleID, "channel", input.DeliveryChannel)

	// Run the agent loop as a child workflow
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: fmt.Sprintf("%s-agent-%d", input.ScheduleID, workflow.Now(ctx).UnixMilli()),
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	})

	var result AgentWorkflowOutput
	err := workflow.ExecuteChildWorkflow(childCtx, AgentWorkflow, AgentWorkflowInput{
		SessionID:   input.ScheduleID,
		UserMessage: input.Prompt,
		Model:       "", // Uses default from LLM provider
	}).Get(ctx, &result)

	response := result.Response
	if err != nil {
		response = fmt.Sprintf("Scheduled task failed: %s", err.Error())
		logger.Error("Scheduled agent failed", "schedule_id", input.ScheduleID, "error", err)
	}

	// Deliver the result
	deliverCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	})

	var deliverAct *activity.DeliveryActivities
	if err := workflow.ExecuteActivity(deliverCtx, deliverAct.DeliverResult, activity.DeliverInput{
		UserID:     input.UserID,
		Channel:    input.DeliveryChannel,
		Content:    response,
		ScheduleID: input.ScheduleID,
	}).Get(ctx, nil); err != nil {
		logger.Error("Failed to deliver result", "schedule_id", input.ScheduleID, "error", err)
		return fmt.Errorf("deliver result: %w", err)
	}

	return nil
}
