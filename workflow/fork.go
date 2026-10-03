package workflow

import (
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
)

// SSE events a fork sends to its own session when its summary is settled.
const (
	EventForkReady  = "fork_ready"
	EventForkFailed = "fork_failed"
)

// ForkWorkflowID is the ID of the workflow seeding a fork. The handlers look
// it up to know whether the summary is still being written.
func ForkWorkflowID(forkSessionID string) string { return "fork-" + forkSessionID }

type ForkSessionInput struct {
	ForkSessionID   string `json:"fork_session_id"`
	ParentSessionID string `json:"parent_session_id"`
	UpToMessageID   int64  `json:"up_to_message_id"`
	Purpose         string `json:"purpose,omitempty"` // what the fork is for: steers the summary
	Model           string `json:"model,omitempty"`   // summary model; empty = the worker's default
}

// ForkSessionWorkflow seeds a forked session: it summarizes the parent up to
// the fork's message and posts the summary as the fork's first message,
// recorded on the fork (store.AppendForkSummary). The session record exists
// already; the fork takes messages once the summary is recorded.
func ForkSessionWorkflow(ctx workflow.Context, in ForkSessionInput) error {
	llmCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 3 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        5 * time.Second,
			BackoffCoefficient:     3.0,
			MaximumInterval:        2 * time.Minute,
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"PermanentAPIError", "MessageNotFound"},
		},
	})
	var forkAct *activity.ForkActivities
	var out activity.SummarizeConversationOutput
	err := workflow.ExecuteActivity(llmCtx, forkAct.SummarizeConversation, activity.SummarizeConversationInput{
		SessionID:     in.ParentSessionID,
		UpToMessageID: in.UpToMessageID,
		Purpose:       in.Purpose,
		Model:         in.Model,
	}).Get(ctx, &out)
	if err != nil {
		notifySession(ctx, in.ForkSessionID, EventForkFailed, map[string]string{"error": err.Error()})
		return fmt.Errorf("summarize parent session: %w", err)
	}

	summary := out.Summary
	if out.Truncated {
		summary = "(The parent conversation was too long: its beginning is not covered by this summary.)\n\n" + summary
	}

	var postAct *activity.ForkPostActivities
	postCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5, NonRetryableErrorTypes: []string{activity.ErrTypeForkGone}},
	})
	if err := workflow.ExecuteActivity(postCtx, postAct.PostForkSummary, activity.PostForkSummaryInput{
		ForkSessionID: in.ForkSessionID,
		Summary:       summary,
	}).Get(ctx, nil); err != nil {
		notifySession(ctx, in.ForkSessionID, EventForkFailed, map[string]string{"error": err.Error()})
		return fmt.Errorf("persist summary: %w", err)
	}

	notifySession(ctx, in.ForkSessionID, EventForkReady, map[string]string{})
	return nil
}

// notifySession sends an event to the members watching a session. Best effort:
// a member who misses it sees the result when the session reloads.
func notifySession(ctx workflow.Context, sessionID, eventType string, payload map[string]string) {
	payload["type"] = eventType
	data, _ := json.Marshal(payload)
	var notifAct *activity.NotificationActivities
	_ = workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second, RetryPolicy: notifyRetry}),
		notifAct.NotifyStep,
		activity.NotifyInput{SessionID: sessionID, Event: activity.SSEEvent{Type: eventType, Data: data}},
	).Get(ctx, nil)
}
