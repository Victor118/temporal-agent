package workflow

import (
	"fmt"
	"strconv"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
)

// SSE events of a fork's report: to the parent when it arrives, to the fork
// when it is posted or failed.
const (
	EventForkReport       = "fork_report"
	EventForkReported     = "fork_reported"
	EventForkReportFailed = "fork_report_failed"
)

// ReportWorkflowID is the ID of the workflow writing a fork's report, from
// the fork's last reported message: every attempt at one report has it, so
// a second click while it runs starts nothing, and the handlers find it.
// Once posted, the fork's last reported message moves, and so does the ID.
func ReportWorkflowID(forkSessionID string, from int64) string {
	return "report-" + forkSessionID + "-" + strconv.FormatInt(from, 10)
}

type ReportToParentInput struct {
	ForkSessionID   string `json:"fork_session_id"`
	ParentSessionID string `json:"parent_session_id"`
	ForkTitle       string `json:"fork_title"`
	Purpose         string `json:"purpose,omitempty"`
	// The fork's messages to report on: after From (its last reported
	// message, 0 = none yet), up to UpTo.
	From int64 `json:"from"`
	UpTo int64 `json:"up_to"`
	// The member who sends the report: it is their message in the parent.
	ReporterID   string `json:"reporter_id"`
	ReporterName string `json:"reporter_name"`
	Model        string `json:"model,omitempty"` // empty = the worker's default
}

// ReportToParentWorkflow sends a fork's report to its parent session: it
// summarizes the fork's messages since its last report, and posts the summary
// into the parent as the reporting member's message. It calls no agent: the
// parent's members mention one if they want its reaction.
func ReportToParentWorkflow(ctx workflow.Context, in ReportToParentInput) error {
	failed := func(step string, err error) error {
		notifySession(ctx, in.ForkSessionID, EventForkReportFailed, map[string]string{"error": err.Error()})
		return fmt.Errorf("%s: %w", step, err)
	}

	llmCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 3 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        5 * time.Second,
			BackoffCoefficient:     3.0,
			MaximumInterval:        2 * time.Minute,
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"PermanentAPIError", "MessageNotFound", "NothingToReport"},
		},
	})
	var forkAct *activity.ForkActivities
	var out activity.SummarizeConversationOutput
	if err := workflow.ExecuteActivity(llmCtx, forkAct.SummarizeForkReport, activity.SummarizeForkReportInput{
		SessionID:      in.ForkSessionID,
		AfterMessageID: in.From,
		UpToMessageID:  in.UpTo,
		Purpose:        in.Purpose,
		Model:          in.Model,
	}).Get(ctx, &out); err != nil {
		return failed("summarize the fork", err)
	}
	report := out.Summary
	if out.Truncated {
		report = "(This part of the fork was too long: its beginning is not covered by this report.)\n\n" + report
	}

	postCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 5,
			NonRetryableErrorTypes: []string{
				activity.ErrTypeForkGone, activity.ErrTypeReportParentGone,
				activity.ErrTypeReportNotForkMember, activity.ErrTypeReportNotParentMember, activity.ErrTypeReportStale,
			},
		},
	})
	var reportAct *activity.ForkPostActivities
	var messageID int64
	if err := workflow.ExecuteActivity(postCtx, reportAct.PostForkReport, activity.PostForkReportInput{
		ForkSessionID:   in.ForkSessionID,
		ParentSessionID: in.ParentSessionID,
		ForkTitle:       in.ForkTitle,
		From:            in.From,
		UpTo:            in.UpTo,
		ReporterID:      in.ReporterID,
		ReporterName:    in.ReporterName,
		Report:          report,
	}).Get(ctx, &messageID); err != nil {
		return failed("post the report", err)
	}

	id := strconv.FormatInt(messageID, 10)
	notifySession(ctx, in.ParentSessionID, EventForkReport, map[string]string{"fork_session_id": in.ForkSessionID, "message_id": id})
	notifySession(ctx, in.ForkSessionID, EventForkReported, map[string]string{
		"message": "Rapport envoyé à la session parente.", "parent_session_id": in.ParentSessionID, "message_id": id,
	})
	return nil
}
