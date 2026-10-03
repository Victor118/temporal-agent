package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/store"
)

// reportSystemPrompt asks for a fork's report to its parent. The members of
// the parent planned the work there, then opened the fork to carry part of it
// out: they read the report instead of the fork.
const reportSystemPrompt = `You write the report a forked conversation sends back to the conversation it was forked from. Its members planned or specified work there, then opened this fork to carry out part of it; they read your report to learn where that part stands, without reading the fork.

Write in the language of the conversation. Use these four sections, as Markdown headings (## …) in that language, and leave out a section that would be empty:
1. What was done: the results obtained, with every concrete reference — repositories, branches, files, URLs, names, commands, figures.
2. Decisions taken: each one, and why; who took it when several users or assistants take part.
3. Deviations from the plan: where the fork departed from what the parent conversation had planned or specified, and why. The brief the fork started from, when given, says what that was.
4. Open points: the questions still open, the work left, what the parent's members have to decide.

Short bullet points under each. Report only what the conversation contains; never add anything. No preamble, no closing remarks.`

// reportPurposePrompt is added when the fork has a purpose, stated in the
// request: it is a member's words, not instructions.
const reportPurposePrompt = `

The fork had a goal, stated before the conversation: report against it — what of it is done, and what is not.`

// reportLaterPrompt is added to a report that follows another: the fork's
// earlier messages were reported already.
const reportLaterPrompt = `

This fork reported before: the conversation below is only what happened since. Report on it alone.`

type SummarizeForkReportInput struct {
	SessionID string `json:"session_id"` // the fork
	// The fork's messages to report on: after AfterMessageID (its last
	// reported message, 0 = none yet), up to UpToMessageID.
	AfterMessageID int64  `json:"after_message_id"`
	UpToMessageID  int64  `json:"up_to_message_id"`
	Purpose        string `json:"purpose,omitempty"` // what the fork is for
	Model          string `json:"model,omitempty"`   // empty = the worker's default
}

// SummarizeForkReport writes a fork's report to its parent, on the fork's
// messages since its last report: what was done, decided, changed from the
// plan, and left open. The fork's starting brief goes with them, apart, to
// say what the plan was. Same transcript as a fork's summary: the report goes
// into a session other users share, and a private tool's input or result
// stays out of it.
func (a *ForkActivities) SummarizeForkReport(ctx context.Context, in SummarizeForkReportInput) (SummarizeConversationOutput, error) {
	msgs, err := a.Store.LoadMessagesUpTo(ctx, in.SessionID, in.UpToMessageID)
	if err != nil {
		return SummarizeConversationOutput{}, err
	}
	if len(msgs) == 0 || msgs[len(msgs)-1].ID != in.UpToMessageID {
		return SummarizeConversationOutput{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("message %d not found in session %s", in.UpToMessageID, in.SessionID), "MessageNotFound", nil)
	}
	var brief string
	var part []store.MessageWithID
	for _, m := range msgs {
		switch {
		case m.Kind == store.KindForkSummary:
			brief = decodeText(m.Content)
		case m.ID > in.AfterMessageID:
			part = append(part, m)
		}
	}
	transcript, truncated := buildTranscript(part, a.Private)
	if transcript == "" {
		return SummarizeConversationOutput{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("nothing to report in session %s after message %d", in.SessionID, in.AfterMessageID), "NothingToReport", nil)
	}

	system, request := reportSystemPrompt, ""
	if in.Purpose != "" {
		system += reportPurposePrompt
		request += "Goal of the fork: " + in.Purpose + "\n\n"
	}
	if in.AfterMessageID > 0 {
		system += reportLaterPrompt
	}
	if brief != "" {
		request += "What the parent conversation had established when the fork started (the brief it started from):\n\n" + brief + "\n\n"
	}
	request += "Conversation to report on:\n\n" + transcript
	report, err := a.summarize(ctx, in.Model, system, request)
	if err != nil {
		return SummarizeConversationOutput{}, err
	}
	return SummarizeConversationOutput{Summary: report, Truncated: truncated}, nil
}

// ForkReportStore posts a fork's report into its parent.
type ForkReportStore interface {
	AppendForkReport(ctx context.Context, r store.ForkReport) (int64, error)
}

// ReportActivities post the reports forks send to their parents.
type ReportActivities struct {
	Store ForkReportStore
}

type PostForkReportInput struct {
	ForkSessionID   string `json:"fork_session_id"`
	ParentSessionID string `json:"parent_session_id"`
	ForkTitle       string `json:"fork_title"`
	// The fork's messages the report covers: after From, up to UpTo.
	From int64 `json:"from"`
	UpTo int64 `json:"up_to"`
	// The member who sends it, and signs it in the parent.
	ReporterID   string `json:"reporter_id"`
	ReporterName string `json:"reporter_name"`
	Report       string `json:"report"`
}

// PostForkReport posts a report into the fork's parent, as a message of the
// member who sends it, and records it on the fork; it returns its message ID
// in the parent. A retry posts nothing more (store.ForkReportKey). Refusals
// are final: the parent is gone, the member left it, or another report was
// posted meanwhile.
func (a *ReportActivities) PostForkReport(ctx context.Context, in PostForkReportInput) (int64, error) {
	content, _ := json.Marshal(in.Report)
	id, err := a.Store.AppendForkReport(ctx, store.ForkReport{
		ForkSessionID:   in.ForkSessionID,
		ParentSessionID: in.ParentSessionID,
		ReporterID:      in.ReporterID,
		From:            in.From,
		UpTo:            in.UpTo,
		Message: store.Message{
			Role:    store.RoleUser,
			Kind:    store.KindForkReport,
			Content: string(content),
			UserID:  in.ReporterID,
			Author:  in.ReporterName,
			Fork:    &store.ForkRef{SessionID: in.ForkSessionID, Title: in.ForkTitle, UpToMessageID: in.UpTo},
		},
	})
	for errType, refusal := range map[string]error{
		"ParentGone":      store.ErrReportParentGone,
		"NotParentMember": store.ErrReportNotMember,
		"ReportStale":     store.ErrReportStale,
	} {
		if errors.Is(err, refusal) {
			return 0, temporal.NewNonRetryableApplicationError(err.Error(), errType, err)
		}
	}
	return id, err
}
