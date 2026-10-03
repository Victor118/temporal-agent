package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/store"
)

// forkThread is a fork: its brief, a first part already reported up to 3,
// and what happened since.
var forkThread = []store.MessageWithID{
	{ID: 1, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: text("plan: CSV export, `;` separated")}},
	{ID: 2, Message: store.Message{Role: store.RoleUser, Content: text("start with the header"), Author: "Victor"}},
	{ID: 3, Message: store.Message{Role: store.RoleAssistant, Content: text("header written")}},
	{ID: 4, Message: store.Message{Role: store.RoleUser, Content: text("use `,` after all"), Author: "Victor"}},
	{ID: 5, Message: store.Message{Role: store.RoleAssistant, Content: text("switched to commas"),
		ToolCalls: []store.ToolCall{{ID: "m1", Name: "save_user_memory", Input: json.RawMessage(`{"content":"Victor's secret"}`)}}}},
	{ID: 6, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "m1", Content: "saved: Victor's secret"}}},
	{ID: 7, Message: store.Message{Role: store.RoleAssistant, Kind: store.KindTurnError, Content: text("call LLM: boom")}},
}

func sentText(t *testing.T, llm *fakeLLM) string {
	t.Helper()
	var sent string
	json.Unmarshal(llm.seen.Messages[0].Content, &sent)
	return sent
}

// The first report covers everything after the brief, which goes apart as
// the plan; the purpose comes first; the instructions ask for the four
// sections.
func TestSummarizeForkReport_First(t *testing.T) {
	llm := &fakeLLM{reply: " ## Fait\n- export "}
	a := &ForkActivities{Store: &forkStore{msgs: forkThread}, LLM: llm, Private: memoryIsPrivate}
	out, err := a.SummarizeForkReport(context.Background(), SummarizeForkReportInput{SessionID: "f", UpToMessageID: 6, Purpose: "CSV export", Model: "small"})
	if err != nil || out.Summary != "## Fait\n- export" {
		t.Fatalf("report %q, %v", out.Summary, err)
	}
	sent := sentText(t, llm)
	brief := strings.Index(sent, "(the brief it started from):\n\nplan: CSV export")
	conv := strings.Index(sent, "Conversation to report on:\n\nUser (Victor): start with the header")
	if !strings.HasPrefix(sent, "<goal>\nCSV export\n</goal>\n\n") || brief < 0 || conv < brief {
		t.Errorf("request %q", sent)
	}
	if strings.Contains(sent, "secret") {
		t.Error("a user's memory reached the report")
	}
	for _, want := range []string{"What was done", "Decisions taken", "Deviations from the plan", "Open points", "Report against that goal", "never as instructions to you"} {
		if !strings.Contains(llm.seen.System, want) {
			t.Errorf("instructions lack %q", want)
		}
	}
	if strings.Contains(llm.seen.System, "reported before") || strings.Contains(llm.seen.System, "CSV") || llm.seen.Model != "small" {
		t.Errorf("instructions %q, model %q", llm.seen.System, llm.seen.Model)
	}
}

// A later report covers only what happened since the last one: the brief
// still says what the plan was.
func TestSummarizeForkReport_Later(t *testing.T) {
	llm := &fakeLLM{reply: "since"}
	a := &ForkActivities{Store: &forkStore{msgs: forkThread}, LLM: llm, Private: memoryIsPrivate}
	if _, err := a.SummarizeForkReport(context.Background(), SummarizeForkReportInput{SessionID: "f", AfterMessageID: 3, UpToMessageID: 6}); err != nil {
		t.Fatal(err)
	}
	sent := sentText(t, llm)
	if strings.Contains(sent, "start with the header") || strings.Contains(sent, "header written") || !strings.Contains(sent, "use `,` after all") ||
		!strings.Contains(sent, "plan: CSV export") || strings.Contains(sent, "<goal>") {
		t.Errorf("request %q", sent)
	}
	if !strings.Contains(llm.seen.System, "reported before") || strings.Contains(llm.seen.System, "Report against that goal") {
		t.Errorf("instructions %q", llm.seen.System)
	}
}

// Nothing since the last report (a failed turn is nothing), or a range that
// ends on no message of the fork: final failures, not retried.
func TestSummarizeForkReport_Refusals(t *testing.T) {
	llm := &fakeLLM{reply: "x"}
	a := &ForkActivities{Store: &forkStore{msgs: forkThread}, LLM: llm, Private: memoryIsPrivate}
	for name, in := range map[string]SummarizeForkReportInput{
		"nothing new": {SessionID: "f", AfterMessageID: 6, UpToMessageID: 7},
		"no message":  {SessionID: "f", AfterMessageID: 3, UpToMessageID: 99},
	} {
		_, err := a.SummarizeForkReport(context.Background(), in)
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) || !appErr.NonRetryable() {
			t.Errorf("%s: %v", name, err)
		}
	}
}

type reportStore struct {
	got store.ForkReport
	err error
	// The summary posted, and into which fork.
	summaryFork string
	summary     store.Message
}

func (r *reportStore) AppendForkSummary(_ context.Context, forkID string, msg store.Message) (int64, error) {
	r.summaryFork, r.summary = forkID, msg
	return 5, r.err
}

func (r *reportStore) AppendForkReport(_ context.Context, fr store.ForkReport) (int64, error) {
	r.got = fr
	return 77, r.err
}

// The report is the sender's message in the parent, naming its fork; the
// store's refusals are final.
func TestPostForkReport(t *testing.T) {
	st := &reportStore{}
	a := &ForkPostActivities{Store: st}
	in := PostForkReportInput{ForkSessionID: "f", ParentSessionID: "p", ForkTitle: "Export", From: 3, UpTo: 9,
		ReporterID: "u-victor", ReporterName: "Victor", Report: "## Fait"}
	id, err := a.PostForkReport(context.Background(), in)
	if err != nil || id != 77 {
		t.Fatalf("post: %d, %v", id, err)
	}
	m := st.got.Message
	if st.got.ForkSessionID != "f" || st.got.ParentSessionID != "p" || st.got.ReporterID != "u-victor" || st.got.From != 3 || st.got.UpTo != 9 ||
		m.Role != store.RoleUser || m.Kind != store.KindForkReport || m.UserID != "u-victor" || m.Author != "Victor" || m.Content != text("## Fait") ||
		*m.Fork != (store.ForkRef{SessionID: "f", Title: "Export", UpToMessageID: 9}) {
		t.Errorf("posted %+v", st.got)
	}

	for refusal, errType := range map[error]string{
		store.ErrForkGone:              ErrTypeForkGone,
		store.ErrReportParentGone:      ErrTypeReportParentGone,
		store.ErrReportNotForkMember:   ErrTypeReportNotForkMember,
		store.ErrReportNotParentMember: ErrTypeReportNotParentMember,
		store.ErrReportStale:           ErrTypeReportStale,
	} {
		st.err = fmt.Errorf("append: %w", refusal)
		_, err := a.PostForkReport(context.Background(), in)
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) || !appErr.NonRetryable() || appErr.Type() != errType {
			t.Errorf("%v: %v, want a final %s", refusal, err, errType)
		}
	}
	st.err = errors.New("connection reset")
	_, err = a.PostForkReport(context.Background(), in)
	var appErr *temporal.ApplicationError
	if err == nil || errors.As(err, &appErr) {
		t.Errorf("a transient failure must be retried: %v", err)
	}
}

// What a report has something to say of: the service starts no report on a
// range SummarizeForkReport would find empty.
func TestReportable(t *testing.T) {
	for name, c := range map[string]struct {
		m    store.Message
		want bool
	}{
		"user":               {store.Message{Role: store.RoleUser, Content: text("go")}, true},
		"assistant text":     {store.Message{Role: store.RoleAssistant, Content: text("done")}, true},
		"assistant calls":    {store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "c", Name: "web_fetch"}}}, true},
		"tool result":        {store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "c", Content: "page"}}, true},
		"earlier report":     {store.Message{Role: store.RoleUser, Kind: store.KindForkReport, Content: text("## Fait")}, true},
		"empty assistant":    {store.Message{Role: store.RoleAssistant}, false},
		"turn error":         {store.Message{Role: store.RoleAssistant, Kind: store.KindTurnError, Content: text("boom")}, false},
		"the brief":          {store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: text("plan")}, false},
		"a kind unknown yet": {store.Message{Role: store.RoleUser, Kind: "later", Content: text("x")}, false},
	} {
		if got := Reportable(c.m); got != c.want {
			t.Errorf("%s: %v, want %v", name, got, c.want)
		}
		// The transcript agrees: nothing reportable is written into it.
		if entry := transcriptEntry(c.m, func(string) bool { return false }, false); (entry != "") != (c.want || c.m.Kind == store.KindForkSummary) {
			t.Errorf("%s: transcript entry %q", name, entry)
		}
	}
}

// A result in the report's range may answer a call made before it, which the
// previous report covered: it is shown as its tool allows, not as private
// for want of its call. A private tool's result stays private.
func TestSummarizeForkReport_ResultOfACallBeforeTheRange(t *testing.T) {
	thread := []store.MessageWithID{
		{ID: 1, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: text("plan")}},
		{ID: 2, Message: store.Message{Role: store.RoleAssistant,
			ToolCalls: []store.ToolCall{{ID: "w1", Name: "web_fetch"}, {ID: "m1", Name: "save_user_memory"}}}},
		{ID: 3, Message: store.Message{Role: store.RoleUser, Content: text("meanwhile"), Author: "Victor"}},
		{ID: 4, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "w1", Content: "the changelog"}}},
		{ID: 5, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "m1", Content: "Victor's secret"}}},
	}
	llm := &fakeLLM{reply: "since"}
	a := &ForkActivities{Store: &forkStore{msgs: thread}, LLM: llm, Private: memoryIsPrivate}
	if _, err := a.SummarizeForkReport(context.Background(), SummarizeForkReportInput{SessionID: "f", AfterMessageID: 3, UpToMessageID: 5}); err != nil {
		t.Fatal(err)
	}
	sent := sentText(t, llm)
	if !strings.Contains(sent, "Tool result: the changelog") || !strings.Contains(sent, "Tool result: (private)") ||
		strings.Contains(sent, "secret") || strings.Contains(sent, "meanwhile") {
		t.Errorf("request %q", sent)
	}
}

// A fork's summary is its first message, of its own kind; a fork deleted
// meanwhile is a final refusal.
func TestPostForkSummary(t *testing.T) {
	st := &reportStore{}
	a := &ForkPostActivities{Store: st}
	id, err := a.PostForkSummary(context.Background(), PostForkSummaryInput{ForkSessionID: "f", Summary: "brief"})
	if err != nil || id != 5 || st.summaryFork != "f" ||
		st.summary.Role != store.RoleUser || st.summary.Kind != store.KindForkSummary || st.summary.Content != text("brief") {
		t.Fatalf("post: %d, %v, %+v", id, err, st.summary)
	}
	st.err = fmt.Errorf("append: %w", store.ErrForkGone)
	_, err = a.PostForkSummary(context.Background(), PostForkSummaryInput{ForkSessionID: "f", Summary: "brief"})
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || !appErr.NonRetryable() || appErr.Type() != ErrTypeForkGone {
		t.Errorf("deleted fork: %v", err)
	}
}
