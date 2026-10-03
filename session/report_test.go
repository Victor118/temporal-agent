package session

import (
	"context"
	"errors"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

const parentID = "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d"

var victor = &store.User{ID: "u-victor", Email: "victor@example.com", DisplayName: "Victor"}

// forkStore is a fork of parentID, with its brief and two messages since.
func forkStore() *memStore {
	return &memStore{
		session: &store.Session{SessionID: sid, Title: "Export CSV", AgentID: "default", ParentSessionID: parentID,
			ForkedAtMessageID: 7, ForkedBy: victor.ID, ForkPurpose: "CSV export", SummaryMessageID: 1},
		others: map[string]*store.Session{parentID: {SessionID: parentID, Title: "Plan"}},
		messages: []store.MessageWithID{
			{ID: 1, Message: store.Message{Role: store.RoleUser, Kind: store.KindForkSummary, Content: `"brief"`}},
			{ID: 2, Message: store.Message{Role: store.RoleUser, Content: `"go"`}},
			{ID: 3, Message: store.Message{Role: store.RoleAssistant, Content: `"done"`}},
		},
	}
}

// A member of both reports the fork's messages since its start; the
// workflow's ID is the report's, retried only once failed.
func TestReportToParent_First(t *testing.T) {
	st, tc := forkStore(), &fakeTemporal{}
	if err := newTest(st, tc).ReportToParent(context.Background(), sid, victor); err != nil {
		t.Fatal(err)
	}
	if len(tc.started) != 1 || tc.started[0] != workflow.ReportWorkflowID(sid, 0) ||
		tc.options[0].WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY {
		t.Fatalf("started %v %+v", tc.started, tc.options)
	}
	want := workflow.ReportToParentInput{ForkSessionID: sid, ParentSessionID: parentID, ForkTitle: "Export CSV", Purpose: "CSV export",
		From: 0, UpTo: 3, ReporterID: victor.ID, ReporterName: "Victor"}
	if tc.inputs[0] != want {
		t.Errorf("input %+v, want %+v", tc.inputs[0], want)
	}
}

// A second report starts after the first one's last message.
func TestReportToParent_Second(t *testing.T) {
	st, tc := forkStore(), &fakeTemporal{}
	st.session.LastReportedMessageID = 3
	st.messages = append(st.messages, store.MessageWithID{ID: 9, Message: store.Message{Role: store.RoleUser, Content: `"more"`}})
	if err := newTest(st, tc).ReportToParent(context.Background(), sid, victor); err != nil {
		t.Fatal(err)
	}
	in := tc.inputs[0].(workflow.ReportToParentInput)
	if tc.started[0] != workflow.ReportWorkflowID(sid, 3) || in.From != 3 || in.UpTo != 9 {
		t.Errorf("started %v, input %+v", tc.started, in)
	}
}

// Who may report, and when: a member of the parent, from a fork that still
// has one, once its brief is in, with something new to say.
func TestReportToParent_Refusals(t *testing.T) {
	for name, c := range map[string]struct {
		adjust func(*memStore, *fakeTemporal)
		want   error
	}{
		"not a fork": {func(st *memStore, _ *fakeTemporal) {
			st.session.ParentSessionID, st.session.ForkedAtMessageID = "", 0
		}, ErrNotAFork},
		"parent deleted": {func(st *memStore, _ *fakeTemporal) { st.session.ParentSessionID = "" }, ErrNoParent},
		"not a member of the parent": {func(st *memStore, _ *fakeTemporal) {
			st.outsiders = map[string][]string{parentID: {victor.ID}}
		}, ErrNotParentMember},
		"brief pending": {func(st *memStore, tc *fakeTemporal) {
			st.session.SummaryMessageID, st.messages = 0, st.messages[1:]
			tc.running = []string{workflow.ForkWorkflowID(sid)}
		}, ErrSummaryPending},
		"nothing new": {func(st *memStore, _ *fakeTemporal) {
			st.session.LastReportedMessageID = 3
			st.messages = append(st.messages, store.MessageWithID{ID: 4, Message: store.Message{Role: store.RoleAssistant, Kind: store.KindTurnError, Content: `"boom"`}})
		}, ErrNothingToReport},
		"only the brief": {func(st *memStore, _ *fakeTemporal) { st.messages = st.messages[:1] }, ErrNothingToReport},
		// The summary would find nothing in it either (activity.Reportable).
		"only an empty answer": {func(st *memStore, _ *fakeTemporal) {
			st.session.LastReportedMessageID = 3
			st.messages = append(st.messages, store.MessageWithID{ID: 4, Message: store.Message{Role: store.RoleAssistant}})
		}, ErrNothingToReport},
	} {
		t.Run(name, func(t *testing.T) {
			st, tc := forkStore(), &fakeTemporal{}
			c.adjust(st, tc)
			if err := newTest(st, tc).ReportToParent(context.Background(), sid, victor); !errors.Is(err, c.want) {
				t.Errorf("err %v, want %v", err, c.want)
			}
			if len(tc.started) != 0 {
				t.Errorf("started %v", tc.started)
			}
		})
	}
}

// The rail's state: the reason a member cannot report, a report running or
// failed, the latest one.
func TestReportState(t *testing.T) {
	ctx := context.Background()
	at := time.Date(2026, 10, 3, 14, 2, 0, 0, time.UTC)

	st, tc := forkStore(), &fakeTemporal{}
	st.session.LastReportID, st.session.LastReportedAt = 12, &at
	got, err := newTest(st, tc).ReportState(ctx, st.session, st.messages, victor)
	if err != nil || !got.CanReport() || got.Pending || got.Failed || got.LastReportID != 12 || got.LastReportedAt != &at || got.ParentSessionID != parentID {
		t.Errorf("ready: %+v, %v", got, err)
	}

	tc.running = []string{workflow.ReportWorkflowID(sid, 0)}
	if got, _ := newTest(st, tc).ReportState(ctx, st.session, st.messages, victor); !got.Pending || got.CanReport() {
		t.Errorf("running: %+v", got)
	}

	tc = &fakeTemporal{closed: map[string]enumspb.WorkflowExecutionStatus{workflow.ReportWorkflowID(sid, 0): enumspb.WORKFLOW_EXECUTION_STATUS_FAILED}}
	if got, _ := newTest(st, tc).ReportState(ctx, st.session, st.messages, victor); !got.Failed || !got.CanReport() {
		t.Errorf("failed: %+v, want a retry allowed", got)
	}
	// A failure is shown for a while, not forever.
	tc.closedAt = map[string]time.Time{workflow.ReportWorkflowID(sid, 0): time.Now().Add(-reportFailureShown - time.Minute)}
	if got, _ := newTest(st, tc).ReportState(ctx, st.session, st.messages, victor); got.Failed || !got.CanReport() {
		t.Errorf("failed long ago: %+v", got)
	}
	tc.closedAt = nil

	// The agent on a turn, or waiting on a member's answer: a report may go,
	// covering what is written so far, and the page says so. A fork's
	// summary being written is no turn.
	for name, byType := range map[string]map[string][]string{
		"on a turn":         {"AgentWorkflow": {sid + "-turn-1"}},
		"asking a question": {"AgentWorkflow": {sid + "-turn-1"}, "AskUserWorkflow": {sid + "-tool-ask_user-1"}},
	} {
		working := &fakeTemporal{byType: byType}
		if got, _ := newTest(st, working).ReportState(ctx, st.session, st.messages, victor); !got.CanReport() || !got.AgentWorking {
			t.Errorf("agent %s: %+v", name, got)
		}
	}
	summarizing := &fakeTemporal{running: []string{workflow.ForkWorkflowID(sid)}}
	if got, _ := newTest(st, summarizing).ReportState(ctx, st.session, st.messages, victor); got.AgentWorking || !got.CanReport() {
		t.Errorf("summary workflow finishing: %+v", got)
	}
	// Posted: the next report has another ID, and nothing is new yet.
	st.session.LastReportedMessageID = 3
	if got, _ := newTest(st, tc).ReportState(ctx, st.session, st.messages, victor); got.Failed || !got.NothingNew || got.CanReport() {
		t.Errorf("after a report: %+v", got)
	}

	st = forkStore()
	st.outsiders = map[string][]string{parentID: {victor.ID}}
	if got, err := newTest(st, &fakeTemporal{}).ReportState(ctx, st.session, st.messages, victor); err != nil || !errors.Is(got.Refused, ErrNotParentMember) || got.CanReport() {
		t.Errorf("not a member of the parent: %+v, %v", got, err)
	}
	st.session.ParentSessionID = ""
	if got, _ := newTest(st, &fakeTemporal{}).ReportState(ctx, st.session, st.messages, victor); !errors.Is(got.Refused, ErrNoParent) {
		t.Errorf("parent deleted: %+v", got)
	}
	st.session.SummaryMessageID, st.messages = 0, st.messages[1:]
	st.session.ParentSessionID = parentID
	st.outsiders = nil
	if got, _ := newTest(st, &fakeTemporal{running: []string{workflow.ForkWorkflowID(sid)}}).ReportState(ctx, st.session, st.messages, victor); !got.SummaryPending || got.CanReport() {
		t.Errorf("brief pending: %+v", got)
	}
	if _, err := newTest(st, &fakeTemporal{}).ReportState(ctx, &store.Session{SessionID: parentID}, nil, victor); !errors.Is(err, ErrNotAFork) {
		t.Errorf("not a fork: %v", err)
	}
}

// A report never ends between a tool call and its result. The turns store a
// call with its results, in one write, so a stored call without its result
// does not happen today: the cut guards that invariant. Were one stored by
// the latest turn, the range would stop before it, and what was written after
// it (a member's message) would wait for the next report. A call an earlier
// turn left unanswered does not hold reports back.
func TestReportToParent_StopsBeforeAPendingToolCall(t *testing.T) {
	call := func(id int64, key string, calls ...string) store.MessageWithID {
		m := store.MessageWithID{ID: id, Key: key, Message: store.Message{Role: store.RoleAssistant}}
		for _, c := range calls {
			m.ToolCalls = append(m.ToolCalls, store.ToolCall{ID: c, Name: "web_fetch"})
		}
		return m
	}
	result := func(id int64, key, callID string) store.MessageWithID {
		return store.MessageWithID{ID: id, Key: key, Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: callID, Content: "page"}}}
	}
	human := func(id int64, text string) store.MessageWithID {
		return store.MessageWithID{ID: id, Key: store.HumanMessageKey(text), Message: store.Message{Role: store.RoleUser, Content: `"` + text + `"`}}
	}
	upTo := func(st *memStore) (int64, error) {
		tc := &fakeTemporal{}
		if err := newTest(st, tc).ReportToParent(context.Background(), sid, victor); err != nil {
			return 0, err
		}
		return tc.inputs[0].(workflow.ReportToParentInput).UpTo, nil
	}

	st := forkStore()
	st.messages = append(st.messages,
		human(4, "fetch the spec"),
		call(5, "t2.0:0", "c1", "c2"),
		result(6, "t2.0:1", "c1"),
		human(7, "and the changelog"), // a member writes during the turn
	)
	if got, err := upTo(st); err != nil || got != 4 {
		t.Errorf("turn waiting on c2: up to %d, %v; want 4", got, err)
	}
	// Nothing before the call since the last report: nothing to report yet.
	st.session.LastReportedMessageID = 4
	if _, err := upTo(st); !errors.Is(err, ErrNothingToReport) {
		t.Errorf("only the pending call since the last report: %v", err)
	}
	st.session.LastReportedMessageID = 0

	// The result is in: the whole turn, and the message after it.
	st.messages = append(st.messages, result(8, "t2.0:2", "c2"))
	if got, err := upTo(st); err != nil || got != 8 {
		t.Errorf("turn answered: up to %d, %v; want 8", got, err)
	}

	// An earlier turn left c0 unanswered: it is over, nothing waits on it.
	st = forkStore()
	st.messages = append(st.messages,
		call(4, "t1.0:0", "c0"),
		human(5, "go on"),
		store.MessageWithID{ID: 6, Key: "t2.0:0", Message: store.Message{Role: store.RoleAssistant, Content: `"done"`}},
	)
	if got, err := upTo(st); err != nil || got != 6 {
		t.Errorf("an old unanswered call: up to %d, %v; want 6", got, err)
	}
}

// A page reads the fork's workflows once per few seconds, like the session
// states; a report started drops them, and the next page sees it running.
// The cache's clock stands still: two renders are within its TTL however
// slow the machine.
func TestReportState_WorkflowStatesShared(t *testing.T) {
	ctx := context.Background()
	st, tc := forkStore(), &fakeTemporal{}
	s := newTest(st, tc)
	at := time.Now()
	s.statuses.now = func() time.Time { return at }
	if _, err := s.ReportState(ctx, st.session, st.messages, victor); err != nil {
		t.Fatal(err)
	}
	read := tc.describes
	if _, err := s.ReportState(ctx, st.session, st.messages, victor); err != nil || tc.describes != read {
		t.Errorf("second render: %d describes, want %d; %v", tc.describes, read, err)
	}

	if err := s.ReportToParent(ctx, sid, victor); err != nil {
		t.Fatal(err)
	}
	tc.running = []string{workflow.ReportWorkflowID(sid, 0)}
	if got, _ := s.ReportState(ctx, st.session, st.messages, victor); !got.Pending {
		t.Errorf("after the click: %+v, want the report running", got)
	}

	// Past the TTL, a render reads them again.
	read = tc.describes
	at = at.Add(statusesTTL)
	if _, err := s.ReportState(ctx, st.session, st.messages, victor); err != nil || tc.describes == read {
		t.Errorf("render past the TTL: %d describes, want more than %d; %v", tc.describes, read, err)
	}
}

// While the fork's agent is on a turn, a report covers what the turn wrote so
// far; the next one starts after it, with the rest of the turn.
func TestReportToParent_DuringATurn(t *testing.T) {
	st := forkStore()
	st.messages = append(st.messages,
		store.MessageWithID{ID: 4, Key: store.HumanMessageKey("h4"), Message: store.Message{Role: store.RoleUser, Content: `"fetch the spec"`}},
		store.MessageWithID{ID: 5, Key: "t2.0:0", Message: store.Message{Role: store.RoleAssistant, ToolCalls: []store.ToolCall{{ID: "c1", Name: "web_fetch"}}}},
		store.MessageWithID{ID: 6, Key: "t2.0:1", Message: store.Message{Role: store.RoleTool, ToolResult: &store.ToolResult{ToolCallID: "c1", Content: "spec"}}},
	)
	working := func() *fakeTemporal {
		return &fakeTemporal{byType: map[string][]string{"AgentWorkflow": {sid + "-turn-2"}}}
	}
	tc := working()
	if err := newTest(st, tc).ReportToParent(context.Background(), sid, victor); err != nil {
		t.Fatalf("report during a turn: %v", err)
	}
	if in := tc.inputs[0].(workflow.ReportToParentInput); in.From != 0 || in.UpTo != 6 {
		t.Errorf("first report %+v, want up to 6", in)
	}

	// Posted; the turn goes on, and ends.
	st.session.LastReportedMessageID = 6
	st.messages = append(st.messages, store.MessageWithID{ID: 7, Key: "t2.0:2", Message: store.Message{Role: store.RoleAssistant, Content: `"the spec says CSV"`}})
	tc = working()
	if err := newTest(st, tc).ReportToParent(context.Background(), sid, victor); err != nil {
		t.Fatalf("next report: %v", err)
	}
	if in := tc.inputs[0].(workflow.ReportToParentInput); in.From != 6 || in.UpTo != 7 {
		t.Errorf("next report %+v, want 6 to 7", in)
	}
}
