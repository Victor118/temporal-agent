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
			ForkedAtMessageID: 7, ForkedBy: victor.ID, ForkPurpose: "CSV export"},
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
			st.messages = st.messages[1:]
			tc.running = []string{workflow.ForkWorkflowID(sid)}
		}, ErrSummaryPending},
		"nothing new": {func(st *memStore, _ *fakeTemporal) {
			st.session.LastReportedMessageID = 3
			st.messages = append(st.messages, store.MessageWithID{ID: 4, Message: store.Message{Role: store.RoleAssistant, Kind: store.KindTurnError, Content: `"boom"`}})
		}, ErrNothingToReport},
		"only the brief": {func(st *memStore, _ *fakeTemporal) { st.messages = st.messages[:1] }, ErrNothingToReport},
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
	got, err := newTest(st, tc).ReportState(ctx, st.session, victor)
	if err != nil || !got.CanReport() || got.Pending || got.Failed || got.LastReportID != 12 || got.LastReportedAt != &at || got.ParentSessionID != parentID {
		t.Errorf("ready: %+v, %v", got, err)
	}

	tc.running = []string{workflow.ReportWorkflowID(sid, 0)}
	if got, _ := newTest(st, tc).ReportState(ctx, st.session, victor); !got.Pending || got.CanReport() {
		t.Errorf("running: %+v", got)
	}

	tc = &fakeTemporal{closed: map[string]enumspb.WorkflowExecutionStatus{workflow.ReportWorkflowID(sid, 0): enumspb.WORKFLOW_EXECUTION_STATUS_FAILED}}
	if got, _ := newTest(st, tc).ReportState(ctx, st.session, victor); !got.Failed || !got.CanReport() {
		t.Errorf("failed: %+v, want a retry allowed", got)
	}
	// Posted: the next report has another ID, and nothing is new yet.
	st.session.LastReportedMessageID = 3
	if got, _ := newTest(st, tc).ReportState(ctx, st.session, victor); got.Failed || !got.NothingNew || got.CanReport() {
		t.Errorf("after a report: %+v", got)
	}

	st = forkStore()
	st.outsiders = map[string][]string{parentID: {victor.ID}}
	if got, err := newTest(st, &fakeTemporal{}).ReportState(ctx, st.session, victor); err != nil || !errors.Is(got.Refused, ErrNotParentMember) || got.CanReport() {
		t.Errorf("not a member of the parent: %+v, %v", got, err)
	}
	st.session.ParentSessionID = ""
	if got, _ := newTest(st, &fakeTemporal{}).ReportState(ctx, st.session, victor); !errors.Is(got.Refused, ErrNoParent) {
		t.Errorf("parent deleted: %+v", got)
	}
	st.messages = st.messages[1:]
	st.session.ParentSessionID = parentID
	st.outsiders = nil
	if got, _ := newTest(st, &fakeTemporal{running: []string{workflow.ForkWorkflowID(sid)}}).ReportState(ctx, st.session, victor); !got.SummaryPending || got.CanReport() {
		t.Errorf("brief pending: %+v", got)
	}
	if _, err := newTest(st, &fakeTemporal{}).ReportState(ctx, &store.Session{SessionID: parentID}, victor); !errors.Is(err, ErrNotAFork) {
		t.Errorf("not a fork: %v", err)
	}
}
