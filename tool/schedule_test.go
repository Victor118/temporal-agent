package tool

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/victor/temporal-agent/store"
)

type fakeScheduleStore struct {
	logs map[string]store.TaskLog
}

func (f *fakeScheduleStore) SaveTaskLog(_ context.Context, l store.TaskLog) error {
	f.logs[l.ScheduleID] = l
	return nil
}

func (f *fakeScheduleStore) ListTaskLogsByUser(_ context.Context, userID string) ([]store.TaskLog, error) {
	var out []store.TaskLog
	for _, l := range f.logs {
		if l.UserID == userID {
			out = append(out, l)
		}
	}
	return out, nil
}

func (f *fakeScheduleStore) GetTaskLog(_ context.Context, id string) (*store.TaskLog, error) {
	if l, ok := f.logs[id]; ok {
		return &l, nil
	}
	return nil, nil
}

func (f *fakeScheduleStore) UpdateTaskLogStatus(_ context.Context, id, status string) error {
	l := f.logs[id]
	l.Status = status
	f.logs[id] = l
	return nil
}

type fakeScheduler struct {
	created []string
	deleted []string
}

func (f *fakeScheduler) Create(_ context.Context, o client.ScheduleOptions) (client.ScheduleHandle, error) {
	f.created = append(f.created, o.ID)
	return &fakeHandle{id: o.ID, s: f}, nil
}

func (f *fakeScheduler) GetHandle(_ context.Context, id string) client.ScheduleHandle {
	return &fakeHandle{id: id, s: f}
}

type fakeHandle struct {
	client.ScheduleHandle
	id string
	s  *fakeScheduler
}

func (h *fakeHandle) Delete(context.Context) error {
	h.s.deleted = append(h.s.deleted, h.id)
	return nil
}

func scheduleTest(t *testing.T) (*Registry, *fakeScheduleStore, *fakeScheduler) {
	t.Helper()
	st := &fakeScheduleStore{logs: map[string]store.TaskLog{
		"schedule-alice": {ScheduleID: "schedule-alice", UserID: "u-alice", Description: "alice's reminder", Status: "scheduled"},
		"schedule-bob":   {ScheduleID: "schedule-bob", UserID: "u-bob", Description: "bob's secret plan", Status: "scheduled"},
	}}
	sc := &fakeScheduler{}
	r := NewRegistry()
	RegisterScheduleTools(r, sc, st, "ScheduledAgentWorkflow", "agent")
	return r, st, sc
}

func runAs(t *testing.T, r *Registry, userID, name string, params any) string {
	t.Helper()
	input, _ := json.Marshal(params)
	ctx := context.Background()
	if userID != "" {
		ctx = WithUserID(ctx, userID)
	}
	out, err := r.Execute(ctx, name, input)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return out
}

// A user sees their own tasks, never anyone else's.
func TestListSchedules_OwnOnly(t *testing.T) {
	r, _, _ := scheduleTest(t)
	out := runAs(t, r, "u-alice", "list_schedules", map[string]any{})
	if !strings.Contains(out, "alice's reminder") || strings.Contains(out, "bob") {
		t.Errorf("alice's list: %q", out)
	}
	if out := runAs(t, r, "", "list_schedules", map[string]any{}); out != noUser {
		t.Errorf("no user: %q", out)
	}
}

func TestCancelSchedule_OwnOnly(t *testing.T) {
	r, st, sc := scheduleTest(t)

	out := runAs(t, r, "u-alice", "cancel_schedule", map[string]string{"schedule_id": "schedule-bob"})
	if !strings.Contains(out, "No scheduled task") || len(sc.deleted) != 0 || st.logs["schedule-bob"].Status != "scheduled" {
		t.Errorf("alice cancelling bob's task: %q, deleted %v", out, sc.deleted)
	}
	runAs(t, r, "u-alice", "cancel_schedule", map[string]string{"schedule_id": "nope"})
	if len(sc.deleted) != 0 {
		t.Errorf("an unknown task was deleted: %v", sc.deleted)
	}

	out = runAs(t, r, "u-alice", "cancel_schedule", map[string]string{"schedule_id": "schedule-alice"})
	if !strings.Contains(out, "cancelled") || len(sc.deleted) != 1 || st.logs["schedule-alice"].Status != "cancelled" {
		t.Errorf("alice cancelling her task: %q, deleted %v", out, sc.deleted)
	}
}

// A new task is recorded as its owner's.
func TestScheduleTask_RecordsTheOwner(t *testing.T) {
	r, st, sc := scheduleTest(t)
	runAs(t, r, "u-carol", "schedule_task", map[string]string{"prompt": "check the news", "description": "news", "delay": "1h"})
	if len(sc.created) != 1 {
		t.Fatalf("created %v", sc.created)
	}
	if l := st.logs[sc.created[0]]; l.UserID != "u-carol" {
		t.Errorf("recorded %+v", l)
	}
	if out := runAs(t, r, "", "schedule_task", map[string]string{"prompt": "x", "description": "x", "delay": "1h"}); out != noUser || len(sc.created) != 1 {
		t.Errorf("no user: %q, created %v", out, sc.created)
	}
}

type fakeQuerier struct{ queried []string }

func (f *fakeQuerier) QueryWorkflow(_ context.Context, id, _, _ string, _ ...interface{}) (converter.EncodedValue, error) {
	f.queried = append(f.queried, id)
	return nil, context.Canceled
}

// query_workflow reaches the workflows of the calling session only.
func TestQueryWorkflow_OwnSessionOnly(t *testing.T) {
	q := &fakeQuerier{}
	r := NewRegistry()
	RegisterQueryWorkflowTool(r, q)
	for _, caller := range []string{"s1", "s1:p:jarvis:m3:tool:agent_analyst:c1"} { // a turn's, a sub-agent's
		ctx := WithSessionID(context.Background(), caller)
		for id, allowed := range map[string]bool{
			"s1:p:jarvis:m3:tool:implement_feature:abc": true,
			"s1:p:smith":                     true,
			"s2:p:jarvis:m1:tool:ask_user:x": false,
			"s1x:p:a":                        false,
			"s1-tool-a":                      false,
			"schedule-bob":                   false,
		} {
			q.queried = nil
			input, _ := json.Marshal(map[string]string{"workflow_id": id})
			out, _ := r.Execute(ctx, "query_workflow", input)
			if got := len(q.queried) == 1; got != allowed {
				t.Errorf("caller %s, %s: queried %v (%q), want %v", caller, id, got, out, allowed)
			}
		}
	}
	input, _ := json.Marshal(map[string]string{"workflow_id": "s1:p:x"})
	q.queried = nil
	r.Execute(context.Background(), "query_workflow", input)
	if len(q.queried) != 0 {
		t.Error("queried without a session")
	}
}
