package activity

import (
	"context"
	"testing"

	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/store"
)

// deletableSchedule is a schedule whose deletion succeeds.
type deletableSchedule struct{ client.ScheduleHandle }

func (deletableSchedule) Delete(context.Context) error { return nil }

type scheduleHandles struct{}

func (scheduleHandles) GetHandle(context.Context, string) client.ScheduleHandle {
	return deletableSchedule{}
}

// statusLog records the last status written.
type statusLog struct{ status string }

func (l *statusLog) UpdateTaskLogStatus(_ context.Context, _, status string) error {
	l.status = status
	return nil
}

// The task log is closed with the status the workflow chose; an input from a
// worker that predates the status closes it as completed, as it did then.
func TestDeleteSchedule_ClosesTheTaskLog(t *testing.T) {
	for in, want := range map[string]string{
		store.TaskFailed:    store.TaskFailed,
		store.TaskCompleted: store.TaskCompleted,
		"":                  store.TaskCompleted,
	} {
		log := &statusLog{}
		a := &ScheduleActivities{Client: scheduleHandles{}, Store: log}
		if err := a.DeleteSchedule(context.Background(), DeleteScheduleInput{ScheduleID: "s", Status: in}); err != nil {
			t.Fatal(err)
		}
		if log.status != want {
			t.Errorf("status %q closed the log as %q, want %q", in, log.status, want)
		}
	}
}
