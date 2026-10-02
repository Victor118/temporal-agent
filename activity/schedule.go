package activity

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"
)

// TaskLogUpdater records where a scheduled task stands.
type TaskLogUpdater interface {
	UpdateTaskLogStatus(ctx context.Context, scheduleID, status string) error
}

// ScheduleHandles finds a schedule to act on: what the schedule activities
// need of Temporal's schedule client.
type ScheduleHandles interface {
	GetHandle(ctx context.Context, scheduleID string) client.ScheduleHandle
}

// ScheduleActivities handles Temporal Schedule lifecycle operations.
type ScheduleActivities struct {
	Client ScheduleHandles
	Store  TaskLogUpdater
}

type DeleteScheduleInput struct {
	ScheduleID string `json:"schedule_id"`
}

func (a *ScheduleActivities) DeleteSchedule(ctx context.Context, input DeleteScheduleInput) error {
	handle := a.Client.GetHandle(ctx, input.ScheduleID)
	if err := handle.Delete(ctx); err != nil {
		log.Printf("Warning: failed to delete schedule %s: %v", input.ScheduleID, err)
		// Don't fail the workflow for cleanup errors
		return nil
	}
	a.Store.UpdateTaskLogStatus(ctx, input.ScheduleID, "completed")
	return nil
}
