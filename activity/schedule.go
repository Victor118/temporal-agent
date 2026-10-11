package activity

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/store"
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
	// Status closes the task log: store.TaskCompleted or store.TaskFailed.
	// Empty (scheduled by a worker from before it) = completed.
	Status string `json:"status,omitempty"`
}

func (a *ScheduleActivities) DeleteSchedule(ctx context.Context, input DeleteScheduleInput) error {
	handle := a.Client.GetHandle(ctx, input.ScheduleID)
	if err := handle.Delete(ctx); err != nil {
		log.Printf("Warning: failed to delete schedule %s: %v", input.ScheduleID, err)
		// Don't fail the workflow for cleanup errors
		return nil
	}
	status := input.Status
	if status == "" {
		status = store.TaskCompleted
	}
	a.Store.UpdateTaskLogStatus(ctx, input.ScheduleID, status)
	return nil
}

type PauseScheduleInput struct {
	ScheduleID string `json:"schedule_id"`
	// Reason is the schedule's note in Temporal: why it is paused.
	Reason string `json:"reason"`
}

// PauseSchedule pauses a recurring task whose runs are refused for good
// (its agent closed to its owner: docs/design/permissions.md §4.1), so
// that each of its next runs does not fail again; its log says paused. It
// runs again only when resumed: a right given back does not restart it.
func (a *ScheduleActivities) PauseSchedule(ctx context.Context, input PauseScheduleInput) error {
	if err := a.Client.GetHandle(ctx, input.ScheduleID).Pause(ctx, client.SchedulePauseOptions{Note: input.Reason}); err != nil {
		return err
	}
	return a.Store.UpdateTaskLogStatus(ctx, input.ScheduleID, store.TaskPaused)
}
