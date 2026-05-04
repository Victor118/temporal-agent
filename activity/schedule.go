package activity

import (
	"context"
	"log"

	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/store"
)

// ScheduleActivities handles Temporal Schedule lifecycle operations.
type ScheduleActivities struct {
	Client client.Client
	Store  store.Store
}

type DeleteScheduleInput struct {
	ScheduleID string `json:"schedule_id"`
}

func (a *ScheduleActivities) DeleteSchedule(ctx context.Context, input DeleteScheduleInput) error {
	handle := a.Client.ScheduleClient().GetHandle(ctx, input.ScheduleID)
	if err := handle.Delete(ctx); err != nil {
		log.Printf("Warning: failed to delete schedule %s: %v", input.ScheduleID, err)
		// Don't fail the workflow for cleanup errors
		return nil
	}
	a.Store.UpdateTaskLogStatus(ctx, input.ScheduleID, "completed")
	return nil
}
