package store

import "time"

type TaskLog struct {
	ScheduleID  string    `json:"schedule_id"`
	Type        string    `json:"type"` // "schedule"
	Description string    `json:"description"`
	Cron        string    `json:"cron,omitempty"`
	Delay       string    `json:"delay,omitempty"`
	Prompt      string    `json:"prompt"`
	UserID      string    `json:"user_id,omitempty"`
	Channel     string    `json:"channel,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	Status      string    `json:"status"` // a Task* status
}

// Where a scheduled task stands. A one-shot fires once and ends completed or
// failed; a recurring task stays scheduled until it is cancelled.
const (
	TaskScheduled = "scheduled"
	TaskCancelled = "cancelled"
	TaskCompleted = "completed" // a one-shot ran and its result was delivered
	TaskFailed    = "failed"    // a one-shot's run failed, or its result was not delivered
)
