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
	Status      string    `json:"status"` // "scheduled", "cancelled"
}
