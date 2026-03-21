package activity

import (
	"context"
	"encoding/json"
)

// SSEHub is an interface for in-process SSE notifications.
type SSEHub interface {
	Publish(sessionID string, event SSEEvent)
}

type SSEEvent struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type NotificationActivities struct {
	Hub SSEHub
}

type NotifyInput struct {
	SessionID string   `json:"session_id"`
	Event     SSEEvent `json:"event"`
}

func (a *NotificationActivities) NotifyStep(ctx context.Context, input NotifyInput) error {
	if a.Hub != nil {
		a.Hub.Publish(input.SessionID, input.Event)
	}
	return nil
}
