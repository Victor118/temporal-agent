package activity

import (
	"context"
	"fmt"

	"github.com/victor/temporal-agent/store"
)

// DeliveryActivities handles delivering scheduled task results to users.
type DeliveryActivities struct {
	Hub   SSEHub
	Store store.Store
}

type DeliverInput struct {
	UserID     string `json:"user_id"`
	Content    string `json:"content"`
	ScheduleID string `json:"schedule_id"`
}

func (a *DeliveryActivities) DeliverResult(ctx context.Context, input DeliverInput) error {
	if a.Hub == nil {
		return fmt.Errorf("no notification hub configured")
	}

	sessionID := fmt.Sprintf("notifications:%s", input.UserID)

	// Persist as a message in the user's notification session
	msg := store.Message{
		Role:    store.RoleAssistant,
		Content: input.Content,
	}
	if err := a.Store.AppendMessage(ctx, sessionID, msg); err != nil {
		return fmt.Errorf("persist notification: %w", err)
	}

	// Publish live via SSE
	a.Hub.Publish(sessionID, SSEEvent{
		Type: "notification",
		Data: []byte(input.Content),
	})

	return nil
}
