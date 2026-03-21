package activity

import (
	"context"
	"encoding/json"
	"fmt"
)

// DeliveryActivities handles delivering scheduled task results to users.
type DeliveryActivities struct {
	Hub SSEHub // For app_notification delivery
}

type DeliverInput struct {
	UserID     string `json:"user_id"`
	Channel    string `json:"channel"` // "slack", "email", "app_notification"
	Content    string `json:"content"`
	ScheduleID string `json:"schedule_id"`
}

func (a *DeliveryActivities) DeliverResult(ctx context.Context, input DeliverInput) error {
	switch input.Channel {
	case "slack":
		return a.deliverSlack(ctx, input)
	case "email":
		return a.deliverEmail(ctx, input)
	case "app_notification":
		return a.deliverAppNotification(ctx, input)
	default:
		return a.deliverAppNotification(ctx, input)
	}
}

func (a *DeliveryActivities) deliverSlack(ctx context.Context, input DeliverInput) error {
	// TODO: Implement Slack delivery (webhook or API)
	// For now, fall back to app notification
	return a.deliverAppNotification(ctx, input)
}

func (a *DeliveryActivities) deliverEmail(ctx context.Context, input DeliverInput) error {
	// TODO: Implement email delivery
	// For now, fall back to app notification
	return a.deliverAppNotification(ctx, input)
}

func (a *DeliveryActivities) deliverAppNotification(ctx context.Context, input DeliverInput) error {
	if a.Hub == nil {
		return fmt.Errorf("no notification hub configured")
	}

	data, _ := json.Marshal(map[string]string{
		"type":        "scheduled_task_result",
		"schedule_id": input.ScheduleID,
		"content":     input.Content,
	})

	// Use the schedule ID as session ID for SSE delivery
	a.Hub.Publish(input.ScheduleID, SSEEvent{
		Type: "scheduled_task_result",
		Data: data,
	})

	return nil
}
