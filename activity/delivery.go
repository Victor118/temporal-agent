package activity

import (
	"context"
	"fmt"

	"github.com/victor/temporal-agent/store"
)

// MessageAppender stores one message under an idempotency key.
type MessageAppender interface {
	AppendMessage(ctx context.Context, sessionID, key string, msg store.Message) error
}

// DeliveryActivities handles delivering scheduled task results to users.
type DeliveryActivities struct {
	// Web is the web channel's notifier, which shows the result live.
	Web   Notifier
	Store MessageAppender
}

type DeliverInput struct {
	UserID     string `json:"user_id"`
	Content    string `json:"content"`
	ScheduleID string `json:"schedule_id"`
	// RunUnixMilli identifies this run of the schedule. It keys the stored
	// message, so a retried delivery dedupes while a later cron run does not.
	RunUnixMilli int64 `json:"run_unix_milli"`
}

func (a *DeliveryActivities) DeliverResult(ctx context.Context, input DeliverInput) error {
	if a.Web == nil {
		return fmt.Errorf("no web notifier configured")
	}

	sessionID := fmt.Sprintf("notifications:%s", input.UserID)

	// Persist as a message in the user's notification session
	msg := store.Message{
		Role:    store.RoleAssistant,
		Content: input.Content,
	}
	key := store.ScheduledMessageKey(input.ScheduleID, input.RunUnixMilli)
	if err := a.Store.AppendMessage(ctx, sessionID, key, msg); err != nil {
		return fmt.Errorf("persist notification: %w", err)
	}

	// Show it live. A failure fails the activity, and the retry stores
	// nothing twice: the message is keyed.
	return a.Web.Notify(ctx, Notification{SessionID: sessionID, Event: SSEEvent{
		Type: "notification",
		Data: []byte(input.Content),
	}})
}
