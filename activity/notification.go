package activity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"go.temporal.io/sdk/temporal"
)

// SSEHub is an interface for in-process SSE notifications.
type SSEHub interface {
	Publish(sessionID string, event SSEEvent)
}

type SSEEvent struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// ChannelWeb is the web interface's channel, and the one a session without a
// channel uses.
const ChannelWeb = "web"

// Notification is an event for the members of a session, on one channel.
type Notification struct {
	SessionID string
	// ChannelID is the user's address on the channel (a Telegram chat ID);
	// empty on the web, where the session ID is the address.
	ChannelID string
	Event     SSEEvent
}

// Notifier delivers events on one channel. Each channel is one
// implementation, registered under its name when the worker starts: adding a
// channel adds a Notifier, and touches neither this package nor the
// workflows, which only carry the channel's name.
type Notifier interface {
	Notify(ctx context.Context, n Notification) error
}

// PartialDelivery is the error of a notification a channel delivered in part:
// the first Delivered of Total messages went out, the rest did not. It is
// never retried: the channel has no way to tell a message it already
// received, so a retry would deliver those parts a second time.
type PartialDelivery struct {
	Delivered, Total int
	Err              error
}

func (e *PartialDelivery) Error() string {
	return fmt.Sprintf("delivered %d of %d parts: %v", e.Delivered, e.Total, e.Err)
}

func (e *PartialDelivery) Unwrap() error { return e.Err }

// HubNotifier is the web channel: it publishes on the SSE hub — the
// server's own in dev mode, or the server's through an HTTPNotifier.
type HubNotifier struct {
	Hub SSEHub
}

func (h HubNotifier) Notify(_ context.Context, n Notification) error {
	h.Hub.Publish(n.SessionID, n.Event)
	return nil
}

type NotificationActivities struct {
	// Notifiers by channel name. An empty channel is the web.
	Notifiers map[string]Notifier
}

type NotifyInput struct {
	SessionID string   `json:"session_id"`
	Event     SSEEvent `json:"event"`
	Channel   string   `json:"channel,omitempty"`    // "web", "telegram"
	ChannelID string   `json:"channel_id,omitempty"` // chat_id for telegram
}

// NotifyStep sends an event on the session's channel. A channel this worker
// has no notifier for (Telegram without a bot token) is skipped, not an error:
// retrying would not make one appear.
func (a *NotificationActivities) NotifyStep(ctx context.Context, input NotifyInput) error {
	channel := input.Channel
	if channel == "" {
		channel = ChannelWeb
	}
	n, ok := a.Notifiers[channel]
	if !ok {
		log.Printf("Warning: %s notification skipped (no notifier for this channel)", channel)
		return nil
	}
	err := n.Notify(ctx, Notification{SessionID: input.SessionID, ChannelID: input.ChannelID, Event: input.Event})
	var partial *PartialDelivery
	if errors.As(err, &partial) {
		return temporal.NewNonRetryableApplicationError(partial.Error(), "PartialDelivery", err)
	}
	return err
}
