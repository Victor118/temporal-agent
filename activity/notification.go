package activity

import (
	"context"
	"encoding/json"
	"log"
)

// SSEHub is an interface for in-process SSE notifications.
type SSEHub interface {
	Publish(sessionID string, event SSEEvent)
}

// TelegramSender sends messages to Telegram.
type TelegramSender interface {
	SendMessage(chatID, text string) error
}

type SSEEvent struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type NotificationActivities struct {
	Hub      SSEHub
	Telegram TelegramSender
}

type NotifyInput struct {
	SessionID string   `json:"session_id"`
	Event     SSEEvent `json:"event"`
	Channel   string   `json:"channel,omitempty"`    // "web", "telegram"
	ChannelID string   `json:"channel_id,omitempty"` // chat_id for telegram
}

func (a *NotificationActivities) NotifyStep(ctx context.Context, input NotifyInput) error {
	switch input.Channel {
	case "telegram":
		if a.Telegram == nil {
			log.Printf("Warning: telegram notification skipped (no client configured)")
			return nil
		}

		switch input.Event.Type {
		case "message":
			var data struct {
				Content string `json:"content"`
			}
			if err := json.Unmarshal(input.Event.Data, &data); err != nil || data.Content == "" {
				return nil
			}
			return a.Telegram.SendMessage(input.ChannelID, data.Content)

		case "ask_user":
			var data struct {
				Question string `json:"question"`
			}
			if err := json.Unmarshal(input.Event.Data, &data); err != nil || data.Question == "" {
				return nil
			}
			return a.Telegram.SendMessage(input.ChannelID, "❓ "+data.Question)

		default:
			// Ignore tool_calls and other streaming events for Telegram
			return nil
		}

	default: // "web" or empty
		if a.Hub != nil {
			a.Hub.Publish(input.SessionID, input.Event)
		}
		return nil
	}
}
