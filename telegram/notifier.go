package telegram

import (
	"context"
	"encoding/json"

	"github.com/victor/temporal-agent/activity"
)

// Sender sends a text to a chat.
type Sender interface {
	SendMessage(ctx context.Context, chatID, text string) error
}

// Notifier is the Telegram channel's side of the notifications: the agent's
// answers and its questions become messages in the user's chat. The stream of
// tool calls, and the rest, stays on the web.
type Notifier struct {
	Client Sender
}

func (n *Notifier) Notify(ctx context.Context, note activity.Notification) error {
	switch note.Event.Type {
	case "message":
		var data struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(note.Event.Data, &data); err != nil || data.Content == "" {
			return nil
		}
		return n.Client.SendMessage(ctx, note.ChannelID, data.Content)

	case "ask_user":
		var data struct {
			Question string `json:"question"`
		}
		if err := json.Unmarshal(note.Event.Data, &data); err != nil || data.Question == "" {
			return nil
		}
		return n.Client.SendMessage(ctx, note.ChannelID, "❓ "+data.Question)

	default:
		return nil
	}
}
