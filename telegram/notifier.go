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
	case activity.EventMessage:
		var data struct {
			Content string `json:"content"`
			// Agent signs an answer that could be taken for another agent's:
			// several agents answer in the session.
			Agent string `json:"agent"`
		}
		if err := json.Unmarshal(note.Event.Data, &data); err != nil || data.Content == "" {
			return nil
		}
		return n.Client.SendMessage(ctx, note.ChannelID, signed(data.Agent, data.Content))

	case activity.EventAskUser:
		var data struct {
			Question string `json:"question"`
			Agent    string `json:"agent"` // signed as the answer is
		}
		if err := json.Unmarshal(note.Event.Data, &data); err != nil || data.Question == "" {
			return nil
		}
		return n.Client.SendMessage(ctx, note.ChannelID, signed(data.Agent, "❓ "+data.Question))

	default:
		return nil
	}
}

// signed starts text with the agent's name, when there is one.
func signed(agent, text string) string {
	if agent == "" {
		return text
	}
	return agent + " :\n" + text
}
