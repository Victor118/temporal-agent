package activity

import (
	"context"

	"github.com/victor/temporal-agent/store"
)

// ConversationStore is what the memory activities need of the store: how to
// append a turn's messages.
type ConversationStore interface {
	AppendMessages(ctx context.Context, sessionID, turnKey string, startIndex int, messages []store.Message) error
}

type MemoryActivities struct {
	Store ConversationStore
}

// PersistContextInput appends the messages a turn produced. Messages holds the
// slice starting at StartIndex within the turn, so the agent can flush as it
// goes and its participant can re-flush the whole turn at the end: both write
// the same keys, and the second write is a no-op.
type PersistContextInput struct {
	SessionID  string
	TurnKey    string
	StartIndex int
	Messages   []store.Message
}

func (a *MemoryActivities) PersistContext(ctx context.Context, input PersistContextInput) error {
	return a.Store.AppendMessages(ctx, input.SessionID, input.TurnKey, input.StartIndex, input.Messages)
}
