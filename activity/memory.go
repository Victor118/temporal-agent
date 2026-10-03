package activity

import (
	"context"

	"github.com/victor/temporal-agent/store"
)

// ConversationStore is what the memory activities need of the store: where a
// session's messages end, how to append a turn's, and a scope's memory.
type ConversationStore interface {
	LastMessageID(ctx context.Context, sessionID string) (int64, error)
	AppendMessages(ctx context.Context, sessionID, turnKey string, startIndex int, messages []store.Message) error
	SaveMemory(ctx context.Context, scope store.MemoryScope, scopeID string, content string) error
}

type MemoryActivities struct {
	Store ConversationStore
}

type LastMessageIDInput struct {
	SessionID string
}

// LastMessageID is where a session's history ends, 0 for an empty one. It is
// only the fallback snapshot (TurnHistory.UpTo) of a message the server did
// not store, so has no ID: a stored one is its own snapshot. The history
// itself never goes through the workflows, the LLM call loads it.
func (a *MemoryActivities) LastMessageID(ctx context.Context, input LastMessageIDInput) (int64, error) {
	return a.Store.LastMessageID(ctx, input.SessionID)
}

// PersistContextInput appends the messages a turn produced. Messages holds the
// slice starting at StartIndex within the turn, so the agent can flush as it
// goes and the session can re-flush the whole turn at the end: both write the
// same keys, and the second write is a no-op.
type PersistContextInput struct {
	SessionID  string
	TurnKey    string
	StartIndex int
	Messages   []store.Message
}

func (a *MemoryActivities) PersistContext(ctx context.Context, input PersistContextInput) error {
	return a.Store.AppendMessages(ctx, input.SessionID, input.TurnKey, input.StartIndex, input.Messages)
}

type SaveMemoryInput struct {
	Scope   store.MemoryScope
	ScopeID string
	Content string
}

func (a *MemoryActivities) SaveMemory(ctx context.Context, input SaveMemoryInput) error {
	return a.Store.SaveMemory(ctx, input.Scope, input.ScopeID, input.Content)
}
