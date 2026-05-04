package activity

import (
	"context"

	"github.com/victor/temporal-agent/store"
)

type MemoryActivities struct {
	Store store.Store
}

type LoadContextInput struct {
	SessionID string
	UserID    string
}

type LoadContextOutput struct {
	Messages   []store.Message
	UserMemory string
}

func (a *MemoryActivities) LoadContext(ctx context.Context, input LoadContextInput) (LoadContextOutput, error) {
	messages, err := a.Store.LoadMessages(ctx, input.SessionID)
	if err != nil {
		return LoadContextOutput{}, err
	}

	var userMemory string
	if input.UserID != "" {
		userMemory, _ = a.Store.LoadMemory(ctx, store.MemoryScopeUser, input.UserID)
	}

	return LoadContextOutput{Messages: messages, UserMemory: userMemory}, nil
}

type PersistContextInput struct {
	SessionID string
	Messages  []store.Message
}

func (a *MemoryActivities) PersistContext(ctx context.Context, input PersistContextInput) error {
	return a.Store.SaveMessages(ctx, input.SessionID, input.Messages)
}

type LoadMemoryInput struct {
	Scope   store.MemoryScope
	ScopeID string
}

func (a *MemoryActivities) LoadMemory(ctx context.Context, input LoadMemoryInput) (string, error) {
	return a.Store.LoadMemory(ctx, input.Scope, input.ScopeID)
}

type SaveMemoryInput struct {
	Scope   store.MemoryScope
	ScopeID string
	Content string
}

func (a *MemoryActivities) SaveMemory(ctx context.Context, input SaveMemoryInput) error {
	return a.Store.SaveMemory(ctx, input.Scope, input.ScopeID, input.Content)
}
