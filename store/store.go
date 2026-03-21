package store

import "context"

type MemoryScope string

const (
	MemoryScopeUser    MemoryScope = "user"
	MemoryScopeProject MemoryScope = "project"
	MemoryScopeSession MemoryScope = "session"
)

type Store interface {
	// Session messages
	LoadMessages(ctx context.Context, sessionID string) ([]Message, error)
	SaveMessages(ctx context.Context, sessionID string, messages []Message) error

	// Agent memory
	LoadMemory(ctx context.Context, scope MemoryScope, scopeID string) (string, error)
	SaveMemory(ctx context.Context, scope MemoryScope, scopeID string, content string) error

	// Task logs (scheduled tasks)
	SaveTaskLog(ctx context.Context, log TaskLog) error
	ListTaskLogs(ctx context.Context) ([]TaskLog, error)
	UpdateTaskLogStatus(ctx context.Context, scheduleID, status string) error

	// Lifecycle
	Close() error
}
