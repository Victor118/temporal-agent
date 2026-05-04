package store

import "context"

type MemoryScope string

const (
	MemoryScopeUser    MemoryScope = "user"
	MemoryScopeProject MemoryScope = "project"
	MemoryScopeSession MemoryScope = "session"
)

type Store interface {
	// Users
	GetUserByTelegramID(ctx context.Context, telegramID int64) (*User, error)

	// Sessions
	CreateSession(ctx context.Context, session Session) error
	GetSession(ctx context.Context, sessionID string) (*Session, error)
	GetSessionUser(ctx context.Context, sessionID string) (string, error)
	GetActiveSessionByChannel(ctx context.Context, userID, channel, channelID string) (*Session, error)
	ListSessionsByUser(ctx context.Context, userID string) ([]Session, error)
	UpdateSessionTitle(ctx context.Context, sessionID, title string) error
	DeleteSession(ctx context.Context, sessionID string) error

	// Session messages
	LoadMessages(ctx context.Context, sessionID string) ([]Message, error)
	LoadMessagesWithID(ctx context.Context, sessionID string) ([]MessageWithID, error)
	SaveMessages(ctx context.Context, sessionID string, messages []Message) error
	AppendMessage(ctx context.Context, sessionID string, msg Message) error
	DeleteMessage(ctx context.Context, sessionID string, id int64) error
	DeleteMessagesBySession(ctx context.Context, sessionID string) error

	// Agent memory
	LoadMemory(ctx context.Context, scope MemoryScope, scopeID string) (string, error)
	SaveMemory(ctx context.Context, scope MemoryScope, scopeID string, content string) error

	// Task logs (scheduled tasks)
	SaveTaskLog(ctx context.Context, log TaskLog) error
	ListTaskLogs(ctx context.Context) ([]TaskLog, error)
	UpdateTaskLogStatus(ctx context.Context, scheduleID, status string) error

	// Agent catalog
	UpsertAgent(ctx context.Context, taskQueue string, skills []string) error
	ListAgents(ctx context.Context) ([]AgentEntry, error)

	// Skills version
	GetSkillsVersion(ctx context.Context) (int64, error)
	IncrementSkillsVersion(ctx context.Context) (int64, error)

	// Activity queue mapping
	GetActivityQueueMap(ctx context.Context) (map[string]string, error)
	SetActivityQueue(ctx context.Context, activityName, taskQueue string) error
	DeleteActivityQueue(ctx context.Context, activityName string) error
	ListActivityQueues(ctx context.Context) ([]ActivityQueueEntry, error)

	// Lifecycle
	Close() error
}

type ActivityQueueEntry struct {
	ActivityName string `json:"activity_name"`
	TaskQueue    string `json:"task_queue"`
}

type AgentEntry struct {
	TaskQueue string   `json:"task_queue"`
	Skills    []string `json:"skills"`
}
