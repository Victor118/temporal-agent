package store

import (
	"context"
	"encoding/json"
	"time"
)

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
	// AppendMessages appends the messages a turn produced, keyed by
	// TurnMessageKey(turnKey, startIndex+i). Re-writing a message already stored
	// is a no-op, so a replayed activity never duplicates or renumbers.
	AppendMessages(ctx context.Context, sessionID, turnKey string, startIndex int, messages []Message) error
	// AppendMessage appends one message under an explicit idempotency key.
	AppendMessage(ctx context.Context, sessionID, key string, msg Message) error
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
	UpsertAgent(ctx context.Context, agent Agent) error
	InsertAgentIfAbsent(ctx context.Context, agent Agent) (bool, error)
	ListAgents(ctx context.Context) ([]Agent, error)
	GetAgent(ctx context.Context, agentID string) (*Agent, error)

	// Tool catalog (published by workers)
	UpsertTool(ctx context.Context, tool ToolRecord) error
	ListTools(ctx context.Context) ([]ToolRecord, error)

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

type Agent struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Skills      []string `json:"skills"`
	Tools       []string `json:"tools"` // Allowed tool name globs; nil = no allowlist (all tools)
}

// ToolRecord is a tool published by a worker: where it runs and its contract.
type ToolRecord struct {
	Name          string          `json:"name"`
	TaskQueue     string          `json:"task_queue"`
	Description   string          `json:"description"`
	InputSchema   json.RawMessage `json:"input_schema"`
	Kind          string          `json:"kind"`
	WorkflowName  string          `json:"workflow_name,omitempty"`
	FireAndForget bool            `json:"fire_and_forget,omitempty"`
	SchemaHash    string          `json:"schema_hash"`
	UpdatedAt     time.Time       `json:"updated_at"`
}
