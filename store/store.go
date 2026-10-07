package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type MemoryScope string

const (
	MemoryScopeUser    MemoryScope = "user"
	MemoryScopeProject MemoryScope = "project"
	MemoryScopeSession MemoryScope = "session"
)

// Memory is a scope's memory and its version, bumped by every save: 0, with
// no content, while none was saved.
type Memory struct {
	Content string `json:"content"`
	Version int64  `json:"version"`
}

// ErrMemoryConflict is the error of a memory save made from a version another
// save replaced since: nothing was written. It does not carry the memory: a
// user's memory stays out of errors, which are logged and returned.
var ErrMemoryConflict = errors.New("memory changed since it was read")

type Store interface {
	// Users
	CreateUser(ctx context.Context, u User) error
	GetUser(ctx context.Context, id string) (*User, error)
	GetUserByEmail(ctx context.Context, email string) (*User, error)
	GetUserByTelegramID(ctx context.Context, telegramID int64) (*User, error)
	ListUsers(ctx context.Context) ([]User, error)
	UpdateUser(ctx context.Context, u User) error
	SetUserPassword(ctx context.Context, id, passwordHash string) error
	SetUserDisabled(ctx context.Context, id string, disabled bool) error

	// Login sessions
	CreateLoginSession(ctx context.Context, tokenHash, userID string, expiresAt time.Time) error
	GetLoginSessionUser(ctx context.Context, tokenHash string) (*User, error)
	DeleteLoginSession(ctx context.Context, tokenHash string) error

	// Sessions and their members
	CreateSession(ctx context.Context, session Session) error
	GetSession(ctx context.Context, sessionID string) (*Session, error)
	GetActiveSessionByChannel(ctx context.Context, userID, channel, channelID string) (*Session, error)
	ListSessionsByUser(ctx context.Context, userID string) ([]Session, error)
	ListSessionStats(ctx context.Context, userID string) (map[string]SessionStats, error)
	UpdateSessionTitle(ctx context.Context, sessionID, title string) error
	DeleteSession(ctx context.Context, sessionID string) error
	IsSessionMember(ctx context.Context, sessionID, userID string) (bool, error)
	SessionMembership(ctx context.Context, sessionID, userID string) (members int, isMember bool, err error)
	ListForks(ctx context.Context, sessionID, userID string) ([]Session, error)
	SetSessionAgentMode(ctx context.Context, sessionID, mode string) error
	ListSessionMembers(ctx context.Context, sessionID string) ([]SessionMember, error)
	AddSessionMember(ctx context.Context, sessionID, userID, addedBy string) error
	RemoveSessionMember(ctx context.Context, sessionID, userID string) error

	// Session messages
	LoadMessages(ctx context.Context, sessionID string) ([]Message, error)
	LoadMessagesWithID(ctx context.Context, sessionID string) ([]MessageWithID, error)
	LoadMessagesUpTo(ctx context.Context, sessionID string, lastID int64) ([]MessageWithID, error)
	LoadConversation(ctx context.Context, sessionID string, scope TurnScope) ([]MessageWithID, error)
	// AppendMessages appends the messages a turn produced, keyed by
	// TurnMessageKey(turnKey, startIndex+i). Re-writing a message already stored
	// is a no-op, so a replayed activity never duplicates or renumbers.
	AppendMessages(ctx context.Context, sessionID, turnKey string, startIndex int, messages []Message) error
	// AppendMessage appends one message under an explicit idempotency key,
	// and returns its ID (the stored one, when the key was already written).
	AppendMessage(ctx context.Context, sessionID, key string, msg Message) (int64, error)
	// AppendTurnEnd writes a turn's end under TurnEndKey, after what the turn
	// wrote, and returns its ID; HasTurnEnd tells whether it is there.
	AppendTurnEnd(ctx context.Context, sessionID, turnKey string, msg Message) (int64, error)
	HasTurnEnd(ctx context.Context, sessionID, turnKey string) (bool, error)
	// AppendForkReport posts a fork's report into its parent and records it
	// on the fork, in one transaction (see ForkReport).
	AppendForkReport(ctx context.Context, r ForkReport) (int64, error)
	// AppendForkSummary posts a fork's summary and records it on the fork,
	// in one transaction.
	AppendForkSummary(ctx context.Context, forkID string, msg Message) (int64, error)
	DeleteMessage(ctx context.Context, sessionID string, id int64) error
	DeleteMessagesBySession(ctx context.Context, sessionID string) error

	// Files agents published in sessions
	FileStore

	// Agent memory. Every write is conditional (SaveMemory): there is no
	// last-write-wins path.
	LoadMemory(ctx context.Context, scope MemoryScope, scopeID string) (Memory, error)
	SaveMemory(ctx context.Context, scope MemoryScope, scopeID string, content string, expected int64) (int64, error)

	// Background tasks (background_task.go)
	RegisterTask(ctx context.Context, t BackgroundTask, max int) error
	DropTask(ctx context.Context, id string) error
	GetTask(ctx context.Context, id string) (*BackgroundTask, error)
	ListRunningTasks(ctx context.Context, sessionID, participant string) ([]BackgroundTask, error)
	ListTasksRunningSince(ctx context.Context, before time.Time) ([]BackgroundTask, error)
	AddTaskFollowUp(ctx context.Context, sessionID, participant, id string, f TaskFollowUp, max int) error
	SetTaskCancelledBy(ctx context.Context, id, name string) error
	EndTask(ctx context.Context, id, by, state string, build func(BackgroundTask) Message) (TaskEnding, error)
	ListTasksToWake(ctx context.Context, endedBefore time.Time) ([]BackgroundTask, error)
	ListTasksToCancel(ctx context.Context) ([]BackgroundTask, error)
	SetTaskWoken(ctx context.Context, id string) error
	SetTaskCancelSent(ctx context.Context, id string) error

	// Task logs (scheduled tasks)
	SaveTaskLog(ctx context.Context, log TaskLog) error
	ListTaskLogsByUser(ctx context.Context, userID string) ([]TaskLog, error)
	GetTaskLog(ctx context.Context, scheduleID string) (*TaskLog, error)
	UpdateTaskLogStatus(ctx context.Context, scheduleID, status string) error

	// Agent catalog
	InsertAgentIfAbsent(ctx context.Context, agent Agent) (bool, error)
	ListAgents(ctx context.Context) ([]Agent, error)
	GetAgent(ctx context.Context, agentID string) (*Agent, error)
	CreateAgent(ctx context.Context, agent Agent) error
	UpdateAgent(ctx context.Context, agent Agent, expectedRevision int64) (int64, error)
	DeleteAgent(ctx context.Context, agentID string) error
	CountSessionsByAgent(ctx context.Context) (map[string]int, error)

	// Tool catalog (published by workers)
	UpsertTool(ctx context.Context, tool ToolRecord) error
	ListTools(ctx context.Context) ([]ToolRecord, error)
	DeleteTool(ctx context.Context, name, taskQueue string) (bool, error)

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

var (
	ErrAgentExists   = errors.New("agent already exists")
	ErrMentionTaken  = errors.New("mention already used by another agent")
	ErrAgentNotFound = errors.New("agent not found")
	ErrAgentConflict = errors.New("agent changed since it was read")
)

type Agent struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	// Mention is what members write to call the agent (@jarvis); empty = the
	// ID (MentionName). Unique, case aside.
	Mention     string   `json:"mention"`
	Description string   `json:"description"`
	Skills      []string `json:"skills"`
	Tools       []string `json:"tools"` // Allowed tool name globs; empty = no tool, "*" = all
	// LLMOnMachine says where its turns' model runs (LLMOnMachine*): the
	// server's key, or the machine of the turn's author.
	LLMOnMachine string    `json:"llm_on_machine"`
	Revision     int64     `json:"revision"` // bumped on every update
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// Where an agent's turns call their model (docs/design/machine-llm.md §3):
// never on a machine, the server's key (the default); on the machine of the
// turn's author when it offers its model, else the server's key; or there
// only, the turn stopping when no machine of the author's offers one.
const (
	LLMOnMachineNever   = "never"
	LLMOnMachinePrefer  = "prefer"
	LLMOnMachineRequire = "require"
)

// ValidLLMOnMachine reports an LLMOnMachine value; empty is never.
func ValidLLMOnMachine(v string) bool {
	switch v {
	case "", LLMOnMachineNever, LLMOnMachinePrefer, LLMOnMachineRequire:
		return true
	}
	return false
}

// LLMOn is where the agent's turns call their model: LLMOnMachine, never
// when unset.
func (a Agent) LLMOn() string {
	if a.LLMOnMachine == "" {
		return LLMOnMachineNever
	}
	return a.LLMOnMachine
}

// MentionName is what calls the agent in a session: its mention, or its ID.
func (a Agent) MentionName() string {
	if a.Mention != "" {
		return a.Mention
	}
	return a.ID
}

// ToolRecord is a tool published by a worker: where it runs and its contract.
type ToolRecord struct {
	Name         string          `json:"name"`
	TaskQueue    string          `json:"task_queue"`
	Description  string          `json:"description"`
	InputSchema  json.RawMessage `json:"input_schema"`
	Kind         string          `json:"kind"`
	WorkflowName string          `json:"workflow_name,omitempty"`
	// What the tool is (see tool.Tool): readable from here by the processes
	// that run no tool, the server first.
	Sensitive        bool `json:"sensitive,omitempty"`
	PrivateInput     bool `json:"private_input,omitempty"`
	NeedsCallContext bool `json:"needs_call_context,omitempty"`
	// Timeout bounds one call (tool.Tool.Timeout); zero = the default.
	Timeout    time.Duration `json:"timeout,omitempty"`
	SchemaHash string        `json:"schema_hash"`
	UpdatedAt  time.Time     `json:"updated_at"`
}
