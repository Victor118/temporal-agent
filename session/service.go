// Package session holds the rules of a conversation, whatever the interface
// it comes through: who may do what to a session, when a message starts an
// agent turn, how a session is opened, forked, left or deleted, and how its
// workflows are found in Temporal.
//
// A session has no workflow of its own: each agent answering in it is a
// participant (workflow.ParticipantWorkflow), which runs while it has
// messages to answer.
//
// The JSON API, the htmx interface and the Telegram webhook are adapters over
// this service: they decode a request, call it, and shape its answer.
package session

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
)

// Errors the adapters turn into their own answers.
var (
	ErrNotFound        = errors.New("session not found")
	ErrNotCreator      = errors.New("only the session's creator can delete it; leave it instead")
	ErrNoSuchUser      = errors.New("no active user with this email")
	ErrBadForkPoint    = errors.New("no such message to fork from in this session")
	ErrPurposeTooLong  = fmt.Errorf("a fork's purpose is at most %d characters", MaxPurposeRunes)
	ErrEmptyMessage    = errors.New("message content is required")
	ErrSummaryPending  = errors.New("the summary of the parent session is still being written")
	ErrBadMode         = errors.New("mode must be auto, always or mention")
	ErrNothingToStop   = errors.New("no agent is working in this session")
	ErrTurnOver        = errors.New("the turn to stop is over")
	ErrStopNotAllowed  = errors.New("only the author of the message an agent answers, or the session's creator, may stop its turn")
	ErrClearNotAllowed = errors.New("only the session's creator may drop an agent's waiting messages")
	ErrQueueFull       = fmt.Errorf("the agent has %d messages waiting already: wait for it to answer them", MaxQueued)
	ErrForeignQuestion = errors.New("this question does not belong to this session")
	ErrEmptyAnswer     = errors.New("an answer is required")
)

// ChannelWeb is the channel of a session opened from the web.
const ChannelWeb = activity.ChannelWeb

// Store is what the service reads and writes.
type Store interface {
	CreateSession(ctx context.Context, session store.Session) error
	GetSession(ctx context.Context, sessionID string) (*store.Session, error)
	GetActiveSessionByChannel(ctx context.Context, userID, channel, channelID string) (*store.Session, error)
	DeleteSession(ctx context.Context, sessionID string) error
	UpdateSessionTitle(ctx context.Context, sessionID, title string) error
	SetSessionAgentMode(ctx context.Context, sessionID, mode string) error
	IsSessionMember(ctx context.Context, sessionID, userID string) (bool, error)
	ListSessionMembers(ctx context.Context, sessionID string) ([]store.SessionMember, error)
	AddSessionMember(ctx context.Context, sessionID, userID, addedBy string) error
	RemoveSessionMember(ctx context.Context, sessionID, userID string) error

	GetUserByEmail(ctx context.Context, email string) (*store.User, error)
	GetAgent(ctx context.Context, agentID string) (*store.Agent, error)
	ListAgents(ctx context.Context) ([]store.Agent, error)

	AppendMessage(ctx context.Context, sessionID, key string, msg store.Message) (int64, error)
	LoadMessagesUpTo(ctx context.Context, sessionID string, lastID int64) ([]store.MessageWithID, error)

	// The agents' background tasks (tasks.go).
	ListRunningTasks(ctx context.Context, sessionID, participant string) ([]store.BackgroundTask, error)
	GetTask(ctx context.Context, id string) (*store.BackgroundTask, error)
	SetTaskCancelledBy(ctx context.Context, id, name string) error
	ListTasksRunningSince(ctx context.Context, before time.Time) ([]store.BackgroundTask, error)
	EndTask(ctx context.Context, id, by, state string, build func(store.BackgroundTask) store.Message) (store.TaskEnding, error)
	ListTasksToWake(ctx context.Context, endedBefore time.Time) ([]store.BackgroundTask, error)
	ListTasksToCancel(ctx context.Context) ([]store.BackgroundTask, error)
	SetTaskWoken(ctx context.Context, id string) error
	SetTaskCancelSent(ctx context.Context, id string) error
}

// Temporal is what the service needs of the Temporal client: start, signal,
// query, find and stop the workflows of a session.
type Temporal interface {
	ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
	SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg interface{}) error
	SignalWithStartWorkflow(ctx context.Context, workflowID, signalName string, signalArg interface{}, options client.StartWorkflowOptions, workflow interface{}, workflowArgs ...interface{}) (client.WorkflowRun, error)
	QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...interface{}) (converter.EncodedValue, error)
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
	ListWorkflow(ctx context.Context, request *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error)
	TerminateWorkflow(ctx context.Context, workflowID, runID, reason string, details ...interface{}) error
	CancelWorkflow(ctx context.Context, workflowID, runID string) error
}

// Publisher shows an event to the members watching a session, live.
type Publisher interface {
	Publish(sessionID string, event activity.SSEEvent)
}

// Config is the part of the server's configuration the service uses.
type Config struct {
	Namespace      string // Temporal namespace, for the visibility queries
	WorkflowQueue  string // where the participants and fork workflows run
	DefaultAgentID string // the agent of a session that names none
	SummaryModel   string // writes a fork's summary; empty = the workers' default
	// Channels are the channels' notifiers, by name, for what the server
	// tells a session itself (a background task's wake given up by the
	// sweep); the web is the hub when absent.
	Channels map[string]activity.Notifier
}

// Service applies the rules of a conversation. Build it with New.
type Service struct {
	store    Store
	temporal Temporal
	hub      Publisher
	cfg      Config
	statuses statusCache
	turns    turns // what the turn events tell, in memory
	// background counts the work started off a caller's way (a title, the
	// trees' rings): tests wait for it.
	background sync.WaitGroup
}

func New(st Store, tc Temporal, hub Publisher, cfg Config) *Service {
	return &Service{store: st, temporal: tc, hub: hub, cfg: cfg}
}
