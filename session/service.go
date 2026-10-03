// Package session holds the rules of a conversation, whatever the interface
// it comes through: who may do what to a session, when a message starts an
// agent turn, how a session is opened, forked, left or deleted, and how its
// workflows are found in Temporal.
//
// The JSON API, the htmx interface and the Telegram webhook are adapters over
// this service: they decode a request, call it, and shape its answer.
package session

import (
	"context"
	"errors"

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
	ErrEmptyMessage    = errors.New("message content is required")
	ErrSummaryPending  = errors.New("the summary of the parent session is still being written")
	ErrBadMode         = errors.New("mode must be auto, always or mention")
	ErrNoActiveSession = errors.New("no active session found")
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
	LoadMessages(ctx context.Context, sessionID string) ([]store.Message, error)
	LoadMessagesUpTo(ctx context.Context, sessionID string, lastID int64) ([]store.MessageWithID, error)
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
}

// Publisher shows an event to the members watching a session, live.
type Publisher interface {
	Publish(sessionID string, event activity.SSEEvent)
}

// Config is the part of the server's configuration the service uses.
type Config struct {
	Namespace      string // Temporal namespace, for the visibility queries
	WorkflowQueue  string // where session and fork workflows run
	DefaultAgentID string // the agent of a session that names none
	SummaryModel   string // writes a fork's summary; empty = the workers' default
}

// Service applies the rules of a conversation. Build it with New.
type Service struct {
	store    Store
	temporal Temporal
	hub      Publisher
	cfg      Config
	statuses statusCache
}

func New(st Store, tc Temporal, hub Publisher, cfg Config) *Service {
	return &Service{store: st, temporal: tc, hub: hub, cfg: cfg}
}
