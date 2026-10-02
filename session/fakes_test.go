package session

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
)

// memStore is a session.Store in memory: one session at most is enough here.
type memStore struct {
	session  *store.Session
	members  []store.SessionMember
	messages []store.MessageWithID
	appended []store.Message
	title    string
}

func (m *memStore) CreateSession(_ context.Context, s store.Session) error {
	m.session = &s
	return nil
}
func (m *memStore) GetSession(_ context.Context, id string) (*store.Session, error) {
	if m.session == nil || m.session.SessionID != id {
		return nil, nil
	}
	return m.session, nil
}
func (m *memStore) GetActiveSessionByChannel(context.Context, string, string, string) (*store.Session, error) {
	return nil, nil
}
func (m *memStore) DeleteSession(context.Context, string) error { return nil }
func (m *memStore) UpdateSessionTitle(_ context.Context, _, title string) error {
	m.title = title
	return nil
}
func (m *memStore) SetSessionAgentMode(context.Context, string, string) error { return nil }
func (m *memStore) IsSessionMember(context.Context, string, string) (bool, error) {
	return true, nil
}
func (m *memStore) ListSessionMembers(context.Context, string) ([]store.SessionMember, error) {
	return m.members, nil
}
func (m *memStore) AddSessionMember(context.Context, string, string, string) error { return nil }
func (m *memStore) RemoveSessionMember(context.Context, string, string) error      { return nil }
func (m *memStore) GetUserByEmail(context.Context, string) (*store.User, error)    { return nil, nil }
func (m *memStore) GetAgent(_ context.Context, id string) (*store.Agent, error) {
	return &store.Agent{ID: id}, nil
}
func (m *memStore) ListAgents(context.Context) ([]store.Agent, error) {
	return []store.Agent{{ID: "default"}}, nil
}
func (m *memStore) AppendMessage(_ context.Context, _, _ string, msg store.Message) error {
	m.appended = append(m.appended, msg)
	return nil
}
func (m *memStore) LoadMessages(context.Context, string) ([]store.Message, error) {
	var out []store.Message
	for _, msg := range m.messages {
		out = append(out, msg.Message)
	}
	return out, nil
}
func (m *memStore) LoadMessagesUpTo(context.Context, string, int64) ([]store.MessageWithID, error) {
	return m.messages, nil
}

// fakeTemporal answers every list with running, the same workflow IDs, and
// records the queries and signals.
type fakeTemporal struct {
	running []string // workflow IDs the visibility queries return
	lists   []string // the queries
	signals []string // workflow IDs signalled
	started []string
}

func (f *fakeTemporal) ExecuteWorkflow(_ context.Context, o client.StartWorkflowOptions, _ interface{}, _ ...interface{}) (client.WorkflowRun, error) {
	f.started = append(f.started, o.ID)
	return nil, nil
}
func (f *fakeTemporal) SignalWorkflow(_ context.Context, id, _, _ string, _ interface{}) error {
	f.signals = append(f.signals, id)
	return nil
}
func (f *fakeTemporal) QueryWorkflow(context.Context, string, string, string, ...interface{}) (converter.EncodedValue, error) {
	return nil, errors.New("not running")
}
func (f *fakeTemporal) DescribeWorkflowExecution(context.Context, string, string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return nil, errors.New("not running")
}
func (f *fakeTemporal) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.lists = append(f.lists, req.Query)
	resp := &workflowservice.ListWorkflowExecutionsResponse{}
	for _, id := range f.running {
		resp.Executions = append(resp.Executions, &workflowpb.WorkflowExecutionInfo{Execution: &commonpb.WorkflowExecution{WorkflowId: id}})
	}
	return resp, nil
}
func (f *fakeTemporal) TerminateWorkflow(context.Context, string, string, string, ...interface{}) error {
	return nil
}

type nopHub struct{ events []activity.SSEEvent }

func (h *nopHub) Publish(_ string, ev activity.SSEEvent) { h.events = append(h.events, ev) }
