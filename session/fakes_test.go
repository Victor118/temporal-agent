package session

import (
	"context"
	"errors"
	"slices"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
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
// records the queries and signals. A workflow of running is described as
// running, and answers a query with states, if set.
type fakeTemporal struct {
	running      []string // workflow IDs the visibility queries return
	lists        []string // the queries
	listErr      error    // what ListWorkflow returns
	signals      []string // workflow IDs signalled
	signalErr    error    // what SignalWorkflow returns
	started      []string
	signalStarts []signalStart
	states       map[string]workflow.SessionState // by workflow ID
	queried      []string                         // workflow IDs queried
}

// signalStart is a SignalWithStartWorkflow call.
type signalStart struct {
	id      string
	options client.StartWorkflowOptions
	signal  string
	arg     interface{}
	input   []interface{}
}

func (f *fakeTemporal) SignalWithStartWorkflow(_ context.Context, id, signal string, arg interface{}, o client.StartWorkflowOptions, _ interface{}, input ...interface{}) (client.WorkflowRun, error) {
	f.signalStarts = append(f.signalStarts, signalStart{id: id, options: o, signal: signal, arg: arg, input: input})
	return nil, nil
}

// encodedState is a query's answer.
type encodedState struct{ state workflow.SessionState }

func (e encodedState) HasValue() bool { return true }
func (e encodedState) Get(v interface{}) error {
	*v.(*workflow.SessionState) = e.state
	return nil
}

func (f *fakeTemporal) ExecuteWorkflow(_ context.Context, o client.StartWorkflowOptions, _ interface{}, _ ...interface{}) (client.WorkflowRun, error) {
	f.started = append(f.started, o.ID)
	return nil, nil
}
func (f *fakeTemporal) SignalWorkflow(_ context.Context, id, _, _ string, _ interface{}) error {
	f.signals = append(f.signals, id)
	return f.signalErr
}
func (f *fakeTemporal) QueryWorkflow(_ context.Context, id, _, _ string, _ ...interface{}) (converter.EncodedValue, error) {
	f.queried = append(f.queried, id)
	if state, ok := f.states[id]; ok {
		return encodedState{state}, nil
	}
	return nil, errors.New("not running")
}
func (f *fakeTemporal) DescribeWorkflowExecution(_ context.Context, id, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	if !slices.Contains(f.running, id) {
		return nil, errors.New("not running")
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}}, nil
}
func (f *fakeTemporal) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.lists = append(f.lists, req.Query)
	if f.listErr != nil {
		return nil, f.listErr
	}
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
