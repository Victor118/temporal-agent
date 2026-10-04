package session

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
)

// memStore is a session.Store in memory: one session at most is enough here.
type memStore struct {
	session *store.Session
	others  map[string]*store.Session // more sessions, by ID: a fork's parent
	// outsiders are the users who are not members of a session, by session
	// ID: everyone else is.
	outsiders map[string][]string
	members   []store.SessionMember
	// membersErr fails ListSessionMembers; membersGate, when set, holds it
	// until closed.
	membersErr  error
	membersGate chan struct{}
	messages    []store.MessageWithID
	appended    []store.Message
	titleMu     sync.Mutex // the title is set in the background
	title       string
	agents      []store.Agent // nil: the default agent alone
	loads       int           // conversations loaded
}

func (m *memStore) CreateSession(_ context.Context, s store.Session) error {
	m.session = &s
	return nil
}
func (m *memStore) GetSession(_ context.Context, id string) (*store.Session, error) {
	if m.session == nil || m.session.SessionID != id {
		return m.others[id], nil
	}
	return m.session, nil
}
func (m *memStore) GetActiveSessionByChannel(context.Context, string, string, string) (*store.Session, error) {
	return nil, nil
}
func (m *memStore) DeleteSession(context.Context, string) error { return nil }
func (m *memStore) UpdateSessionTitle(_ context.Context, _, title string) error {
	m.titleMu.Lock()
	defer m.titleMu.Unlock()
	m.title = title
	return nil
}
func (m *memStore) SetSessionAgentMode(context.Context, string, string) error { return nil }
func (m *memStore) IsSessionMember(_ context.Context, sessionID, userID string) (bool, error) {
	return !slices.Contains(m.outsiders[sessionID], userID), nil
}
func (m *memStore) ListSessionMembers(context.Context, string) ([]store.SessionMember, error) {
	if m.membersGate != nil {
		<-m.membersGate
	}
	if m.membersErr != nil {
		return nil, m.membersErr
	}
	return m.members, nil
}
func (m *memStore) AddSessionMember(context.Context, string, string, string) error { return nil }
func (m *memStore) RemoveSessionMember(context.Context, string, string) error      { return nil }
func (m *memStore) GetUserByEmail(context.Context, string) (*store.User, error)    { return nil, nil }
func (m *memStore) GetAgent(_ context.Context, id string) (*store.Agent, error) {
	return &store.Agent{ID: id}, nil
}
func (m *memStore) ListAgents(context.Context) ([]store.Agent, error) {
	if m.agents != nil {
		return m.agents, nil
	}
	return []store.Agent{{ID: "default"}}, nil
}
func (m *memStore) AppendMessage(_ context.Context, _, _ string, msg store.Message) (int64, error) {
	m.appended = append(m.appended, msg)
	return int64(len(m.appended)), nil
}
func (m *memStore) LoadMessagesUpTo(context.Context, string, int64) ([]store.MessageWithID, error) {
	m.loads++
	return m.messages, nil
}

// fakeTemporal answers every list with running, the same workflow IDs, and
// records the queries and signals. A workflow of running is described as
// running, and answers a query with states, if set: a workflow listed
// without one fails it (no worker answers), one not listed is not found.
type fakeTemporal struct {
	mu           sync.Mutex                                 // the statuses load in the background
	running      []string                                   // workflow IDs the visibility queries return
	byType       map[string][]string                        // set: what a query naming a workflow type returns instead
	closed       map[string]enumspb.WorkflowExecutionStatus // how the workflows not running ended
	closedAt     map[string]time.Time                       // when; now if not set
	describes    int                                        // DescribeWorkflowExecution calls
	options      []client.StartWorkflowOptions              // of each workflow started
	lists        []string                                   // the queries
	signals      []string                                   // workflow IDs signalled
	signalNames  []string                                   // the signal of each
	signalArgs   []interface{}                              // the argument of each
	started      []string
	inputs       []interface{} // the input of each workflow started
	signalStarts []signalStart
	startErr     error                  // fails every SignalWithStart
	startedAt    map[string]time.Time   // when the workflows listed started; zero if not set
	states       map[string]interface{} // query answers, by workflow ID
	queryErrs    map[string]error       // query failures, by workflow ID
	listGate     chan struct{}          // set: every list waits for it to close
	queried      []string               // workflow IDs queried
	terminated   []string
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return nil, f.startErr
	}
	f.signalStarts = append(f.signalStarts, signalStart{id: id, options: o, signal: signal, arg: arg, input: input})
	return nil, nil
}

// encodedState is a query's answer, as JSON goes through Temporal.
type encodedState struct{ state interface{} }

func (e encodedState) HasValue() bool { return true }
func (e encodedState) Get(v interface{}) error {
	b, err := json.Marshal(e.state)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func (f *fakeTemporal) ExecuteWorkflow(_ context.Context, o client.StartWorkflowOptions, _ interface{}, args ...interface{}) (client.WorkflowRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started = append(f.started, o.ID)
	f.options = append(f.options, o)
	if len(args) > 0 {
		f.inputs = append(f.inputs, args[0])
	}
	return nil, nil
}
func (f *fakeTemporal) SignalWorkflow(_ context.Context, id, _, signal string, arg interface{}) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signals = append(f.signals, id)
	f.signalNames = append(f.signalNames, signal)
	f.signalArgs = append(f.signalArgs, arg)
	return nil
}
func (f *fakeTemporal) QueryWorkflow(_ context.Context, id, _, _ string, _ ...interface{}) (converter.EncodedValue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queried = append(f.queried, id)
	if err, ok := f.queryErrs[id]; ok {
		return nil, err
	}
	if state, ok := f.states[id]; ok {
		return encodedState{state}, nil
	}
	if f.listed(id) {
		return nil, errors.New("no worker answered") // running, with no answer set
	}
	return nil, serviceerror.NewNotFound("not running")
}

// listed reports whether the visibility queries list a workflow. Under f.mu.
func (f *fakeTemporal) listed(id string) bool {
	if slices.Contains(f.running, id) {
		return true
	}
	for _, ids := range f.byType {
		if slices.Contains(ids, id) {
			return true
		}
	}
	return false
}

func (f *fakeTemporal) DescribeWorkflowExecution(_ context.Context, id, _ string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.describes++
	if status, ok := f.closed[id]; ok {
		at, ok := f.closedAt[id]
		if !ok {
			at = time.Now()
		}
		return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Status: status, CloseTime: timestamppb.New(at),
		}}, nil
	}
	if !slices.Contains(f.running, id) {
		return nil, errors.New("not running")
	}
	return &workflowservice.DescribeWorkflowExecutionResponse{WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}}, nil
}
func (f *fakeTemporal) ListWorkflow(_ context.Context, req *workflowservice.ListWorkflowExecutionsRequest) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	if f.listGate != nil {
		<-f.listGate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists = append(f.lists, req.Query)
	resp := &workflowservice.ListWorkflowExecutionsResponse{}
	ids := f.running
	if f.byType != nil {
		ids = nil
		for workflowType, of := range f.byType {
			if strings.Contains(req.Query, "'"+workflowType+"'") {
				ids = append(ids, of...)
			}
		}
	}
	for _, id := range ids {
		resp.Executions = append(resp.Executions, &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: id}, StartTime: timestamppb.New(f.startedAt[id]),
		})
	}
	return resp, nil
}
func (f *fakeTemporal) TerminateWorkflow(_ context.Context, id, _, _ string, _ ...interface{}) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.terminated = append(f.terminated, id)
	return nil
}

// nopHub records the events published, whatever their topic: the trees
// ring from a background goroutine too.
type nopHub struct {
	mu     sync.Mutex
	events []activity.SSEEvent
	topics []string
}

func (h *nopHub) Publish(topic string, ev activity.SSEEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, ev)
	h.topics = append(h.topics, topic)
}

// on returns the events published on topic.
func (h *nopHub) on(topic string) []activity.SSEEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []activity.SSEEvent
	for i, t := range h.topics {
		if t == topic {
			out = append(out, h.events[i])
		}
	}
	return out
}
