package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// memTasks keeps background tasks in memory, over a session's messages:
// a task's end goes into session as Postgres writes it (store.EndTask),
// the first writer winning. The session deleted takes its tasks.
type memTasks struct {
	mu         sync.Mutex
	session    *memSession
	agent      string // the session's agent
	tasks      map[string]*store.BackgroundTask
	registered []store.BackgroundTask
}

func newMemTasks(session *memSession) *memTasks {
	return &memTasks{session: session, agent: "jarvis", tasks: map[string]*store.BackgroundTask{}}
}

func (m *memTasks) GetSession(ctx context.Context, id string) (*store.Session, error) {
	sess, err := m.session.GetSession(ctx, id)
	if sess != nil {
		sess.AgentID, sess.Channel = m.agent, "web"
	}
	return sess, err
}
func (m *memTasks) RegisterTask(_ context.Context, t store.BackgroundTask, max int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[t.ID]; ok {
		return nil
	}
	n := 0
	for _, have := range m.tasks {
		if have.SessionID == t.SessionID && have.Participant == t.Participant && have.State == store.BackgroundRunning {
			n++
		}
	}
	if n >= max {
		return store.ErrTooManyTasks
	}
	t.State, t.StartedAt = store.BackgroundRunning, time.Now()
	m.tasks[t.ID] = &t
	m.registered = append(m.registered, t)
	return nil
}
func (m *memTasks) DropTask(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, id)
	return nil
}
func (m *memTasks) GetTask(_ context.Context, id string) (*store.BackgroundTask, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.tasks[id]; ok {
		c := *t
		return &c, nil
	}
	return nil, nil
}
func (m *memTasks) EndTask(_ context.Context, id, by, state string, build func(store.BackgroundTask) store.Message) (store.TaskEnding, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	if !ok {
		return store.TaskEnding{Gone: true}, nil
	}
	if t.State != store.BackgroundRunning {
		return store.TaskEnding{Task: *t, MessageID: t.ResultMessageID, Mine: t.EndedBy == by}, nil
	}
	now := time.Now()
	t.State, t.EndedAt, t.EndedBy = state, &now, by
	if state != store.BackgroundCancelled {
		t.CancelledBy = ""
	}
	t.ResultMessageID = m.session.add(store.TaskResultKey(id), build(*t))
	return store.TaskEnding{Task: *t, MessageID: t.ResultMessageID, Mine: true}, nil
}

func (m *memTasks) SetTaskWoken(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.tasks[id]; ok {
		now := time.Now()
		t.WokenAt = &now
	}
	return nil
}

// woken reports whether a task's wake was recorded done with.
func (m *memTasks) woken(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.tasks[id]
	return ok && t.WokenAt != nil
}

// deleteSession deletes the session, and its tasks with it.
func (m *memTasks) deleteSession() {
	m.mu.Lock()
	m.tasks = map[string]*store.BackgroundTask{}
	m.mu.Unlock()
	m.session.mu.Lock()
	m.session.deleted = true
	m.session.mu.Unlock()
}

// recordedStarts records the SignalWithStarts of the relay.
type recordedStarts struct {
	mu    sync.Mutex
	calls []activity.RelayInput
	err   error
}

func (r *recordedStarts) SignalWithStartWorkflow(_ context.Context, id, signal string, arg interface{}, o client.StartWorkflowOptions, wf interface{}, args ...interface{}) (client.WorkflowRun, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	in := activity.RelayInput{WorkflowID: id, Signal: signal, TaskQueue: o.TaskQueue, WorkflowType: wf.(string)}
	in.Message, _ = arg.(json.RawMessage)
	if len(args) > 0 {
		in.Start, _ = args[0].(json.RawMessage)
	}
	r.calls = append(r.calls, in)
	return nil, r.err
}

func (r *recordedStarts) all() []activity.RelayInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

// taskFiles publishes in memory.
type taskFiles struct{ names []string }

func (p *taskFiles) Publish(_ context.Context, name string, content []byte) (tool.FileRef, error) {
	p.names = append(p.names, name)
	return tool.FileRef{ID: "file-1", Name: name, Size: int64(len(content))}, nil
}

// taskEnv is a background task's workflow over memory: its tool a child
// workflow, its end real, its wake recorded.
type taskEnv struct {
	env      *testsuite.TestWorkflowEnvironment
	session  *memSession
	tasks    *memTasks
	starts   *recordedStarts
	files    *taskFiles
	taskAct  *activity.TaskActivities // its Store may be replaced before run
	mu       sync.Mutex
	notified []activity.NotifyInput
}

const testTaskID = "s1:p:smith:m4:bg:c1"

func newTaskEnv(t *testing.T, tool func(sdkworkflow.Context, json.RawMessage) (tool.Result, error)) *taskEnv {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	e := &taskEnv{env: suite.NewTestWorkflowEnvironment(), session: &memSession{memory: map[string]store.Memory{}}, starts: &recordedStarts{}, files: &taskFiles{}}
	e.tasks = newMemTasks(e.session)
	e.env.RegisterWorkflowWithOptions(tool, sdkworkflow.RegisterOptions{Name: "FakeToolWorkflow"})
	e.taskAct = &activity.TaskActivities{Store: e.tasks, Publisher: e.files}
	e.env.RegisterActivity(e.taskAct)
	e.env.RegisterActivity(&activity.RelayActivities{Client: e.starts, Sessions: e.tasks})
	turnAct := &activity.TurnActivities{Store: e.session}
	e.env.RegisterActivityWithOptions(turnAct.EndTurn, sdkactivity.RegisterOptions{Name: "EndTurn"})
	e.env.RegisterActivityWithOptions(func(_ context.Context, in activity.NotifyInput) error {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.notified = append(e.notified, in)
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	// The task as the turn registered it, an instruction attached since.
	e.tasks.tasks[testTaskID] = &store.BackgroundTask{ID: testTaskID, SessionID: "s1", Participant: "smith", UserID: "u-alice", UserName: "Alice",
		Tool: "analyze_repo", Summary: "cinesense", TurnKey: "m4.smith", CallID: "c1", Channel: "telegram", ChannelID: "42",
		State: store.BackgroundRunning, StartedAt: time.Now(), FollowUps: []store.TaskFollowUp{{Text: "then implement it", UserName: "Alice"}}}
	e.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: testTaskID, TaskQueue: "agent"})
	return e
}

func (e *taskEnv) run() {
	e.env.ExecuteWorkflow(BackgroundTaskWorkflow, BackgroundTaskInput{SessionID: "s1", AgentID: "smith", Tool: "analyze_repo",
		Workflow: "FakeToolWorkflow", ChildID: testTaskID + ":tool:analyze_repo:c1", TaskQueue: "agent", Input: json.RawMessage(`{"repo":"cinesense"}`)})
}

func (e *taskEnv) events(eventType string) []activity.NotifyInput {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []activity.NotifyInput
	for _, n := range e.notified {
		if n.Event.Type == eventType {
			out = append(out, n)
		}
	}
	return out
}

// taskResults are the task results in the session.
func (e *taskEnv) taskResults() []store.MessageWithID {
	var out []store.MessageWithID
	for _, m := range e.session.history() {
		if m.Kind == store.KindTaskResult {
			out = append(out, m)
		}
	}
	return out
}

// The task runs its tool, posts its result into the session, with the
// instructions attached meanwhile, and wakes its participant with it: the
// launching turn's user and channel, signed (Smith is not the session's
// agent), the session read first.
func TestBackgroundTask_PostsItsResultAndWakesItsParticipant(t *testing.T) {
	e := newTaskEnv(t, func(sdkworkflow.Context, json.RawMessage) (tool.Result, error) {
		return tool.Result{Content: "the report"}, nil
	})
	e.run()
	if err := e.env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	results := e.taskResults()
	if len(results) != 1 {
		t.Fatalf("task results: %+v", results)
	}
	m := results[0]
	if m.Key != store.TaskResultKey(testTaskID) || m.Content != `"the report"` || m.AgentID != "smith" || m.UserID != "u-alice" ||
		m.Task.State != store.BackgroundDone || len(m.Task.FollowUps) != 1 || m.Task.FollowUps[0].Text != "then implement it" {
		t.Errorf("message %+v %+v", m, m.Task)
	}
	starts := e.starts.all()
	if len(starts) != 1 {
		t.Fatalf("wakes: %+v", starts)
	}
	s := starts[0]
	var msg ParticipantMessage
	json.Unmarshal(s.Message, &msg)
	var start ParticipantInput
	json.Unmarshal(s.Start, &start)
	if s.WorkflowID != "s1:p:smith" || s.Signal != SignalMessage || s.WorkflowType != "ParticipantWorkflow" || s.TaskQueue != "agent" ||
		msg.MessageID != m.ID || msg.UserID != "u-alice" || msg.UserName != "Alice" || msg.Channel != "telegram" || msg.ChannelID != "42" || !msg.SignReply ||
		start.SessionID != "s1" || start.AgentID != "smith" || start.Channel != "web" {
		t.Errorf("wake %+v: %+v, start %+v", s, msg, start)
	}
	if ev := e.events(EventTaskResult); len(ev) != 1 || !strings.Contains(string(ev[0].Event.Data), testTaskID) || ev[0].Channel != "" {
		t.Errorf("task_result events %+v", ev)
	}
	if !e.tasks.woken(testTaskID) {
		t.Error("the wake was not recorded: the sweep would wake again")
	}
}

// The sweep ended the task first (its workflow was thought gone): the task
// posts nothing, tells nothing, wakes nobody.
func TestBackgroundTask_SweptFirstDoesNothing(t *testing.T) {
	e := newTaskEnv(t, func(sdkworkflow.Context, json.RawMessage) (tool.Result, error) {
		return tool.Result{Content: "the report"}, nil
	})
	if _, err := e.tasks.EndTask(context.Background(), testTaskID, store.TaskEndedBySweep, store.BackgroundFailed, func(t store.BackgroundTask) store.Message {
		return activity.TaskResultMessage(t, "stopped without a result", nil)
	}); err != nil {
		t.Fatal(err)
	}
	e.run()
	if err := e.env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if len(e.taskResults()) != 1 || len(e.events(EventTaskResult)) != 0 || len(e.starts.all()) != 0 {
		t.Errorf("results %d, task_result events %d, wakes %d", len(e.taskResults()), len(e.events(EventTaskResult)), len(e.starts.all()))
	}
}

// A member's stop that comes once the tool has ended changes nothing: the
// task ended done, its end is posted and its participant woken, from a
// context the stop does not reach; the message names no canceller.
func TestBackgroundTask_StopAfterTheToolStillWakes(t *testing.T) {
	e := newTaskEnv(t, func(sdkworkflow.Context, json.RawMessage) (tool.Result, error) {
		return tool.Result{Content: "the report"}, nil
	})
	e.tasks.tasks[testTaskID].CancelledBy = "Bob"
	// The stop lands while the end is being written.
	e.taskAct.Store = &cancellingTasks{memTasks: e.tasks, cancel: e.env.CancelWorkflow}
	e.run()
	results := e.taskResults()
	if len(results) != 1 || results[0].Task.State != store.BackgroundDone || results[0].Task.CancelledBy != "" {
		t.Fatalf("results %+v", results)
	}
	if len(e.starts.all()) != 1 || !e.tasks.woken(testTaskID) {
		t.Errorf("wakes %d, recorded %v", len(e.starts.all()), e.tasks.woken(testTaskID))
	}
}

// cancellingTasks cancels the task's workflow as its end is written.
type cancellingTasks struct {
	*memTasks
	cancel func()
	once   sync.Once
}

func (c *cancellingTasks) EndTask(ctx context.Context, id, by, state string, build func(store.BackgroundTask) store.Message) (store.TaskEnding, error) {
	c.once.Do(c.cancel)
	time.Sleep(300 * time.Millisecond) // for the cancellation to be handled
	return c.memTasks.EndTask(ctx, id, by, state, build)
}

// A failed tool is a failed task: its message says why, and wakes its
// participant all the same.
func TestBackgroundTask_FailureIsPosted(t *testing.T) {
	e := newTaskEnv(t, func(sdkworkflow.Context, json.RawMessage) (tool.Result, error) {
		return tool.Result{}, errors.New("clone refused")
	})
	e.run()
	results := e.taskResults()
	if len(results) != 1 || results[0].Task.State != store.BackgroundFailed || !strings.Contains(results[0].Content, "clone refused") {
		t.Fatalf("results %+v", results)
	}
	if len(e.starts.all()) != 1 {
		t.Error("the participant was not woken")
	}
}

// A result past the message's bound is published as a file of the call;
// the message keeps its start, and the members are told of the file.
func TestBackgroundTask_LongResultAsFile(t *testing.T) {
	long := strings.Repeat("x", activity.MaxTaskMessageBytes+100)
	e := newTaskEnv(t, func(sdkworkflow.Context, json.RawMessage) (tool.Result, error) {
		return tool.Result{Content: long}, nil
	})
	e.run()
	results := e.taskResults()
	if len(results) != 1 || results[0].Task.File == nil || results[0].Task.File.Name != "resultat-analyze_repo.md" ||
		len(results[0].Content) > activity.MaxTaskMessageBytes+2 {
		t.Fatalf("results %+v", results)
	}
	if ev := e.events(activity.EventFilePublished); len(ev) != 1 || !strings.Contains(string(ev[0].Event.Data), "file-1") {
		t.Errorf("file_published %+v", ev)
	}
}

// Cancelled by a member: its tool is cancelled, its message says who
// stopped it, and wakes nobody.
func TestBackgroundTask_CancelledWakesNobody(t *testing.T) {
	e := newTaskEnv(t, func(ctx sdkworkflow.Context, _ json.RawMessage) (tool.Result, error) {
		if err := sdkworkflow.Sleep(ctx, time.Hour); err != nil {
			return tool.Result{}, err
		}
		return tool.Result{Content: "too late"}, nil
	})
	e.tasks.tasks[testTaskID].CancelledBy = "Bob"
	e.env.RegisterDelayedCallback(e.env.CancelWorkflow, time.Minute)
	e.run()
	results := e.taskResults()
	if len(results) != 1 || results[0].Task.State != store.BackgroundCancelled || results[0].Task.CancelledBy != "Bob" {
		t.Fatalf("results %+v", results)
	}
	if starts := e.starts.all(); len(starts) != 0 {
		t.Errorf("woken: %+v", starts)
	}
}

// The session deleted while the task runs: its end writes nothing, wakes
// nobody.
func TestBackgroundTask_SessionDeletedMeanwhile(t *testing.T) {
	e := newTaskEnv(t, func(ctx sdkworkflow.Context, _ json.RawMessage) (tool.Result, error) {
		sdkworkflow.Sleep(ctx, time.Hour)
		return tool.Result{Content: "the report"}, nil
	})
	e.env.RegisterDelayedCallback(e.tasks.deleteSession, time.Minute)
	e.run()
	if err := e.env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if results := e.taskResults(); len(results) != 0 {
		t.Errorf("results %+v", results)
	}
	if starts := e.starts.all(); len(starts) != 0 {
		t.Errorf("woken: %+v", starts)
	}
}

// The wake fails for good: the message gets an end under the turn that
// would have answered it, and the channel is told.
func TestBackgroundTask_WakeFailureEndsItsTurn(t *testing.T) {
	e := newTaskEnv(t, func(sdkworkflow.Context, json.RawMessage) (tool.Result, error) {
		return tool.Result{Content: "the report"}, nil
	})
	e.starts.err = errors.New("temporal away")
	e.run()
	results := e.taskResults()
	if len(results) != 1 {
		t.Fatalf("results %+v", results)
	}
	turn := store.TurnKey(results[0].ID, "smith")
	found := false
	for _, m := range e.session.history() {
		if m.Key == store.TurnEndKey(turn) && strings.Contains(store.TurnEndError(m.Message), "réveiller @smith") {
			found = true
		}
	}
	if !found {
		t.Errorf("no end under %s: %+v", turn, e.session.history())
	}
	told := false
	for _, n := range e.events(activity.EventMessage) {
		told = told || (n.Channel == "telegram" && strings.Contains(string(n.Event.Data), "Error processing message"))
	}
	if !told {
		t.Error("the channel was not told")
	}
}

// turnEnv runs a session turn of Jarvis's, which may launch tasks: its
// task workflow recorded, not run.
type turnEnv struct {
	env      *testsuite.TestWorkflowEnvironment
	f        *llmFakes
	tasks    *memTasks
	mu       sync.Mutex
	launched []BackgroundTaskInput
	children []json.RawMessage // inputs of SlowWorkflow
	notified []activity.NotifyInput
	// task and slow, when set, run instead of recording the task's
	// workflow, and of answering SlowWorkflow at once.
	task func(sdkworkflow.Context, BackgroundTaskInput) error
	slow func(sdkworkflow.Context, json.RawMessage) (tool.Result, error)
}

func newTurnEnv(t *testing.T, calls ...provider.ToolCallInfo) *turnEnv {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	e := &turnEnv{env: suite.NewTestWorkflowEnvironment()}
	e.f = registerLLM(e.env, answers(provider.ChatResponse{ToolCalls: calls}, done))
	e.f.catalog.SetAgents([]activity.AgentCatalogEntry{{ID: "jarvis", Name: "Jarvis"}, {ID: "smith", Name: "Smith"}})
	e.f.catalog.SetTools([]store.ToolRecord{{Name: "slow_tool", Kind: "workflow", InputSchema: json.RawMessage(`{"type":"object"}`)}})
	e.tasks = newMemTasks(e.f.session)
	e.env.RegisterActivity(&activity.TaskActivities{Store: e.tasks})
	e.env.RegisterActivityWithOptions(func(context.Context, activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{Tools: []provider.ToolDefinition{{Name: "agent_smith"}, {Name: "slow_tool"}}, Resolutions: map[string]activity.ToolResolution{
			"agent_smith": {Kind: "workflow", AgentID: "smith", Background: true},
			"slow_tool":   {Kind: "workflow", WorkflowName: "SlowWorkflow", TaskQueue: "tools", Background: true},
		}}, nil
	}, sdkactivity.RegisterOptions{Name: "ListTools"})
	e.env.RegisterActivityWithOptions(func(context.Context, activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{Name: "Jarvis"}, nil
	}, sdkactivity.RegisterOptions{Name: "LoadSkillsForAgent"})
	e.env.RegisterActivityWithOptions(func(_ context.Context, in activity.NotifyInput) error {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.notified = append(e.notified, in)
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	e.env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in BackgroundTaskInput) error {
		e.mu.Lock()
		e.launched = append(e.launched, in)
		e.mu.Unlock()
		if e.task != nil {
			return e.task(ctx, in)
		}
		return nil
	}, sdkworkflow.RegisterOptions{Name: "BackgroundTaskWorkflow"})
	e.env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, in json.RawMessage) (tool.Result, error) {
		e.mu.Lock()
		e.children = append(e.children, in)
		e.mu.Unlock()
		if e.slow != nil {
			return e.slow(ctx, in)
		}
		return tool.Result{Content: "slow result"}, nil
	}, sdkworkflow.RegisterOptions{Name: "SlowWorkflow"})
	return e
}

// runTurn runs Jarvis's turn on a message of Alice's, on Telegram.
func (e *turnEnv) runTurn() {
	id := e.f.session.add(store.HumanMessageKey("q"), store.Message{Role: store.RoleUser, Content: `"review it, in the background"`, UserID: "u-alice", Author: "Alice"})
	e.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: "s1:p:jarvis:m1", TaskQueue: "agent"})
	e.env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1", AgentID: "jarvis", UserID: "u-alice", UserName: "Alice",
		TurnKey: store.TurnKey(id, "jarvis"), Channel: "telegram", ChannelID: "42"})
}

// result is what the model read of the turn's call.
func (e *turnEnv) result(t *testing.T) *provider.ToolResultInfo {
	t.Helper()
	sent := e.f.model.sent()
	if len(sent) < 2 {
		t.Fatalf("the model was called %d times", len(sent))
	}
	for _, m := range sent[1].Messages {
		if m.ToolResult != nil {
			return m.ToolResult
		}
	}
	t.Fatal("no tool result")
	return nil
}

// A session turn launches a sub-agent in the background: the task is
// recorded, its workflow started under the turn, abandoned, with the
// sub-agent as its child; the turn goes on with "started" and ends. The
// model was offered the field; its stored call keeps it, the sub-agent
// does not read it.
func TestAgentWorkflow_LaunchesABackgroundTask(t *testing.T) {
	e := newTurnEnv(t, provider.ToolCallInfo{ID: "c1", Name: "agent_smith", Input: json.RawMessage(`{"task":"review the PR","background":true}`)})
	e.runTurn()
	if err := e.env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	const task = "s1:p:jarvis:m1:bg:c1"
	if len(e.tasks.registered) != 1 {
		t.Fatalf("registered %+v", e.tasks.registered)
	}
	if r := e.tasks.registered[0]; r.ID != task || r.SessionID != "s1" || r.Participant != "jarvis" || r.UserID != "u-alice" || r.UserName != "Alice" ||
		r.Tool != "agent_smith" || r.Summary != "review the PR" || r.TurnKey != "m1.jarvis" || r.CallID != "c1" || r.Channel != "telegram" || r.ChannelID != "42" {
		t.Errorf("registered %+v", r)
	}
	if len(e.launched) != 1 {
		t.Fatalf("launched %+v", e.launched)
	}
	l := e.launched[0]
	var sub AgentWorkflowInput
	json.Unmarshal(l.Input, &sub)
	if l.Workflow != "AgentWorkflow" || !l.SubAgent || l.ChildID != task+":tool:agent_smith:c1" || l.AgentID != "jarvis" || l.TaskQueue != "agent" ||
		sub.UserMessage != "review the PR" || sub.SessionID != l.ChildID || sub.AgentID != "smith" || sub.UserID != "u-alice" ||
		sub.CallPrefix != "c1/" || sub.SessionTurn == nil || sub.SessionTurn.TurnKey != "m1.jarvis" || sub.Channel != "telegram" {
		t.Errorf("launched %+v, sub-agent %+v", l, sub)
	}
	if r := e.result(t); r.IsError || !strings.HasPrefix(r.Content, "Background task started, ID "+task) {
		t.Errorf("result %+v", r)
	}
	// Offered, and kept in the stored call.
	offered := false
	for _, d := range e.f.model.sent()[0].Tools {
		offered = offered || (d.Name == "agent_smith" && strings.Contains(string(d.InputSchema), `"background"`))
	}
	if !offered {
		t.Error("the field was not offered")
	}
	kept := false
	for _, m := range e.f.session.history() {
		for _, tc := range m.ToolCalls {
			kept = kept || strings.Contains(string(tc.Input), `"background":true`)
		}
	}
	if !kept {
		t.Error("the stored call lost its field")
	}
	started := false
	for _, n := range e.notified {
		started = started || (n.Event.Type == EventTaskStarted && strings.Contains(string(n.Event.Data), task))
	}
	if !started {
		t.Error("no task_started event")
	}
}

// Three tasks run already: the call is refused, for the model to wait or
// make it now.
func TestAgentWorkflow_BackgroundCap(t *testing.T) {
	e := newTurnEnv(t, provider.ToolCallInfo{ID: "c1", Name: "agent_smith", Input: json.RawMessage(`{"task":"review","background":true}`)})
	for _, id := range []string{"a", "b", "c"} {
		e.tasks.tasks[id] = &store.BackgroundTask{ID: id, SessionID: "s1", Participant: "jarvis", State: store.BackgroundRunning}
	}
	e.runTurn()
	if r := e.result(t); !r.IsError || !strings.Contains(r.Content, "3 background tasks running already") {
		t.Errorf("result %+v", r)
	}
	if len(e.launched) != 0 {
		t.Errorf("launched %+v", e.launched)
	}
}

// Asked to wait (or with no field), the tool runs as ever, and never reads
// the field.
func TestAgentWorkflow_ForegroundCallLosesTheField(t *testing.T) {
	e := newTurnEnv(t, provider.ToolCallInfo{ID: "c1", Name: "slow_tool", Input: json.RawMessage(`{"repo":"x","background":false}`)})
	e.runTurn()
	if r := e.result(t); r.IsError || r.Content != "slow result" {
		t.Errorf("result %+v", r)
	}
	if len(e.children) != 1 || strings.Contains(string(e.children[0]), "background") {
		t.Errorf("the tool read %s", e.children)
	}
	if len(e.launched) != 0 || len(e.tasks.registered) != 0 {
		t.Error("a task was launched")
	}
}

// A run with no session turn (a sub-agent, a scheduled task) launches no
// task: the field is not offered, and a call that has it is refused.
func TestAgentWorkflow_NoTaskOutsideASessionTurn(t *testing.T) {
	e := newTurnEnv(t, provider.ToolCallInfo{ID: "c1", Name: "slow_tool", Input: json.RawMessage(`{"background":true}`)})
	e.env.ExecuteWorkflow(AgentWorkflow, AgentWorkflowInput{SessionID: "s1:p:jarvis:m1:tool:agent_x:c9", AgentID: "jarvis", UserMessage: "go"})
	if r := e.result(t); !r.IsError || !strings.Contains(r.Content, "session's turn only") {
		t.Errorf("result %+v", r)
	}
	for _, d := range e.f.model.sent()[0].Tools {
		if strings.Contains(string(d.InputSchema), `"background"`) {
			t.Errorf("%s offered the field", d.Name)
		}
	}
	if len(e.launched) != 0 || len(e.tasks.registered) != 0 || len(e.children) != 0 {
		t.Error("something ran")
	}
}

// A task's end is a message for its participant like a member's: it waits
// behind the one being answered, or goes first, by the order they came in.
// The turn it wakes answers the user who asked for the task, and reads it
// framed; a member's message written after it is read after it.
func TestParticipant_AnswersATaskResultInItsTurn(t *testing.T) {
	for _, c := range []struct {
		name      string
		taskFirst bool
	}{{"task first", true}, {"member first", false}} {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, answers(done), nil)
			h.human("analyse cinesense in the background", "Victor")
			task := func() ParticipantMessage {
				ref := store.TaskRef{ID: "s1:p:jarvis:m1:bg:c1", Tool: "analyze_repo", State: store.BackgroundDone, RequestedBy: "Victor"}
				id := h.f.session.add(store.TaskResultKey(ref.ID), store.Message{Role: store.RoleUser, Kind: store.KindTaskResult,
					UserID: "u-victor", AgentID: "jarvis", Content: `"the report"`, Task: &ref})
				return ParticipantMessage{MessageID: id, UserID: "u-victor", UserName: "Victor", Channel: "telegram", ChannelID: "42"}
			}
			var inbox []ParticipantMessage
			if c.taskFirst {
				inbox = append(inbox, task(), h.human("and the tests?", "Alice"))
			} else {
				inbox = append(inbox, h.human("and the tests?", "Alice"), task())
			}
			if err := h.run("jarvis", inbox...); err != nil {
				t.Fatal(err)
			}
			sent := h.f.model.sent()
			if len(sent) != 2 {
				t.Fatalf("%d calls", len(sent))
			}
			taskTurn, memberTurn := sent[0], sent[1]
			if !c.taskFirst {
				taskTurn, memberTurn = sent[1], sent[0]
			}
			last := read(taskTurn)[len(read(taskTurn))-1]
			if !strings.Contains(last, "[Task result: analyze_repo, a background task you started for Victor") || !strings.HasSuffix(last, "the report") {
				t.Errorf("the task's turn read %v", read(taskTurn))
			}
			// The member's message, stored after the task's end, reads it
			// only when it came after it.
			readsTask := strings.Contains(strings.Join(read(memberTurn), "\n"), "[Task result:")
			if readsTask != c.taskFirst {
				t.Errorf("the member's turn read the task: %v; %v", readsTask, read(memberTurn))
			}
			if ends := h.ends(); len(ends) != 2 {
				t.Errorf("ends %v", ends)
			}
		})
	}
}

// A stop of the turn that launched a task (stop-turn, clear: its context
// cancelled) does not reach the task: it was started from a context the
// turn's cancellation does not cancel, and outlives it.
func TestAgentWorkflow_StopOfTheTurnSparesItsTask(t *testing.T) {
	e := newTurnEnv(t,
		provider.ToolCallInfo{ID: "c1", Name: "slow_tool", Input: json.RawMessage(`{"background":true}`)})
	// The second step waits on a call in the foreground, an hour long: the
	// stop comes then.
	e.f.model.answer = answers(
		provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{{ID: "c1", Name: "slow_tool", Input: json.RawMessage(`{"background":true}`)}}},
		provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{{ID: "c2", Name: "slow_tool", Input: json.RawMessage(`{}`)}}},
		done)
	var mu sync.Mutex
	ran, cancelled := false, false
	e.task = func(ctx sdkworkflow.Context, _ BackgroundTaskInput) error {
		mu.Lock()
		ran = true
		mu.Unlock()
		err := sdkworkflow.Sleep(ctx, 2*time.Hour)
		mu.Lock()
		cancelled = err != nil || ctx.Err() != nil
		mu.Unlock()
		return nil
	}
	e.slow = func(ctx sdkworkflow.Context, _ json.RawMessage) (tool.Result, error) {
		if err := sdkworkflow.Sleep(ctx, time.Hour); err != nil {
			return tool.Result{}, err
		}
		return tool.Result{Content: "slow result"}, nil
	}
	e.env.RegisterDelayedCallback(e.env.CancelWorkflow, 10*time.Minute)
	e.runTurn()
	mu.Lock()
	defer mu.Unlock()
	if !ran || cancelled || len(e.tasks.registered) != 1 {
		t.Errorf("task ran %v, cancelled with the turn %v, registered %d", ran, cancelled, len(e.tasks.registered))
	}
}

// A call the model gave no ID: its task, and the files of a sub-agent it
// launches, are named by its place in the turn, never by an empty ID.
func TestAgentWorkflow_CallWithoutID(t *testing.T) {
	e := newTurnEnv(t, provider.ToolCallInfo{Name: "agent_smith", Input: json.RawMessage(`{"task":"review","background":true}`)})
	e.runTurn()
	if len(e.launched) != 1 || len(e.tasks.registered) != 1 {
		t.Fatalf("launched %+v", e.launched)
	}
	var sub AgentWorkflowInput
	json.Unmarshal(e.launched[0].Input, &sub)
	if r := e.tasks.registered[0]; r.ID != "s1:p:jarvis:m1:bg:0-0" || r.CallID != "0-0" || sub.CallPrefix != "0-0/" {
		t.Errorf("task %s, call %q, prefix %q", r.ID, r.CallID, sub.CallPrefix)
	}
}

// A tool that returns its result as the stop comes keeps it: the task ends
// done, its result posted, its participant woken. A sub-agent that returns
// what it had, cancelled, ends the task cancelled.
func TestBackgroundTask_ResultWithTheStop(t *testing.T) {
	e := newTaskEnv(t, func(ctx sdkworkflow.Context, _ json.RawMessage) (tool.Result, error) {
		sdkworkflow.Sleep(ctx, time.Hour) // cancelled: it returns all the same
		return tool.Result{Content: "finished anyway"}, nil
	})
	e.env.RegisterDelayedCallback(e.env.CancelWorkflow, time.Minute)
	e.run()
	results := e.taskResults()
	if len(results) != 1 || results[0].Task.State != store.BackgroundDone || results[0].Content != `"finished anyway"` || len(e.starts.all()) != 1 {
		t.Errorf("results %+v, wakes %d", results, len(e.starts.all()))
	}

	e = newTaskEnv(t, func(sdkworkflow.Context, json.RawMessage) (tool.Result, error) { return tool.Result{}, nil })
	e.env.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, _ AgentWorkflowInput) (AgentWorkflowOutput, error) {
		if err := sdkworkflow.Sleep(ctx, time.Hour); err != nil {
			return cancelledOutput(nil), nil
		}
		return AgentWorkflowOutput{Response: "the review"}, nil
	}, sdkworkflow.RegisterOptions{Name: "FakeAgentWorkflow"})
	e.env.RegisterDelayedCallback(e.env.CancelWorkflow, time.Minute)
	e.env.ExecuteWorkflow(BackgroundTaskWorkflow, BackgroundTaskInput{SessionID: "s1", AgentID: "smith", Tool: "agent_x", SubAgent: true,
		Workflow: "FakeAgentWorkflow", ChildID: testTaskID + ":tool:agent_x:c1", TaskQueue: "agent", Input: json.RawMessage(`{}`)})
	results = e.taskResults()
	if len(results) != 1 || results[0].Task.State != store.BackgroundCancelled || len(e.starts.all()) != 0 {
		t.Errorf("sub-agent: results %+v, wakes %d", results, len(e.starts.all()))
	}
}
