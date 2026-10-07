package activity

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// memTasks is a TaskStore in memory, ending a task as Postgres does: the
// first writer wins, and only the same writer finds it its own.
type memTasks struct {
	mu       sync.Mutex
	sessions map[string]*store.Session
	tasks    map[string]*store.BackgroundTask
	messages []store.Message
}

func newMemTasks(tasks ...store.BackgroundTask) *memTasks {
	m := &memTasks{sessions: map[string]*store.Session{"s1": {SessionID: "s1", AgentID: "jarvis", Channel: "web"}}, tasks: map[string]*store.BackgroundTask{}}
	for _, t := range tasks {
		m.tasks[t.ID] = &t
	}
	return m
}

func (m *memTasks) GetSession(_ context.Context, id string) (*store.Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[id], nil
}
func (m *memTasks) RegisterTask(_ context.Context, t store.BackgroundTask, max int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, have := range m.tasks {
		if have.Participant == t.Participant && have.State == store.BackgroundRunning {
			n++
		}
	}
	if n >= max {
		return store.ErrTooManyTasks
	}
	t.State = store.BackgroundRunning
	m.tasks[t.ID] = &t
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
	t, ok := m.tasks[id]
	if !ok {
		return nil, nil
	}
	c := *t
	return &c, nil
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
	m.messages = append(m.messages, build(*t))
	t.ResultMessageID = int64(len(m.messages))
	return store.TaskEnding{Task: *t, MessageID: t.ResultMessageID, Mine: true}, nil
}

// memPublisher records what is published.
type memPublisher struct {
	files []tool.FileRef
	calls []tool.CallContext
}

func (p *memPublisher) Publish(ctx context.Context, name string, content []byte) (tool.FileRef, error) {
	call, _ := tool.CallFromContext(ctx)
	p.calls = append(p.calls, call)
	f := tool.FileRef{ID: "f1", Name: name, Size: int64(len(content))}
	p.files = append(p.files, f)
	return f, nil
}

func runningTask() store.BackgroundTask {
	return store.BackgroundTask{ID: "s1:p:smith:m4:bg:c1", SessionID: "s1", Participant: "smith", UserID: "u-alice", UserName: "Alice",
		Tool: "analyze_repo", Summary: "cinesense", TurnKey: "m4.smith", CallID: "c1", Channel: "telegram", ChannelID: "42",
		State: store.BackgroundRunning, StartedAt: time.Now().Add(-14 * time.Minute),
		FollowUps: []store.TaskFollowUp{{Text: "then implement it", UserName: "Alice"}}}
}

func TestPostTaskResult_PostsAndSaysWhomToWake(t *testing.T) {
	st := newMemTasks(runningTask())
	a := &TaskActivities{Store: st, Publisher: &memPublisher{}}
	out, err := a.PostTaskResult(context.Background(), PostTaskResultInput{TaskID: "s1:p:smith:m4:bg:c1", State: store.BackgroundDone, Content: "the report"})
	if err != nil {
		t.Fatal(err)
	}
	// Smith is not the session's agent: the woken turn signs, on the
	// launching turn's channel.
	if !out.Wake || out.MessageID != 1 || out.UserID != "u-alice" || out.Channel != "telegram" || out.ChannelID != "42" ||
		!out.SignReply || out.SessionChannel != "web" || out.File != nil {
		t.Errorf("out = %+v", out)
	}
	m := st.messages[0]
	if m.Kind != store.KindTaskResult || m.Role != store.RoleUser || m.Author != "" || m.UserID != "u-alice" || m.AgentID != "smith" ||
		m.Task == nil || m.Task.State != store.BackgroundDone || len(m.Task.FollowUps) != 1 || m.Content != `"the report"` {
		t.Errorf("message = %+v %+v", m, m.Task)
	}
	// Again (a retry): the same message, still its own, woken again.
	again, err := a.PostTaskResult(context.Background(), PostTaskResultInput{TaskID: "s1:p:smith:m4:bg:c1", State: store.BackgroundDone, Content: "the report"})
	if err != nil || !again.Wake || again.MessageID != 1 || len(st.messages) != 1 {
		t.Errorf("retry: %+v %v, %d messages", again, err, len(st.messages))
	}
}

// A result past the message's bound is published as a file of the call
// first; the message keeps its start and names the file.
func TestPostTaskResult_LongResultAsFile(t *testing.T) {
	st := newMemTasks(runningTask())
	pub := &memPublisher{}
	a := &TaskActivities{Store: st, Publisher: pub}
	long := strings.Repeat("é", MaxTaskMessageBytes) // twice the bound, in bytes
	out, err := a.PostTaskResult(context.Background(), PostTaskResultInput{TaskID: "s1:p:smith:m4:bg:c1", State: store.BackgroundDone, Content: long})
	if err != nil {
		t.Fatal(err)
	}
	if len(pub.files) != 1 || pub.files[0].Name != "resultat-analyze_repo.md" || pub.files[0].Size != int64(len(long)) {
		t.Fatalf("published %+v", pub.files)
	}
	if c := pub.calls[0]; c.Turn == nil || c.Turn.TurnKey != "m4.smith" || c.CallID != "c1" {
		t.Errorf("published under %+v", c)
	}
	if out.File == nil || out.File.ID != "f1" {
		t.Errorf("out file %+v", out.File)
	}
	m := st.messages[0]
	var text string
	json.Unmarshal([]byte(m.Content), &text)
	if len(text) > MaxTaskMessageBytes || !strings.HasPrefix(long, text) || len(text) < MaxTaskMessageBytes-1 {
		t.Errorf("kept %d bytes", len(text))
	}
	if m.Task.File == nil || m.Task.File.Name != "resultat-analyze_repo.md" {
		t.Errorf("the message names %+v", m.Task.File)
	}
}

// The sweep ended it first: nothing more, nobody woken by this one. A
// cancelled task wakes nobody. A task gone with its session writes nothing.
func TestPostTaskResult_FirstWriterWins(t *testing.T) {
	task := runningTask()
	st := newMemTasks(task)
	if _, err := st.EndTask(context.Background(), task.ID, store.TaskEndedBySweep, store.BackgroundFailed, func(t store.BackgroundTask) store.Message {
		return TaskResultMessage(t, "stopped without a result", nil)
	}); err != nil {
		t.Fatal(err)
	}
	a := &TaskActivities{Store: st}
	out, err := a.PostTaskResult(context.Background(), PostTaskResultInput{TaskID: task.ID, State: store.BackgroundDone, Content: "late"})
	if err != nil || out.Wake || len(st.messages) != 1 {
		t.Errorf("after the sweep: %+v %v, %d messages", out, err, len(st.messages))
	}

	st = newMemTasks(task)
	a = &TaskActivities{Store: st}
	out, err = a.PostTaskResult(context.Background(), PostTaskResultInput{TaskID: task.ID, State: store.BackgroundCancelled})
	if err != nil || out.Wake || out.MessageID != 1 || st.messages[0].Task.State != store.BackgroundCancelled {
		t.Errorf("cancelled: %+v %v", out, err)
	}

	st = newMemTasks()
	a = &TaskActivities{Store: st}
	if out, err := a.PostTaskResult(context.Background(), PostTaskResultInput{TaskID: task.ID, State: store.BackgroundDone, Content: "x"}); err != nil || !out.Gone || out.Wake {
		t.Errorf("gone: %+v %v", out, err)
	}

	// The session deleted after the task ended: written, nobody woken.
	st = newMemTasks(task)
	delete(st.sessions, "s1")
	a = &TaskActivities{Store: st}
	if out, err := a.PostTaskResult(context.Background(), PostTaskResultInput{TaskID: task.ID, State: store.BackgroundDone, Content: "x"}); err != nil || out.Wake {
		t.Errorf("session gone: %+v %v", out, err)
	}
}

func TestRegisterTask_RefusesPastTheCap(t *testing.T) {
	st := newMemTasks()
	a := &TaskActivities{Store: st}
	for i := range MaxRunningTasks + 1 {
		task := runningTask()
		task.ID += string(rune('a' + i))
		out, err := a.RegisterTask(context.Background(), RegisterTaskInput{Task: task})
		if err != nil {
			t.Fatal(err)
		}
		if refused := out.Refused != ""; refused != (i == MaxRunningTasks) {
			t.Errorf("task %d: refused %q", i, out.Refused)
		}
	}
}

func TestTaskSummary(t *testing.T) {
	for in, want := range map[string]string{
		`{"repo":"github.com/x/cinesense","question":"what  does\nit do?","depth":3}`: "github.com/x/cinesense · what does it do?",
		`{"task":"review"}`: "review",
		`"x"`:               "",
		`{}`:                "",
	} {
		if got := TaskSummary(json.RawMessage(in)); got != want {
			t.Errorf("%s: %q, want %q", in, got, want)
		}
	}
	if got := TaskSummary(json.RawMessage(`{"task":"` + strings.Repeat("a", 500) + `"}`)); len([]rune(got)) != maxTaskSummaryRunes {
		t.Errorf("clipped to %d runes", len([]rune(got)))
	}
}

// The prompt says how to launch a task when a tool offers it, and lists
// the tasks running, with what to do meanwhile.
func TestTasksSection(t *testing.T) {
	if s := TasksSection(nil, false, true); s != "" {
		t.Errorf("nothing to say: %q", s)
	}
	s := TasksSection(nil, true, true)
	if !strings.Contains(s, "`background` field") || !strings.Contains(s, "By default, wait") || !strings.Contains(s, "when_task_done") {
		t.Errorf("how to: %q", s)
	}
	if strings.Contains(TasksSection(nil, true, false), "when_task_done") {
		t.Error("when_task_done named, not offered")
	}
	task := runningTask()
	task.StartedAt = time.Date(2026, 10, 7, 10, 2, 0, 0, time.UTC)
	s = TasksSection([]store.BackgroundTask{task}, false, true)
	for _, want := range []string{"Never start one of them again", task.ID + ": analyze_repo (cinesense), for Alice, since 2026-10-07 10:02 UTC, 1 instruction(s) attached"} {
		if !strings.Contains(s, want) {
			t.Errorf("running: %q lacks %q", s, want)
		}
	}
}

// A delivery that names its session reads it first: deleted, nothing is
// delivered.
func TestRelay_SessionGoneDeliversNothing(t *testing.T) {
	starter := &recordStarter{}
	st := newMemTasks()
	a := &RelayActivities{Client: starter, Sessions: st}
	if err := a.Relay(context.Background(), RelayInput{WorkflowID: "s2:p:jarvis", SessionID: "s2"}); err != nil || starter.n != 0 {
		t.Errorf("gone: %v, %d deliveries", err, starter.n)
	}
	if err := a.Relay(context.Background(), RelayInput{WorkflowID: "s1:p:jarvis", SessionID: "s1"}); err != nil || starter.n != 1 {
		t.Errorf("there: %v, %d deliveries", err, starter.n)
	}
}

type recordStarter struct{ n int }

func (r *recordStarter) SignalWithStartWorkflow(context.Context, string, string, interface{}, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error) {
	r.n++
	return nil, nil
}
