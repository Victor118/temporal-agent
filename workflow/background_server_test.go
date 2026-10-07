package workflow

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.temporal.io/api/serviceerror"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/provider"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// taskMessageBase numbers the messages that end the background tasks of
// the test below, apart from the members' (1…n).
const taskMessageBase = 100000

// A task's end wakes its participant by SignalWithStart, whether the
// participant still runs, is ending, or ended long ago: every task's end is
// answered once, and every member's message once. The real participant,
// turn and task workflows, on a real server; the model, the store and the
// tool stubbed in process. Each member's message makes its turn launch a
// task in the background, which ends after a random pause and wakes the
// participant. After an even message, the next one goes once the task's
// turn is done: the participant ends between the two, and the wake starts
// it again. After an odd one, the next goes as soon as its own turn is
// done: the task's end comes while the participant answers it, and waits.
func TestBackgroundTask_WakesTheParticipant_RealServer(t *testing.T) {
	host := os.Getenv("TEMPORAL_SMOKE_HOST")
	if host == "" {
		t.Skip("TEMPORAL_SMOKE_HOST is not set: no real Temporal server to test against")
	}
	namespace := cmp.Or(os.Getenv("TEMPORAL_SMOKE_NAMESPACE"), "default")
	n := 40
	if v := os.Getenv("TEMPORAL_SMOKE_MESSAGES"); v != "" {
		var err error
		if n, err = strconv.Atoi(v); err != nil || n < 1 {
			t.Fatalf("TEMPORAL_SMOKE_MESSAGES=%q", v)
		}
		n = max(n/5, 1) // two turns and a task each
	}
	logs := &smokeLogs{}
	c, err := client.Dial(client.Options{HostPort: host, Namespace: namespace, Logger: logs})
	if err != nil {
		t.Fatalf("dial %s: %v", host, err)
	}
	defer c.Close()
	suffix := smokeSuffix(t)
	queue := "smoke-tasks-" + suffix
	sessionID := "smoke-" + suffix
	participantID := ParticipantWorkflowID(sessionID, "jarvis")
	t.Logf("queue %s, participant %s, %d messages and as many tasks", queue, participantID, n)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	defer func() {
		err := c.TerminateWorkflow(context.Background(), participantID, "", "smoke test over")
		var notFound *serviceerror.NotFound
		if err != nil && !errors.As(err, &notFound) {
			t.Logf("terminate %s: %v", participantID, err)
		}
	}()

	var mu sync.Mutex
	answered := map[int64]int{} // turns ended, by message
	llmCalls := map[string]int{}
	done := make(chan int64, 4*n)
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflow(ParticipantWorkflow)
	w.RegisterWorkflow(AgentWorkflow)
	w.RegisterWorkflow(BackgroundTaskWorkflow)
	w.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, _ json.RawMessage) (tool.Result, error) {
		var pause time.Duration
		sdkworkflow.SideEffect(ctx, func(sdkworkflow.Context) any { return time.Duration(mathrand.Int64N(int64(60 * time.Millisecond))) }).Get(&pause)
		sdkworkflow.Sleep(ctx, pause)
		return tool.Result{Content: "the report"}, nil
	}, sdkworkflow.RegisterOptions{Name: "SmokeToolWorkflow"})
	register := func(f any, name string) { w.RegisterActivityWithOptions(f, sdkactivity.RegisterOptions{Name: name}) }
	register(func(context.Context, activity.CheckTurnInput) (activity.CheckTurnOutput, error) {
		return activity.CheckTurnOutput{AgentName: "Jarvis"}, nil // no dedup: a turn twice must show
	}, "CheckTurn")
	register(func(_ context.Context, in activity.EndTurnInput) (int64, error) {
		anchor, _ := store.TurnAnchor(in.TurnKey)
		mu.Lock()
		answered[anchor]++
		mu.Unlock()
		return anchor, nil
	}, "EndTurn")
	register(func(_ context.Context, in activity.NotifyInput) error {
		if in.Event.Type == EventTurnDone {
			var e TurnEvent
			json.Unmarshal(in.Event.Data, &e)
			anchor, _ := store.TurnAnchor(e.Turn)
			done <- anchor
		}
		return nil
	}, "NotifyStep")
	register(func(context.Context, activity.LoadSkillsForAgentInput) (activity.LoadSkillsForAgentOutput, error) {
		return activity.LoadSkillsForAgentOutput{Name: "Jarvis"}, nil
	}, "LoadSkillsForAgent")
	register(func(context.Context, activity.ListToolsInput) (activity.ListToolsOutput, error) {
		return activity.ListToolsOutput{
			Tools:       []provider.ToolDefinition{{Name: "smoke_tool"}},
			Resolutions: map[string]activity.ToolResolution{"smoke_tool": {Kind: "workflow", WorkflowName: "SmokeToolWorkflow", TaskQueue: queue, Background: true}},
		}, nil
	}, "ListTools")
	register(func(context.Context, activity.PersistContextInput) error { return nil }, "PersistContext")
	// A member's message: launch the task, then answer. A task's end: answer.
	register(func(_ context.Context, req activity.LLMTurnRequest) (activity.LLMTurnResponse, error) {
		turn := req.History.TurnKey
		anchor, _ := store.TurnAnchor(turn)
		mu.Lock()
		llmCalls[turn]++
		call := llmCalls[turn]
		mu.Unlock()
		if anchor < taskMessageBase && call == 1 {
			return activity.LLMTurnResponse{ChatResponse: provider.ChatResponse{ToolCalls: []provider.ToolCallInfo{
				{ID: fmt.Sprintf("c%d", anchor), Name: "smoke_tool", Input: json.RawMessage(`{"background":true}`)},
			}}}, nil
		}
		return activity.LLMTurnResponse{ChatResponse: provider.ChatResponse{Content: "ok", StopReason: "end_turn"}}, nil
	}, "CallLLM")
	register(func(context.Context, activity.RegisterTaskInput) (activity.RegisterTaskOutput, error) {
		return activity.RegisterTaskOutput{}, nil
	}, "RegisterTask")
	register(func(_ context.Context, in activity.PostTaskResultInput) (activity.PostTaskResultOutput, error) {
		// "<session>:p:jarvis:m<i>:bg:c<i>": its message is taskMessageBase+i.
		i, _ := strconv.ParseInt(in.TaskID[strings.LastIndex(in.TaskID, ":c")+2:], 10, 64)
		return activity.PostTaskResultOutput{MessageID: taskMessageBase + i, Wake: true, SessionID: sessionID, Participant: "jarvis",
			UserID: "u-smoke", UserName: "Smoke"}, nil
	}, "PostTaskResult")
	register(func(context.Context, string) error { return nil }, "TaskWoken")
	w.RegisterActivity(&activity.RelayActivities{Client: c, Sessions: smokeSessions{}})
	if err := w.Start(); err != nil {
		t.Fatalf("worker: %v", err)
	}
	defer w.Stop()

	send := func(id int64) error {
		_, err := c.SignalWithStartWorkflow(ctx, participantID, SignalMessage,
			ParticipantMessage{MessageID: id, UserID: "u-smoke", UserName: "Smoke"},
			client.StartWorkflowOptions{ID: participantID, TaskQueue: queue},
			ParticipantWorkflow, ParticipantInput{SessionID: sessionID, AgentID: "jarvis"})
		return err
	}
	if err := send(1); err != nil {
		t.Fatalf("send 1: %v", err)
	}
	seen, sent := map[int64]bool{}, map[int64]bool{1: true}
	for len(seen) < 2*n {
		select {
		case id := <-done:
			seen[id] = true
			next := int64(0)
			switch {
			case id < taskMessageBase && id%2 == 1:
				next = id + 1
			case id > taskMessageBase && (id-taskMessageBase)%2 == 0:
				next = id - taskMessageBase + 1
			}
			if next > 0 && next <= int64(n) && !sent[next] {
				sent[next] = true
				if err := send(next); err != nil {
					t.Fatalf("send %d: %v", next, err)
				}
			}
		case <-time.After(time.Minute):
			t.Fatalf("no turn done for a minute: %d of %d turns done", len(seen), 2*n)
		}
	}

	runs := smokeRunsClosed(ctx, t, c, participantID)
	time.Sleep(2 * time.Second)
	mu.Lock()
	var wrong []string
	for i := int64(1); i <= int64(n); i++ {
		for _, id := range []int64{i, taskMessageBase + i} {
			if answered[id] != 1 {
				wrong = append(wrong, fmt.Sprintf("%d answered %d times", id, answered[id]))
			}
		}
	}
	extra := len(answered) - 2*n
	mu.Unlock()
	refused := 0
	for _, r := range runs {
		refused += smokeUnhandledCommands(ctx, t, c, participantID, r.GetExecution().GetRunId())
	}
	t.Logf("%d participant runs; workflow tasks refused for an unhandled signal (UNHANDLED_COMMAND): %d", len(runs), refused)
	if len(wrong) > 0 || extra != 0 {
		t.Errorf("%v; unknown messages %d", wrong, extra)
	}
	if len(runs) < 2 {
		t.Errorf("the participant never ended between a turn and its task's end: %d run", len(runs))
	}
	if logs.contain("unhandled signals") {
		t.Errorf("a participant ended with unhandled signals: %v", logs.errors)
	}
}

// smokeSessions says every session is there.
type smokeSessions struct{}

func (smokeSessions) GetSession(_ context.Context, id string) (*store.Session, error) {
	return &store.Session{SessionID: id}, nil
}
