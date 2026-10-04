package workflow

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
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

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
)

// Tests against a real Temporal server: what the SDK's test environment does
// not simulate. Skipped unless TEMPORAL_SMOKE_HOST names one (host:port);
// TEMPORAL_SMOKE_NAMESPACE (default "default") and TEMPORAL_SMOKE_MESSAGES
// (default 200) tune them. They run on a task queue of their own, with the
// turn and its activities stubbed in process: no database, no model. See
// CLAUDE.md for the command.

// smokeLogs keeps what the worker logs at error.
type smokeLogs struct {
	mu     sync.Mutex
	errors []string
}

func (l *smokeLogs) Debug(string, ...interface{}) {}
func (l *smokeLogs) Info(string, ...interface{})  {}
func (l *smokeLogs) Warn(string, ...interface{})  {}
func (l *smokeLogs) Error(msg string, kv ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, fmt.Sprint(msg, " ", kv))
}

func (l *smokeLogs) contain(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.errors {
		if strings.Contains(e, s) {
			return true
		}
	}
	return false
}

func smokeSuffix(t *testing.T) string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// A signal that arrives while a participant ends is not lost: the server
// refuses to close a workflow with a signal it did not handle
// (UNHANDLED_COMMAND), and the next workflow task gets it. Each message is
// sent once its predecessor's turn_done activity returned, after a random
// pause, so that it lands before, during or after the task that ends the
// run. Every message must be answered exactly once. With short pauses, the
// participant rarely ends and may continue as new, carrying its inbox.
func TestParticipant_NoSignalLostOnExit_RealServer(t *testing.T) {
	host := os.Getenv("TEMPORAL_SMOKE_HOST")
	if host == "" {
		t.Skip("TEMPORAL_SMOKE_HOST is not set: no real Temporal server to test against")
	}
	namespace := cmp.Or(os.Getenv("TEMPORAL_SMOKE_NAMESPACE"), "default")
	n := 200
	if v := os.Getenv("TEMPORAL_SMOKE_MESSAGES"); v != "" {
		var err error
		if n, err = strconv.Atoi(v); err != nil || n < 1 {
			t.Fatalf("TEMPORAL_SMOKE_MESSAGES=%q", v)
		}
	}
	for _, c := range []struct {
		name     string
		maxPause time.Duration
	}{
		{"pauses up to 150ms", 150 * time.Millisecond},
		{"pauses up to 25ms", 25 * time.Millisecond},
	} {
		t.Run(c.name, func(t *testing.T) { smokeNoSignalLost(t, host, namespace, n, c.maxPause) })
	}
}

func smokeNoSignalLost(t *testing.T, host, namespace string, n int, maxPause time.Duration) {
	logs := &smokeLogs{}
	c, err := client.Dial(client.Options{HostPort: host, Namespace: namespace, Logger: logs})
	if err != nil {
		t.Fatalf("dial %s: %v", host, err)
	}
	defer c.Close()
	suffix := smokeSuffix(t)
	queue := "smoke-signals-" + suffix
	sessionID := "smoke-" + suffix
	participantID := ParticipantWorkflowID(sessionID, "jarvis")
	t.Logf("queue %s, participant %s, %d messages", queue, participantID, n)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	// Whatever happens, nothing is left running.
	defer func() {
		err := c.TerminateWorkflow(context.Background(), participantID, "", "smoke test over")
		var notFound *serviceerror.NotFound
		if err != nil && !errors.As(err, &notFound) {
			t.Logf("terminate %s: %v", participantID, err)
		}
	}()

	var mu sync.Mutex
	answered := map[int64]int{} // EndTurn calls, by message
	done := make(chan int64, 4*n)
	w := worker.New(c, queue, worker.Options{})
	w.RegisterWorkflow(ParticipantWorkflow)
	w.RegisterWorkflowWithOptions(func(sdkworkflow.Context, AgentWorkflowInput) (AgentWorkflowOutput, error) {
		return AgentWorkflowOutput{Response: "done"}, nil
	}, sdkworkflow.RegisterOptions{Name: "AgentWorkflow"})
	// No dedup in the check: a message answered twice must show.
	w.RegisterActivityWithOptions(func(context.Context, activity.CheckTurnInput) (activity.CheckTurnOutput, error) {
		return activity.CheckTurnOutput{AgentName: "Jarvis"}, nil
	}, sdkactivity.RegisterOptions{Name: "CheckTurn"})
	w.RegisterActivityWithOptions(func(_ context.Context, in activity.EndTurnInput) (int64, error) {
		anchor, _ := store.TurnAnchor(in.TurnKey)
		mu.Lock()
		answered[anchor]++
		mu.Unlock()
		return anchor, nil
	}, sdkactivity.RegisterOptions{Name: "EndTurn"})
	w.RegisterActivityWithOptions(func(_ context.Context, in activity.NotifyInput) error {
		if in.Event.Type == EventTurnDone {
			var e TurnEvent
			json.Unmarshal(in.Event.Data, &e)
			anchor, _ := store.TurnAnchor(e.Turn)
			done <- anchor
		}
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
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
	// The next message goes once the last one's turn_done returned, after
	// a pause; all are answered when every turn_done came.
	sent := map[int64]bool{1: true}
	seen := map[int64]bool{}
	for len(seen) < n {
		select {
		case id := <-done:
			seen[id] = true
			if next := id + 1; next <= int64(n) && !sent[next] {
				sent[next] = true
				time.Sleep(time.Duration(mathrand.Int64N(int64(maxPause) + 1)))
				if err := send(next); err != nil {
					t.Fatalf("send %d: %v", next, err)
				}
			}
		case <-time.After(time.Minute):
			t.Fatalf("no turn done for a minute: %d of %d messages done", len(seen), n)
		}
	}

	// The participant ends, and nothing more is answered.
	runs := smokeRunsClosed(ctx, t, c, participantID)
	time.Sleep(2 * time.Second)

	mu.Lock()
	once, twice, never := 0, 0, 0
	for id := int64(1); id <= int64(n); id++ {
		switch answered[id] {
		case 0:
			never++
		case 1:
			once++
		default:
			twice++
		}
	}
	extra := len(answered) - once - twice
	mu.Unlock()

	completed, continued, refused := 0, 0, 0
	var other []string
	for _, r := range runs {
		switch r.GetStatus() {
		case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
			completed++
		case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
			continued++
		default:
			other = append(other, r.GetExecution().GetRunId()+" "+r.GetStatus().String())
		}
		refused += smokeUnhandledCommands(ctx, t, c, participantID, r.GetExecution().GetRunId())
	}
	t.Logf("messages %d: answered once %d, twice or more %d, never %d", n, once, twice, never)
	t.Logf("runs %d (completed %d, continued as new %d, other %d); workflow tasks refused for an unhandled signal (UNHANDLED_COMMAND): %d",
		len(runs), completed, continued, len(other), refused)
	if once != n || twice != 0 || never != 0 || extra != 0 {
		t.Errorf("answered once %d of %d, twice %d, never %d, unknown messages %d", once, n, twice, never, extra)
	}
	if len(other) > 0 {
		t.Errorf("runs neither completed nor continued as new: %v", other)
	}
	if logs.contain("unhandled signals") {
		t.Errorf("a participant ended with unhandled signals: %v", logs.errors)
	}
}

// smokeRunsClosed waits until every run of the workflow is closed, as the
// visibility lists them, and returns them.
func smokeRunsClosed(ctx context.Context, t *testing.T, c client.Client, workflowID string) []*workflowpb.WorkflowExecutionInfo {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		var runs []*workflowpb.WorkflowExecutionInfo
		var token []byte
		for {
			resp, err := c.ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{Query: fmt.Sprintf("WorkflowId = '%s'", workflowID), NextPageToken: token})
			if err != nil {
				t.Fatalf("list runs: %v", err)
			}
			runs = append(runs, resp.GetExecutions()...)
			if token = resp.GetNextPageToken(); len(token) == 0 {
				break
			}
		}
		open := 0
		for _, r := range runs {
			if r.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				open++
			}
		}
		if open == 0 && len(runs) > 0 {
			return runs
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d runs still open after a minute", open, len(runs))
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// smokeUnhandledCommands counts the workflow tasks of a run the server
// refused for a signal that came while it closed.
func smokeUnhandledCommands(ctx context.Context, t *testing.T, c client.Client, workflowID, runID string) int {
	t.Helper()
	n := 0
	it := c.GetWorkflowHistory(ctx, workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for it.HasNext() {
		e, err := it.Next()
		if err != nil {
			t.Fatalf("history of %s: %v", runID, err)
		}
		if a := e.GetWorkflowTaskFailedEventAttributes(); a != nil && a.GetCause() == enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND {
			n++
		}
	}
	return n
}
