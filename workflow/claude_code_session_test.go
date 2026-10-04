package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	enumspb "go.temporal.io/api/enums/v1"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/tool"
)

// testRunQueue is the tool's queue the coding workflows run on in the tests.
const testRunQueue = "tools-claude-code"

// The SDK's names for the activities that open and close a session.
const (
	sessionCreation   = "internalSessionCreationActivity"
	sessionCompletion = "internalSessionCompletionActivity"
)

// testRunWorkflowID is the coding workflow's ID in the tests: a tool call of
// jarvis's turn in session "s1" (childWorkflowID).
const testRunWorkflowID = "s1:p:jarvis:m3:tool:implement_feature:c1"

// activityQueues records the task queue each activity ran on, in order, and
// the notifications sent.
type activityQueues struct {
	mu      sync.Mutex
	runs    []activityRun
	notices []activity.NotifyInput
}

type activityRun struct{ name, queue string }

func (q *activityQueues) all() []activityRun {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]activityRun(nil), q.runs...)
}

func (q *activityQueues) sent() []activity.NotifyInput {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]activity.NotifyInput(nil), q.notices...)
}

// asRunWorker makes env what a coding worker is: one of testRunQueue that
// takes sessions (EnableSessionWorker), and answers the probe. It records
// where each activity ran, and the notifications.
func asRunWorker(env *testsuite.TestWorkflowEnvironment) *activityQueues {
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: testRunWorkflowID, TaskQueue: testRunQueue})
	q := &activityQueues{}
	env.SetOnActivityStartedListener(func(info *sdkactivity.Info, _ context.Context, _ converter.EncodedValues) {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.runs = append(q.runs, activityRun{info.ActivityType.Name, info.TaskQueue})
	})
	env.RegisterActivityWithOptions(func(context.Context) (activity.ProbeRunWorkerOutput, error) {
		return activity.ProbeRunWorkerOutput{QueueWaitSeconds: int64(activity.DefaultRunQueueWait / time.Second)}, nil
	}, sdkactivity.RegisterOptions{Name: probeActivity})
	env.RegisterActivityWithOptions(func(_ context.Context, in activity.NotifyInput) error {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.notices = append(q.notices, in)
		return nil
	}, sdkactivity.RegisterOptions{Name: "NotifyStep"})
	return q
}

// probeActivity is the probe's name (ClaudeCodeActivities.ProbeRunWorker).
const probeActivity = "ProbeRunWorker"

// checkOneWorker fails unless the queue was probed, the session was asked of
// testRunQueue, every step of the run went to the session's own queue, and
// the session was completed last.
func checkOneWorker(t *testing.T, runs []activityRun, steps ...string) {
	t.Helper()
	if len(runs) < 2 || runs[0].name != probeActivity || runs[1].name != sessionCreation {
		t.Fatalf("activities %v, want the queue probed, then the session opened", runs)
	}
	if runs[0].queue != testRunQueue {
		t.Errorf("probed %q, want %q", runs[0].queue, testRunQueue)
	}
	if want := testRunQueue + "__internal_session_creation"; runs[1].queue != want {
		t.Errorf("session asked of %q, want %q", runs[1].queue, want)
	}
	var got []string
	for _, r := range runs[2:] {
		if r.name == sessionCompletion {
			continue
		}
		got = append(got, r.name)
		// The session's queue is its worker's alone: "<resource>@<host>".
		if r.queue == testRunQueue || !strings.Contains(r.queue, "@") {
			t.Errorf("%s ran on %q, not on the session's worker", r.name, r.queue)
		}
		if r.queue != runs[2].queue {
			t.Errorf("%s ran on %q, %s on %q: two workers", r.name, r.queue, runs[2].name, runs[2].queue)
		}
	}
	if strings.Join(got, " ") != strings.Join(steps, " ") {
		t.Errorf("steps %v, want %v", got, steps)
	}
	if last := runs[len(runs)-1]; last.name != sessionCompletion {
		t.Errorf("last activity %s, want the session completed after the cleanup", last.name)
	}
}

func TestAnalyzeRepoWorkflow_RunsOnOneWorker(t *testing.T) {
	a := newAnalyzeEnv(t, nil, claudeCodeResult{Report: "ok", Subtype: "success"}, nil)
	a.run_(t, AnalyzeRepoInput{Repo: "/src/repo", Task: "look"})
	checkOneWorker(t, a.queues.all(), "PrepareWorkspace", "RunClaudeCode", "CleanupWorkspace")
}

func TestImplementFeatureWorkflow_RunsOnOneWorker(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "Done.", Subtype: "success"}, nil, oneCommit(), nil)
	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})
	if !out.Pushed {
		t.Fatalf("not pushed: %s", out.Error)
	}
	checkOneWorker(t, e.queues.all(), "PrepareWorkspace", "RunClaudeCode", "InspectWorkspace", "PushBranch", "CleanupWorkspace")
}

// No worker answers the probe: the run says so at once, names the queue,
// and asks for no session.
func TestCodingRuns_NoWorkerAvailable(t *testing.T) {
	noWorker := func(env *testsuite.TestWorkflowEnvironment) {
		env.OnActivity(probeActivity, mock.Anything).
			Return(activity.ProbeRunWorkerOutput{}, temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, nil))
	}
	const want = `no worker available for "tools-claude-code"`
	noSession := func(t *testing.T, q *activityQueues) {
		t.Helper()
		for _, r := range q.all() {
			if r.name != probeActivity {
				t.Errorf("%s ran with no worker", r.name)
			}
		}
	}

	a := newAnalyzeEnv(t, nil, claudeCodeResult{Report: "ok", Subtype: "success"}, nil)
	noWorker(a.env)
	out := a.run_(t, AnalyzeRepoInput{Repo: "/src/repo", Task: "look"})
	if !strings.Contains(out.Error, want) || !strings.Contains(out.Content, want) {
		t.Errorf("analyze: Error = %q", out.Error)
	}
	noSession(t, a.queues)

	e := newImplementEnv(t, claudeCodeResult{Report: "ok", Subtype: "success"}, nil, oneCommit(), nil)
	noWorker(e.env)
	iout := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})
	if !strings.Contains(iout.Error, want) || iout.Pushed {
		t.Errorf("implement: Error = %q, pushed %v", iout.Error, iout.Pushed)
	}
	noSession(t, e.queues)
}

// A worker of the queue has no CLI (a misconfiguration): the run fails at
// once, saying so, and asks for no session.
func TestCodingRuns_WorkerWithoutCLI(t *testing.T) {
	a := newAnalyzeEnv(t, nil, claudeCodeResult{Report: "ok", Subtype: "success"}, nil)
	a.env.OnActivity(probeActivity, mock.Anything).Return(activity.ProbeRunWorkerOutput{},
		temporal.NewNonRetryableApplicationError("no claude CLI", activity.ErrNoClaudeCLI, nil))
	out := a.run_(t, AnalyzeRepoInput{Repo: "/src/repo", Task: "look"})
	if want := `a worker of "tools-claude-code" cannot run Claude Code (no CLI installed)`; !strings.Contains(out.Error, want) {
		t.Errorf("Error = %q, want %q", out.Error, want)
	}
	for _, r := range a.queues.all() {
		if r.name != probeActivity {
			t.Errorf("%s ran on a worker without the CLI", r.name)
		}
	}
}

// Every worker answers, none has a slot to spare for as long as the queue's
// wait, which the worker that answered the probe says: the run says they are
// busy and to try again later. The operator's setting is the worker's log's
// business, not the model's.
func TestCodingRuns_AllWorkersBusy(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "ok", Subtype: "success"}, nil, oneCommit(), nil)
	e.env.OnActivity(probeActivity, mock.Anything).Return(activity.ProbeRunWorkerOutput{QueueWaitSeconds: 7 * 60}, nil)
	e.env.OnActivity(sessionCreation, mock.Anything, mock.Anything).
		Return(temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, nil))

	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	want := `the workers of "tools-claude-code" are all busy (maximum runs reached); waited 7m0s; try again later`
	if !strings.Contains(out.Error, want) {
		t.Errorf("Error = %q, want %q", out.Error, want)
	}
	if strings.Contains(out.Error, "CLAUDE_CODE") {
		t.Errorf("Error = %q names an operator's setting", out.Error)
	}
	if e.run != nil || e.pushed != nil || len(e.cleaned) != 0 {
		t.Error("a step ran with no worker reserved")
	}
}

// workerTakesSessionAfter stands for a worker that frees a slot after d: the
// session opens then, on "resource@host-a".
func workerTakesSessionAfter(env *testsuite.TestWorkflowEnvironment, d time.Duration) {
	env.OnActivity(sessionCreation, mock.Anything, mock.Anything).After(d).Return(
		func(_ context.Context, sessionID string) error {
			env.SignalWorkflow(sessionID, map[string]string{
				"Taskqueue": "resource@host-a", "HostName": "host-a", "ResourceID": "resource",
			})
			return nil
		})
}

// A run that waits for a slot more than a minute tells its user so, once, on
// the turn's channel, through the turn's queue; then runs when a slot frees,
// and says, after, that it waits no more (an empty notice).
func TestCodingRuns_WaitForAWorkerThenRun(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "Done.", Subtype: "success"}, nil, oneCommit(), nil)
	workerTakesSessionAfter(e.env, 3*time.Minute)

	out := e.run_(t, ImplementFeatureInput{
		Repo: "/src/repo", Task: "do it",
		CallContext: tool.CallContext{Channel: "telegram", ChannelID: "42", Agent: "Jarvis", NotifyQueue: "agent"},
	})

	if !out.Pushed || out.Error != "" {
		t.Fatalf("pushed %v, Error %q: want the run done once a slot freed", out.Pushed, out.Error)
	}
	notices := e.queues.sent()
	if len(notices) != 2 {
		t.Fatalf("sent %+v, want the notice, then its clear", notices)
	}
	n := notices[0]
	var data struct{ Type, Text, Agent, Participant string }
	json.Unmarshal(n.Event.Data, &data)
	if n.SessionID != "s1" || n.Channel != "telegram" || n.ChannelID != "42" || n.Event.Type != activity.EventNotice ||
		data.Agent != "Jarvis" || data.Participant != "jarvis" || !strings.Contains(data.Text, "attend un worker libre") || !strings.Contains(data.Text, "30 min") {
		t.Errorf("notice %+v %s", n, n.Event.Data)
	}
	checkCleared(t, notices[1])
	for _, r := range e.queues.all() {
		if r.name == "NotifyStep" && r.queue != "agent" {
			t.Errorf("notice sent through %q, want the turn's queue", r.queue)
		}
	}
}

// checkCleared fails unless n says to the session's user, on the turn's
// channel, that the run waits no more: an empty notice.
func checkCleared(t *testing.T, n activity.NotifyInput) {
	t.Helper()
	var data struct{ Text *string }
	json.Unmarshal(n.Event.Data, &data)
	if n.SessionID != "s1" || n.Channel != "telegram" || n.ChannelID != "42" || n.Event.Type != activity.EventNotice ||
		data.Text == nil || *data.Text != "" {
		t.Errorf("clear %+v %s, want an empty notice", n, n.Event.Data)
	}
}

// A run that waited past the notice, then found every worker busy, clears
// the notice before it ends: the turn goes on, and must not show a wait
// that is over.
func TestCodingRuns_AllBusyClearsTheNotice(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "ok", Subtype: "success"}, nil, oneCommit(), nil)
	e.env.OnActivity(probeActivity, mock.Anything).Return(activity.ProbeRunWorkerOutput{QueueWaitSeconds: 7 * 60}, nil)
	e.env.OnActivity(sessionCreation, mock.Anything, mock.Anything).After(7 * time.Minute).
		Return(temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, nil))
	out := e.run_(t, ImplementFeatureInput{
		Repo: "/src/repo", Task: "do it",
		CallContext: tool.CallContext{Channel: "telegram", ChannelID: "42", NotifyQueue: "agent"},
	})
	if !strings.Contains(out.Error, "all busy") {
		t.Fatalf("Error = %q", out.Error)
	}
	notices := e.queues.sent()
	if len(notices) != 2 {
		t.Fatalf("sent %+v, want the notice, then its clear", notices)
	}
	checkCleared(t, notices[1])
}

// A slot free within the minute: no notice.
func TestCodingRuns_NoNoticeWhenASlotIsFree(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "Done.", Subtype: "success"}, nil, oneCommit(), nil)
	workerTakesSessionAfter(e.env, 30*time.Second)
	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it", CallContext: tool.CallContext{Channel: "web"}})
	if !out.Pushed {
		t.Fatalf("not pushed: %s", out.Error)
	}
	if notices := e.queues.sent(); len(notices) != 0 {
		t.Errorf("sent %+v, want no notice", notices)
	}
}

// A slot that frees just as the notice is due: the session's answer and the
// notice's timer reach the workflow in one workflow task. The wait is over:
// no notice, and so nothing to clear.
//
// The worker's answer is delivered without a workflow task of its own
// (SignalWorkflowSkippingWorkflowTask), before the timer fires: the task the
// timer starts sees both. The session's worker keeps its creation activity
// running, as a real one does for the session's length.
func TestCodingRuns_NoNoticeWhenTheSlotFreesAsItIsDue(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "Done.", Subtype: "success"}, nil, oneCommit(), nil)
	e.env.OnActivity(sessionCreation, mock.Anything, mock.Anything).After(time.Hour).Return(nil)
	q := e.queues
	e.env.SetOnActivityStartedListener(func(info *sdkactivity.Info, _ context.Context, args converter.EncodedValues) {
		q.mu.Lock()
		q.runs = append(q.runs, activityRun{info.ActivityType.Name, info.TaskQueue})
		q.mu.Unlock()
		if info.ActivityType.Name != sessionCreation {
			return
		}
		var sessionID string
		if err := args.Get(&sessionID); err != nil {
			t.Error(err)
			return
		}
		e.env.RegisterDelayedCallback(func() {
			e.env.SignalWorkflowSkippingWorkflowTask(sessionID, map[string]string{
				"Taskqueue": "resource@host-a", "HostName": "host-a", "ResourceID": "resource",
			})
		}, runWaitNotice/2)
	})
	out := e.run_(t, ImplementFeatureInput{
		Repo: "/src/repo", Task: "do it",
		CallContext: tool.CallContext{Channel: "telegram", ChannelID: "42", NotifyQueue: "agent"},
	})
	if !out.Pushed {
		t.Fatalf("not pushed: %s", out.Error)
	}
	if notices := e.queues.sent(); len(notices) != 0 {
		t.Errorf("sent %+v, want no notice", notices)
	}
}

// The session of a workflow is what precedes its first ':', and its
// participant follows ":p:": a sub-agent's tools carry them too.
func TestSessionOf(t *testing.T) {
	for id, want := range map[string][2]string{
		"s1:p:jarvis":     {"s1", "jarvis"},
		"s1:p:jarvis:m3":  {"s1", "jarvis"},
		testRunWorkflowID: {"s1", "jarvis"},
		"s1:p:jarvis:m3:tool:agent_analyst:c1:tool:analyze_repo:c2": {"s1", "jarvis"},
		"s1:i:42:m3:tool:ask_user:c1":                               {"s1", ""},
		"sched-1-agent-17:tool:ask_user:c1":                         {"sched-1-agent-17", ""},
	} {
		if got, ok := SessionOf(id); !ok || got != want[0] {
			t.Errorf("SessionOf(%q) = %q, %v; want %q", id, got, ok, want[0])
		}
		if got, ok := ParticipantOf(id); got != want[1] || ok != (want[1] != "") {
			t.Errorf("ParticipantOf(%q) = %q, %v; want %q", id, got, ok, want[1])
		}
	}
	for _, id := range []string{"scheduled-x", "fork-6f1c", "report-a-b", ":p:x"} {
		if s, ok := SessionOf(id); ok {
			t.Errorf("SessionOf(%q) = %q: a session read from an ID with none", id, s)
		}
	}
}

// workerDiesDuringRun stands for the session's worker dying once the run has
// started: the session's activity fails, as the server fails it when the
// worker's heartbeats stop.
func workerDiesDuringRun(env *testsuite.TestWorkflowEnvironment) (duringRun func(ctx context.Context) error) {
	started := make(chan struct{})
	env.OnActivity(sessionCreation, mock.Anything, mock.Anything).Return(
		func(_ context.Context, sessionID string) error {
			// What the session worker answers once it has taken the session.
			env.SignalWorkflow(sessionID, map[string]string{
				"Taskqueue": "resource@host-a", "HostName": "host-a", "ResourceID": "resource",
			})
			select {
			case <-started:
			case <-time.After(5 * time.Second):
			}
			return temporal.NewNonRetryableApplicationError("heartbeat timeout", "TimeoutError", nil)
		})
	return func(ctx context.Context) error {
		close(started)
		// The run is cancelled with the session; a dead worker would never
		// answer at all.
		select {
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
		}
		return ctx.Err()
	}
}

// The worker dies mid-run: the commits are on its disk, so nothing is pushed
// and nothing more is attempted elsewhere, and the output says what happened.
// The cleanup is refused by the SDK (ErrSessionFailed): the clone goes when
// that worker starts again.
func TestImplementFeatureWorkflow_WorkerLostMidRun(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "Done.", Subtype: "success"}, nil, oneCommit(), nil)
	e.duringRun = workerDiesDuringRun(e.env)

	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	if out.Pushed || e.pushed != nil || e.inspects != 0 {
		t.Errorf("pushed %v, push %+v, %d inspections: want nothing after the worker was lost", out.Pushed, e.pushed, e.inspects)
	}
	for _, want := range []string{"the worker that held the run stopped before the run finished", "nothing was pushed"} {
		if !strings.Contains(out.Error, want) {
			t.Errorf("Error = %q, want %q", out.Error, want)
		}
	}
	if len(e.cleaned) != 0 {
		t.Errorf("cleaned %v on another worker than the clone's", e.cleaned)
	}
	for _, r := range e.queues.all() {
		if r.name != probeActivity && r.name != sessionCreation && r.queue != "resource@host-a" {
			t.Errorf("%s ran on %q, off the lost worker", r.name, r.queue)
		}
	}
}

func TestAnalyzeRepoWorkflow_WorkerLostMidRun(t *testing.T) {
	a := newAnalyzeEnv(t, nil, claudeCodeResult{Report: "ok", Subtype: "success"}, nil)
	a.duringRun = workerDiesDuringRun(a.env)

	out := a.run_(t, AnalyzeRepoInput{Repo: "/src/repo", Task: "look"})

	if !strings.Contains(out.Error, "the worker that held the run stopped before the analysis finished") {
		t.Errorf("Error = %q", out.Error)
	}
	// Cancelled with its session, the run says nothing of how far it got:
	// not that it did nothing, nor that it cost nothing.
	if !out.Interrupted || !strings.Contains(out.Content, "progress unknown; cost unknown (run interrupted)") {
		t.Errorf("content:\n%s", out.Content)
	}
	if len(a.cleaned) != 0 {
		t.Errorf("cleaned %v on another worker than the clone's", a.cleaned)
	}
}

// A worker that stops gracefully cancels its run itself: the session is not
// failed (the SDK only fails it on a timeout), but the workflow did not ask
// for the cancellation, so the worker is gone all the same.
func TestImplementFeatureWorkflow_WorkerStoppedCancelsTheRun(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{Report: "Done.", Subtype: "success"}, nil, oneCommit(), nil)
	e.duringRun = func(context.Context) error { return temporal.NewCanceledError() }

	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	if out.Pushed || e.inspects != 0 {
		t.Errorf("pushed %v after %d inspections, want nothing", out.Pushed, e.inspects)
	}
	if !strings.Contains(out.Error, "the worker that held the run stopped before the run finished") {
		t.Errorf("Error = %q", out.Error)
	}
	// Nothing more is asked of a worker that is gone: the cleanup would
	// wait for no one.
	if len(e.cleaned) != 0 {
		t.Errorf("cleaned %v on a worker that is gone", e.cleaned)
	}
}

// The worker goes while the commits are checked, or while they are pushed:
// the output says which, and nothing more is asked of it, the cleanup
// included.
func TestImplementFeatureWorkflow_WorkerLostAfterTheRun(t *testing.T) {
	t.Run("inspect", func(t *testing.T) {
		e := newImplementEnv(t, claudeCodeResult{Report: "Done.", Subtype: "success"}, nil, oneCommit(), nil)
		e.inspectErr = temporal.NewCanceledError()
		out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})
		if want := "the worker that held the run stopped before the commits were checked: nothing was pushed"; !strings.Contains(out.Error, want) {
			t.Errorf("Error = %q, want %q", out.Error, want)
		}
		if e.pushed != nil || out.Pushed || len(e.cleaned) != 0 {
			t.Errorf("pushed %+v, cleaned %v after the worker was lost", e.pushed, e.cleaned)
		}
	})
	t.Run("push", func(t *testing.T) {
		e := newImplementEnv(t, claudeCodeResult{Report: "Done.", Subtype: "success"}, nil, oneCommit(), temporal.NewCanceledError())
		out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})
		for _, want := range []string{
			"the worker that held the run stopped during the push",
			"the branch may or may not have been published; check " + out.Branch + " on the remote",
		} {
			if !strings.Contains(out.Error, want) {
				t.Errorf("Error = %q, want %q", out.Error, want)
			}
		}
		if out.Pushed || len(e.cleaned) != 0 {
			t.Errorf("pushed %v, cleaned %v after the worker was lost", out.Pushed, e.cleaned)
		}
	})
}

// expiryWorkflow opens a run of the given bound, and runs one step that
// fails as a lost worker's does, after the step's own time.
func expiryWorkflow(ctx workflow.Context, execution time.Duration) (string, error) {
	r, err := openRun(ctx, execution, tool.CallContext{})
	if err != nil {
		return "", err
	}
	defer r.complete()
	err = workflow.ExecuteActivity(r.step(workflow.ActivityOptions{StartToCloseTimeout: time.Hour}), "Step").Get(r.ctx, nil)
	if !r.failed(err) {
		return "", fmt.Errorf("step error %v not read as a lost worker", err)
	}
	return r.lostAt("during the step", "nothing was done"), nil
}

// A session that reaches its bound looks to the step like a lost worker: the
// output says which it was, by the time the session has lasted.
func TestRun_SessionExpiryIsNotALostWorker(t *testing.T) {
	for _, c := range []struct {
		name  string
		after time.Duration // how long the step lasts before it fails
		want  string
	}{
		{"lost worker", 10 * time.Second, "the worker that held the run stopped during the step: nothing was done"},
		{"expired", 20 * time.Minute, "the run reached its time limit (20m0s) during the step: nothing was done"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			asRunWorker(env)
			env.RegisterWorkflow(expiryWorkflow)
			env.RegisterActivityWithOptions(func(context.Context) error { return nil }, sdkactivity.RegisterOptions{Name: "Step"})
			env.OnActivity("Step", mock.Anything).After(c.after).Return(temporal.NewCanceledError())

			env.ExecuteWorkflow(expiryWorkflow, 20*time.Minute)
			var got string
			if err := env.GetWorkflowResult(&got); err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

// A worker that stops ends its run itself and says so
// (activity.ErrWorkerStopping): the worker is gone.
func TestImplementFeatureWorkflow_WorkerStoppingEndsTheRun(t *testing.T) {
	e := newImplementEnv(t, claudeCodeResult{}, nil, oneCommit(), nil)
	e.duringRun = func(context.Context) error {
		return temporal.NewNonRetryableApplicationError("the worker stopped during the run", activity.ErrWorkerStopping, nil,
			runProgress{Events: 12, ToolCalls: 4, LastTool: "Read"})
	}

	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	if out.Pushed || e.inspects != 0 {
		t.Errorf("pushed %v after %d inspections, want nothing", out.Pushed, e.inspects)
	}
	if !strings.Contains(out.Error, "the worker that held the run stopped before the run finished") {
		t.Errorf("Error = %q", out.Error)
	}
	// What the run did before its worker stopped, as the worker said.
	if want := "4 tool calls (last: Read), 12 events; cost unknown (run interrupted)"; !strings.Contains(out.Content, want) {
		t.Errorf("content lacks %q:\n%s", want, out.Content)
	}
}

// Cancelling the workflow mid-run still deletes the clone, on the session's
// worker: the cleanup's disconnected context keeps the session.
func TestCodingRuns_CleanupAfterCancel(t *testing.T) {
	cancelDuringRun := func(env *testsuite.TestWorkflowEnvironment) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			env.CancelWorkflow()
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
			}
			return ctx.Err()
		}
	}
	check := func(t *testing.T, env *testsuite.TestWorkflowEnvironment, queues *activityQueues, cleaned []string) {
		t.Helper()
		if !env.IsWorkflowCompleted() {
			t.Fatal("workflow did not complete")
		}
		if len(cleaned) != 1 {
			t.Fatalf("cleaned %v, want the workspace deleted", cleaned)
		}
		var runQueue, cleanupQueue string
		for _, r := range queues.all() {
			switch r.name {
			case "RunClaudeCode":
				runQueue = r.queue
			case "CleanupWorkspace":
				cleanupQueue = r.queue
			}
		}
		if cleanupQueue == "" || cleanupQueue != runQueue || !strings.Contains(cleanupQueue, "@") {
			t.Errorf("cleanup on %q, run on %q: want both on the session's worker", cleanupQueue, runQueue)
		}
	}

	t.Run("analyze", func(t *testing.T) {
		a := newAnalyzeEnv(t, nil, claudeCodeResult{}, nil)
		a.duringRun = cancelDuringRun(a.env)
		raw, _ := json.Marshal(AnalyzeRepoInput{Repo: "/src/repo", Task: "look"})
		a.env.ExecuteWorkflow(AnalyzeRepoWorkflow, json.RawMessage(raw))
		check(t, a.env, a.queues, a.cleaned)
	})
	t.Run("implement", func(t *testing.T) {
		e := newImplementEnv(t, claudeCodeResult{}, nil, oneCommit(), nil)
		e.duringRun = cancelDuringRun(e.env)
		raw, _ := json.Marshal(ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})
		e.env.ExecuteWorkflow(ImplementFeatureWorkflow, json.RawMessage(raw))
		check(t, e.env, e.queues, e.cleaned)
		if e.pushed != nil {
			t.Error("pushed a cancelled run")
		}
	})
}

// The session outlives every step of its run, retries included: past it, the
// SDK fails the session under a run still going. The values are written out:
// changing a step's timeout or attempts must come with a look at these.
func TestRunSessionTimeouts(t *testing.T) {
	if want := 91 * time.Minute; analyzeSessionTimeout != want {
		t.Errorf("analyze session %s, want %s", analyzeSessionTimeout, want)
	}
	if want := 190 * time.Minute; implementSessionTimeout != want {
		t.Errorf("implement session %s, want %s", implementSessionTimeout, want)
	}
	if want := 205 * time.Minute; RunWorkspaceLifetime != want {
		t.Errorf("RunWorkspaceLifetime %s, want %s", RunWorkspaceLifetime, want)
	}
}
