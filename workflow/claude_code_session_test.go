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
)

// testRunQueue is the tool's queue the coding workflows run on in the tests.
const testRunQueue = "tools-claude-code"

// The SDK's names for the activities that open and close a session.
const (
	sessionCreation   = "internalSessionCreationActivity"
	sessionCompletion = "internalSessionCompletionActivity"
)

// activityQueues records the task queue each activity ran on, in order.
type activityQueues struct {
	mu   sync.Mutex
	runs []activityRun
}

type activityRun struct{ name, queue string }

func (q *activityQueues) all() []activityRun {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]activityRun(nil), q.runs...)
}

// asRunWorker makes env what a coding worker is: one of testRunQueue that
// takes sessions (EnableSessionWorker). It records where each activity ran.
func asRunWorker(env *testsuite.TestWorkflowEnvironment) *activityQueues {
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: testRunQueue})
	q := &activityQueues{}
	env.SetOnActivityStartedListener(func(info *sdkactivity.Info, _ context.Context, _ converter.EncodedValues) {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.runs = append(q.runs, activityRun{info.ActivityType.Name, info.TaskQueue})
	})
	return q
}

// checkOneWorker fails unless the session was asked of testRunQueue, every
// step of the run went to the session's own queue, and the session was
// completed last.
func checkOneWorker(t *testing.T, runs []activityRun, steps ...string) {
	t.Helper()
	if len(runs) == 0 || runs[0].name != sessionCreation {
		t.Fatalf("activities %v, want the session opened first", runs)
	}
	if want := testRunQueue + "__internal_session_creation"; runs[0].queue != want {
		t.Errorf("session asked of %q, want %q", runs[0].queue, want)
	}
	var got []string
	for _, r := range runs[1:] {
		if r.name == sessionCompletion {
			continue
		}
		got = append(got, r.name)
		// The session's queue is its worker's alone: "<resource>@<host>".
		if r.queue == testRunQueue || !strings.Contains(r.queue, "@") {
			t.Errorf("%s ran on %q, not on the session's worker", r.name, r.queue)
		}
		if r.queue != runs[1].queue {
			t.Errorf("%s ran on %q, %s on %q: two workers", r.name, r.queue, runs[1].name, runs[1].queue)
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

// noWorker stands for a queue no worker takes a session of: the creation
// activity is never started, and times out.
func noWorker(env *testsuite.TestWorkflowEnvironment) {
	env.OnActivity(sessionCreation, mock.Anything, mock.Anything).
		Return(temporal.NewTimeoutError(enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START, nil))
}

// No worker to take the run: the output says so and names the queue, and
// nothing was started.
func TestCodingRuns_NoWorkerAvailable(t *testing.T) {
	const want = `no worker available for "tools-claude-code"`

	a := newAnalyzeEnv(t, nil, claudeCodeResult{Report: "ok", Subtype: "success"}, nil)
	noWorker(a.env)
	out := a.run_(t, AnalyzeRepoInput{Repo: "/src/repo", Task: "look"})
	if !strings.Contains(out.Error, want) || !strings.Contains(out.Content, want) {
		t.Errorf("analyze: Error = %q", out.Error)
	}
	if a.prepared != nil || len(a.cleaned) != 0 {
		t.Errorf("analyze: prepared %v, cleaned %v, want nothing done", a.prepared, a.cleaned)
	}

	e := newImplementEnv(t, claudeCodeResult{Report: "ok", Subtype: "success"}, nil, oneCommit(), nil)
	noWorker(e.env)
	iout := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})
	if !strings.Contains(iout.Error, want) || iout.Pushed {
		t.Errorf("implement: Error = %q, pushed %v", iout.Error, iout.Pushed)
	}
	if e.run != nil || e.pushed != nil || len(e.cleaned) != 0 {
		t.Error("implement: a step ran with no worker reserved")
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
		if r.name != sessionCreation && r.queue != "resource@host-a" {
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
	r, err := openRun(ctx, execution)
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
		return temporal.NewNonRetryableApplicationError("the worker stopped during the run", activity.ErrWorkerStopping, nil)
	}

	out := e.run_(t, ImplementFeatureInput{Repo: "/src/repo", Task: "do it"})

	if out.Pushed || e.inspects != 0 {
		t.Errorf("pushed %v after %d inspections, want nothing", out.Pushed, e.inspects)
	}
	if !strings.Contains(out.Error, "the worker that held the run stopped before the run finished") {
		t.Errorf("Error = %q", out.Error)
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
