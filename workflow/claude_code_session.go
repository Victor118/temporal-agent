package workflow

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
)

// A coding run's steps share a clone on one worker's disk: they run in a
// Temporal session, which pins every activity of it to the worker that took
// the session (its own task queue, "<resource>@<host>").
const (
	// runCreationTimeout is how long a run waits for a worker of its queue
	// to take it: one that runs, with a run to spare
	// (CLAUDE_CODE_MAX_CONCURRENT_RUNS). A full worker does not poll for new
	// sessions, so waiting for a slot and finding no worker look the same.
	runCreationTimeout = 5 * time.Minute
	// runHeartbeatTimeout is how long the worker may go silent before the
	// session fails. The SDK beats every 10s at most: six missed beats, not
	// one blip, end a run that may have cost an hour.
	runHeartbeatTimeout = time.Minute
	// runStartTimeout bounds the wait of a step for the session's worker to
	// pick it up, which a live one does at once. A worker that stopped
	// gracefully reports its session cancelled, not failed: the SDK keeps
	// it open, and only this tells the next step that no one is left.
	runStartTimeout = 2 * time.Minute
	// runSessionMargin covers what the sums below leave out: the retries'
	// backoff, the scheduling of each step.
	runSessionMargin = 10 * time.Minute

	prepareAttempts = 2
	cleanupAttempts = 3

	// The session outlives every step of its run, retries included: past
	// it, the SDK fails the session and cancels whatever still runs.
	analyzeSessionTimeout = prepareAttempts*prepareTimeout + analyzeTimeout +
		cleanupAttempts*cleanupTimeout + runSessionMargin
	implementSessionTimeout = prepareAttempts*prepareTimeout + implementTimeout +
		inspectAttempts*inspectTimeout + pushAttempts*pushTimeout +
		cleanupAttempts*cleanupTimeout + runSessionMargin
)

// RunWorkspaceLifetime bounds how long a run's workspace is in use: the
// longest session, which began before the clone did, and the time its worker
// takes to notice that the session is over. A workspace older than this
// belongs to no live run (activity.RootClaim.Sweep).
const RunWorkspaceLifetime = max(analyzeSessionTimeout, implementSessionTimeout) + 15*time.Minute

// runQueue is the task queue a run's worker is taken from: the one the
// workflow runs on, which is the tool's. The one place to choose another, a
// fallback queue among them.
func runQueue(ctx workflow.Context) string {
	return workflow.GetInfo(ctx).TaskQueueName
}

// openRun reserves a worker for a run that lasts at most execution, and
// returns the context its steps run in. The error says, for the output, why
// no worker was reserved. The caller completes the session
// (workflow.CompleteSession) once the run is over.
func openRun(ctx workflow.Context, execution time.Duration) (workflow.Context, error) {
	queue := runQueue(ctx)
	runCtx, err := workflow.CreateSession(workflow.WithTaskQueue(ctx, queue), &workflow.SessionOptions{
		CreationTimeout:  runCreationTimeout,
		ExecutionTimeout: execution,
		HeartbeatTimeout: runHeartbeatTimeout,
	})
	switch {
	case err == nil:
		return runCtx, nil
	case isScheduleToStartTimeout(err):
		return nil, fmt.Errorf("no worker available for %q: none took the run within %s "+
			"(none is running, or each already runs as many as it may, CLAUDE_CODE_MAX_CONCURRENT_RUNS); nothing was done",
			queue, runCreationTimeout)
	default:
		return nil, fmt.Errorf("could not reserve a worker of %q for the run: %w", queue, err)
	}
}

// onRunWorker is runCtx with the options of one step of the run.
func onRunWorker(runCtx workflow.Context, opts workflow.ActivityOptions) workflow.Context {
	opts.ScheduleToStartTimeout = runStartTimeout
	return workflow.WithActivityOptions(runCtx, opts)
}

// workerLost tells whether a step failed because the worker that holds the
// run is gone: the session failed (it died, or lost touch with Temporal), no
// one picked the step up, or the worker ended the step itself — it stopped
// (activity.ErrWorkerStopping, or a cancellation the workflow had not asked
// for).
func workerLost(runCtx workflow.Context, err error) bool {
	if err == nil {
		return false
	}
	if info := workflow.GetSessionInfo(runCtx); info != nil && info.SessionState == workflow.SessionStateFailed {
		return true
	}
	if errors.Is(err, workflow.ErrSessionFailed) || isScheduleToStartTimeout(err) {
		return true
	}
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.Type() == activity.ErrWorkerStopping {
		return true
	}
	var canceled *temporal.CanceledError
	return errors.As(err, &canceled) && runCtx.Err() == nil
}

// workerStopped is what the output says of a run whose worker was lost
// (workerLost) at the step named by when. Its clone stays on that worker's
// disk until it starts again (RootClaim.Sweep): no other worker can reach it.
func workerStopped(when, consequence string) string {
	return fmt.Sprintf("the worker that held the run stopped %s: %s", when, consequence)
}

// cleanupWorkspace deletes the run's clone, on its worker. It is the one step
// that must happen on every path out, a cancelled workflow included, which
// cannot start an activity on its own context: a disconnected one, derived
// from runCtx, still carries the session and so reaches the same worker. A
// failed session does not: the SDK refuses the step, and the clone waits for
// its worker's next start.
func cleanupWorkspace(runCtx workflow.Context, dir string) {
	ctx, cancel := workflow.NewDisconnectedContext(runCtx)
	defer cancel()
	var ccAct *activity.ClaudeCodeActivities
	_ = workflow.ExecuteActivity(
		onRunWorker(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: cleanupTimeout,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: cleanupAttempts},
		}),
		ccAct.CleanupWorkspace,
		activity.CleanupWorkspaceInput{Dir: dir},
	).Get(ctx, nil)
}
