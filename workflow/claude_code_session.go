package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// A coding run's steps share a clone on one worker's disk: they run in a
// Temporal session, which pins every activity of it to the worker that took
// the session (its own task queue, "<resource>@<host>").
const (
	// runProbeTimeout bounds the wait for any worker of the run's queue to
	// answer the probe (activity.ClaudeCodeActivities.ProbeRunWorker): a
	// live one does at once, busy or not. Past it, there is none.
	runProbeTimeout = time.Minute
	// runWaitNotice is how long a run waits for a worker with a slot to
	// spare before its user is told that it waits.
	runWaitNotice = time.Minute
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
		publishOutputsAttempts*publishOutputsTimeout + inspectAttempts*inspectTimeout + pushAttempts*pushTimeout +
		bundleAttempts*bundleTimeout + cleanupAttempts*cleanupTimeout + runSessionMargin
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

// sessionExpirySlack is how much earlier than the workflow learns it a
// session's clock starts: its worker takes it, then tells the workflow.
const sessionExpirySlack = time.Minute

// run is a coding run's hold on the worker that took it: the session its
// steps run in.
type run struct {
	// ctx is the session's: a step run on it goes to the session's worker
	// (step).
	ctx workflow.Context
	// started is when the session opened, and execution its bound: past it,
	// the SDK fails the session, though its worker is fine.
	started   time.Time
	execution time.Duration
	// lost: a step found the worker gone, or the session over (failed). The
	// worker is asked nothing more, the clone's cleanup included.
	lost bool
}

// openRun reserves a worker for a run that lasts at most execution. The
// error says, for the output, why no worker was reserved. The caller
// completes the run (complete) once it is over.
//
// A worker that runs as many runs as it may stops polling for new sessions:
// to the session alone, a busy queue and one no worker serves look the same.
// So a probe first asks the queue itself, which every worker polls: no
// answer within runProbeTimeout, no worker. Then the run waits for a slot,
// as long as the worker that answered says (its CLAUDE_CODE_QUEUE_WAIT); past
// runWaitNotice, its user is told (call: where the user is).
//
// probed is the probe's answer when the caller made it already
// (CodingRunWorkflow, before it starts the run on its fallback queue): the
// queue is not asked twice.
func openRun(ctx workflow.Context, execution time.Duration, call tool.CallContext, probed *activity.ProbeRunWorkerOutput) (*run, error) {
	queue := runQueue(ctx)
	logger := workflow.GetLogger(ctx)

	var ccAct *activity.ClaudeCodeActivities
	var probe activity.ProbeRunWorkerOutput
	var err error
	if probed != nil {
		probe = *probed
	} else {
		err = workflow.ExecuteActivity(
			workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				TaskQueue:              queue,
				ScheduleToStartTimeout: runProbeTimeout,
				StartToCloseTimeout:    10 * time.Second,
				RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 1},
			}),
			ccAct.ProbeRunWorker,
		).Get(ctx, &probe)
	}
	switch {
	case isScheduleToStartTimeout(err):
		logger.Warn("No worker answered on the coding runs' queue: none is running", "queue", queue, "waited", runProbeTimeout)
		return nil, fmt.Errorf("no worker available for %q; nothing was done", queue)
	case hasErrorType(err, activity.ErrNoClaudeCLI):
		logger.Error("A worker of the coding runs' queue has no claude CLI: every worker of it must have it", "queue", queue)
		return nil, fmt.Errorf("a worker of %q cannot run Claude Code (no CLI installed); nothing was done", queue)
	case err != nil:
		return nil, fmt.Errorf("could not reach a worker of %q: %w", queue, err)
	}

	// Told only past runWaitNotice: most runs find a slot at once. The
	// notice runs beside the wait, which needs this coroutine's context.
	// Only the timer is cancelled when the wait ends: a notice already on
	// its way goes out, and the one that clears it (below) after it.
	timer, stopWaiting := workflow.WithCancel(ctx)
	notified := false
	noticeSent, sentNotice := workflow.NewFuture(ctx)
	workflow.Go(ctx, func(ctx workflow.Context) {
		// Cancelled with timer, waited on in this coroutine. A timer that
		// fired as the wait ended (both in one workflow task) is over too:
		// timer.Err tells, read once the wait is over.
		if workflow.NewTimer(timer, runWaitNotice).Get(ctx, nil) != nil || timer.Err() != nil {
			return
		}
		notified = true
		sendRunNotice(ctx, call, waitingNotice(probe.QueueWait()))
		sentNotice.Set(nil, nil)
	})
	runCtx, err := workflow.CreateSession(workflow.WithTaskQueue(ctx, queue), &workflow.SessionOptions{
		CreationTimeout:  probe.QueueWait(),
		ExecutionTimeout: execution,
		HeartbeatTimeout: runHeartbeatTimeout,
	})
	stopWaiting()
	// The wait is over, whatever its end: the user was told it waits, so is
	// told it no longer does (an empty notice: the web's working line drops
	// it, Telegram sends nothing). Coroutines are cooperative: notified is
	// final here, set before the notice was scheduled or never. The clear
	// follows the notice it replaces. A run that goes on does meanwhile; one
	// that ends here waits for it, or the workflow would end first and the
	// clear never go out. That wait delays the run's "all busy" answer by as
	// long as the notices take: next to nothing when they go out; when the
	// turn queue's notifier fails, the retry budget of the notice still on
	// its way and of the clear (notifyRetry: 3 tries of channelNotifyTimeout
	// each, about 3 min apiece). Accepted: the turn waiting for this answer
	// lives on that same queue, so a notifier failing there fails its own
	// reply as well, and a wait left on show would mislead the user.
	if notified && ctx.Err() == nil {
		clear := func(ctx workflow.Context) {
			_ = noticeSent.Get(ctx, nil)
			sendRunNotice(ctx, call, "")
		}
		if err == nil {
			workflow.Go(ctx, clear)
		} else {
			clear(ctx)
		}
	}
	switch {
	case err == nil:
		return &run{ctx: runCtx, started: workflow.Now(ctx), execution: execution}, nil
	case isScheduleToStartTimeout(err):
		logger.Warn("No worker of the coding runs' queue had a slot to spare: each runs its maximum "+
			"(CLAUDE_CODE_MAX_CONCURRENT_RUNS); more workers, or a longer CLAUDE_CODE_QUEUE_WAIT, would take it",
			"queue", queue, "waited", probe.QueueWait())
		return nil, fmt.Errorf("the workers of %q are all busy (maximum runs reached); waited %s; try again later; nothing was done",
			queue, probe.QueueWait())
	default:
		return nil, fmt.Errorf("could not reserve a worker of %q for the run: %w", queue, err)
	}
}

// waitingNotice tells the run's user that the run waits for a free worker,
// for at most wait.
func waitingNotice(wait time.Duration) string {
	return fmt.Sprintf("Ton run attend un worker libre (tous occupés) : il démarre dès qu'un worker se libère, "+
		"ou abandonne au bout de %s.", inMinutes(wait))
}

// sendRunNotice tells the run's user, on the turn's channel, what the run
// waits for (activity.EventNotice); an empty text says it waits no more.
// Best effort: a failure is logged, and the run goes on.
func sendRunNotice(ctx workflow.Context, call tool.CallContext, text string) {
	logger := workflow.GetLogger(ctx)
	wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
	sessionID, ok := SessionOf(wfID)
	if !ok {
		logger.Warn("A coding run has a notice for its user, and no session to say it to")
		return
	}
	// The participant whose turn waits, or its background task: the server
	// shows the notice on its line.
	participant, _ := ParticipantOf(wfID)
	task, _ := store.TaskOfWorkflow(wfID)
	data, _ := json.Marshal(map[string]string{
		"type":        activity.EventNotice,
		"text":        text,
		"agent":       call.Agent,
		"participant": participant,
		"task":        task,
	})
	var notifAct *activity.NotificationActivities
	err := workflow.ExecuteActivity(
		workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			// Where the channels' notifiers are: a coding worker may have none.
			TaskQueue:           call.NotifyQueue,
			StartToCloseTimeout: channelNotifyTimeout,
			RetryPolicy:         notifyRetry,
		}),
		notifAct.NotifyStep,
		activity.NotifyInput{
			SessionID: sessionID,
			Channel:   call.Channel,
			ChannelID: call.ChannelID,
			Event:     activity.SSEEvent{Type: activity.EventNotice, Data: data},
		},
	).Get(ctx, nil)
	if err != nil && ctx.Err() == nil {
		logger.Warn("A coding run's notice was not delivered", "session_id", sessionID, "cleared", text == "", "error", err)
	}
}

// inMinutes writes a wait for the user: "30 min", or as Go does under a
// minute.
func inMinutes(d time.Duration) string {
	if d < time.Minute {
		return d.String()
	}
	return fmt.Sprintf("%d min", int(d.Round(time.Minute)/time.Minute))
}

// complete releases the worker: the session's end.
func (r *run) complete() { workflow.CompleteSession(r.ctx) }

// step is the run's context with the options of one step: on the session's
// worker, which takes it at once if it is alive.
func (r *run) step(opts workflow.ActivityOptions) workflow.Context {
	return onRunWorker(r.ctx, opts)
}

// onRunWorker is runCtx with the options of one step of the run.
func onRunWorker(runCtx workflow.Context, opts workflow.ActivityOptions) workflow.Context {
	opts.ScheduleToStartTimeout = runStartTimeout
	return workflow.WithActivityOptions(runCtx, opts)
}

// failed tells whether a step's error means the run lost its worker
// (workerLost), and remembers it: nothing more is asked of that worker.
func (r *run) failed(err error) bool {
	if !workerLost(r.ctx, err) {
		return false
	}
	r.lost = true
	return true
}

// expired tells whether the session has reached its bound: what a lost
// worker looks like to the steps, though the worker is fine.
func (r *run) expired() bool {
	return workflow.Now(r.ctx).Sub(r.started) >= r.execution-sessionExpirySlack
}

// lostAt is what the output says of a run that lost its worker (failed) at
// the step named by when. Its clone stays on that worker's disk until it
// starts again (RootClaim.Sweep): no other worker can reach it.
func (r *run) lostAt(when, consequence string) string {
	if r.expired() {
		return fmt.Sprintf("the run reached its time limit (%s) %s: %s", r.execution, when, consequence)
	}
	return fmt.Sprintf("the worker that held the run stopped %s: %s", when, consequence)
}

// workerLost tells whether a step failed because the worker that holds the
// run is gone: the session failed (it died, or lost touch with Temporal, or
// the session reached its bound), no one picked the step up, or the worker
// ended the step itself — it stopped (activity.ErrWorkerStopping, or a
// cancellation the workflow had not asked for).
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
	if hasErrorType(err, activity.ErrWorkerStopping) {
		return true
	}
	var canceled *temporal.CanceledError
	return errors.As(err, &canceled) && runCtx.Err() == nil
}

// hasErrorType tells whether err is an activity's application error of type
// typ.
func hasErrorType(err error, typ string) bool {
	var appErr *temporal.ApplicationError
	return errors.As(err, &appErr) && appErr.Type() == typ
}

// cleanup deletes the run's clone, on its worker. It is the one step that
// must happen on every path out, a cancelled workflow included, which cannot
// start an activity on its own context: a disconnected one, derived from the
// session's, still carries the session and so reaches the same worker. A run
// that lost its worker skips it: it would wait for no one (runStartTimeout),
// and a failed session refuses it anyway. The clone then waits for its
// worker's next start.
func (r *run) cleanup(dir string) {
	if r.lost {
		return
	}
	ctx, cancel := workflow.NewDisconnectedContext(r.ctx)
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
