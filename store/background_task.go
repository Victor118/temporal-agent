package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// A background task is a tool call a session turn launched without waiting
// for it (docs/design/async-tasks.md): the turn ends, its participant is
// free, and the task's end posts a message in the session (KindTaskResult)
// that wakes it. Its row is the source of the Agents panel, of the prompt's
// list of tasks running, and of the session's deletion; Temporal runs it.

// Where a background task stands. A task ends once: the first to end it
// (its workflow, or the sweep) writes its message and its state (EndTask).
const (
	BackgroundRunning   = "running"
	BackgroundDone      = "done"
	BackgroundFailed    = "failed"
	BackgroundCancelled = "cancelled"
)

// Who ends a task (BackgroundTask.EndedBy): its own workflow, or the sweep
// for a workflow that closed without saying how it ended.
const (
	TaskEndedByTask  = "task"
	TaskEndedBySweep = "sweep"
)

// KindTaskResult marks a background task's end, posted into its session: its
// result, or why it has none. A message of nobody (no Author), addressed to
// the agent that launched it (AgentID), for the user who asked (UserID).
// Its Task says which task.
const KindTaskResult = "task_result"

// Errors of the background tasks.
var (
	// ErrTooManyTasks: the participant has as many tasks running as it may.
	ErrTooManyTasks = errors.New("too many background tasks running")
	// ErrTaskSessionGone: the session of a task to register is gone.
	ErrTaskSessionGone = errors.New("the session no longer exists")
	// ErrTaskNotFound: no such task, or not one of this participant's.
	ErrTaskNotFound = errors.New("no such background task")
	// ErrTaskOver: the task ended already.
	ErrTaskOver = errors.New("the background task has ended")
	// ErrTooManyFollowUps: the task holds as many instructions as it may.
	ErrTooManyFollowUps = errors.New("too many instructions on this task")
)

// bgMark separates a turn's workflow ID from the call that launched one of
// its background tasks: "<turn>:bg:<call>".
const bgMark = ":bg:"

// BackgroundTaskID is the workflow ID of the background task a turn
// (turnWorkflowID, "<session>:p:<agent>:m<id>") launched by its call callID:
// it starts with the session's, so that everything that finds a session's
// workflows by prefix finds it, and names its participant
// (workflow.ParticipantOf).
func BackgroundTaskID(turnWorkflowID, callID string) string {
	return turnWorkflowID + bgMark + callID
}

// TaskOfWorkflow is the background task a workflow belongs to: the task's
// own ID, or that of a workflow it started ("<task>:tool:…"). False for a
// workflow of no task.
func TaskOfWorkflow(workflowID string) (string, bool) {
	i := strings.Index(workflowID, bgMark)
	if i <= 0 {
		return "", false
	}
	rest := workflowID[i+len(bgMark):]
	call, _, _ := strings.Cut(rest, ":")
	if call == "" {
		return "", false
	}
	return workflowID[:i+len(bgMark)+len(call)], true
}

// TaskResultKey is the idempotency key of a task's end in its session: one
// per task, whoever writes it.
func TaskResultKey(taskID string) string { return "task:" + taskID }

// BackgroundTask is a background task's row.
type BackgroundTask struct {
	ID        string `json:"id"` // its workflow ID (BackgroundTaskID)
	SessionID string `json:"session_id"`
	// Participant is the agent whose turn launched it: its end wakes it.
	Participant string `json:"participant"`
	// UserID and UserName are who asked: the user the launching turn
	// answered, whom the turn its end wakes answers.
	UserID   string `json:"user_id"`
	UserName string `json:"user_name,omitempty"`
	Tool     string `json:"tool"`
	// Summary is its input in a line, for the members and the prompt.
	Summary string `json:"summary,omitempty"`
	// TurnKey and CallID are the turn and the tool call that launched it:
	// its files are published under them.
	TurnKey string `json:"turn_key"`
	CallID  string `json:"call_id"`
	// Channel and ChannelID are the launching turn's: the woken turn
	// answers there.
	Channel   string     `json:"channel,omitempty"`
	ChannelID string     `json:"channel_id,omitempty"`
	State     string     `json:"state"`
	StartedAt time.Time  `json:"started_at"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	// EndedBy is who ended it (TaskEndedBy*), ResultMessageID its message.
	EndedBy         string `json:"ended_by,omitempty"`
	ResultMessageID int64  `json:"result_message_id,omitempty"`
	// CancelledBy names the member who stopped it, once one asked to;
	// CancelSentAt is when its workflow was told (CancelWorkflow), nil
	// until then: the sweep sends it again.
	CancelledBy  string     `json:"cancelled_by,omitempty"`
	CancelSentAt *time.Time `json:"cancel_sent_at,omitempty"`
	// WokenAt is when the wake of its participant by its end's message was
	// done with: delivered, or given up with an end of the turn saying so.
	// Nil for an ended task not cancelled: the sweep wakes it again.
	WokenAt *time.Time `json:"woken_at,omitempty"`
	// FollowUps are the instructions attached while it ran (when_task_done),
	// in order: its end's message starts with them.
	FollowUps []TaskFollowUp `json:"follow_ups,omitempty"`
}

// TaskFollowUp is an instruction to apply at a task's end, signed by the
// user whose turn attached it.
type TaskFollowUp struct {
	Text     string    `json:"text"`
	UserID   string    `json:"user_id,omitempty"`
	UserName string    `json:"user_name,omitempty"`
	At       time.Time `json:"at"`
}

// TaskRef is the task a KindTaskResult message ends: what the members and
// the agents read of it, as it ended.
type TaskRef struct {
	ID          string         `json:"id"`
	Tool        string         `json:"tool"`
	Summary     string         `json:"summary,omitempty"`
	TurnKey     string         `json:"turn_key"`
	CallID      string         `json:"call_id"`
	State       string         `json:"state"`
	RequestedBy string         `json:"requested_by,omitempty"`
	StartedAt   time.Time      `json:"started_at"`
	EndedAt     time.Time      `json:"ended_at"`
	CancelledBy string         `json:"cancelled_by,omitempty"`
	FollowUps   []TaskFollowUp `json:"follow_ups,omitempty"`
	// File is the whole result, published apart when the message holds its
	// start only; nil when the message holds it all.
	File *TaskFile `json:"file,omitempty"`
}

// TaskFile is a file a task's result was published as.
type TaskFile struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// Ref is t as its end's message names it.
func (t BackgroundTask) Ref() TaskRef {
	r := TaskRef{
		ID: t.ID, Tool: t.Tool, Summary: t.Summary, TurnKey: t.TurnKey, CallID: t.CallID, State: t.State,
		RequestedBy: t.UserName, StartedAt: t.StartedAt, CancelledBy: t.CancelledBy, FollowUps: t.FollowUps,
	}
	if t.EndedAt != nil {
		r.EndedAt = *t.EndedAt
	}
	return r
}

// TaskEnding is what EndTask did.
type TaskEnding struct {
	// Task is the task as it ended, now or before.
	Task BackgroundTask
	// MessageID is its end's message.
	MessageID int64
	// Mine: the caller ended it, now or in an earlier attempt (the same
	// EndedBy). Only it wakes the participant: a second writer does
	// nothing.
	Mine bool
	// Gone: there is no such task, its session deleted with it.
	Gone bool
}

const taskSchema = `
		-- A tool call a session turn launched in the background
		-- (docs/design/async-tasks.md): it runs as a workflow, whose ID is
		-- the row's. The source of the Agents panel, the prompt and the
		-- session's deletion; its end posts a task_result message and
		-- wakes its participant. Deleted with its session.
		CREATE TABLE IF NOT EXISTS background_tasks (
			id                TEXT PRIMARY KEY,
			session_id        TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
			participant       TEXT NOT NULL,
			user_id           TEXT NOT NULL DEFAULT '',
			user_name         TEXT NOT NULL DEFAULT '',
			tool              TEXT NOT NULL,
			summary           TEXT NOT NULL DEFAULT '',
			turn_key          TEXT NOT NULL,
			call_id           TEXT NOT NULL,
			channel           TEXT NOT NULL DEFAULT '',
			channel_id        TEXT NOT NULL DEFAULT '',
			state             TEXT NOT NULL DEFAULT 'running'
				CHECK (state IN ('running', 'done', 'failed', 'cancelled')),
			started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			ended_at          TIMESTAMPTZ,
			ended_by          TEXT NOT NULL DEFAULT '',
			result_message_id BIGINT NOT NULL DEFAULT 0,
			cancelled_by      TEXT NOT NULL DEFAULT '',
			cancel_sent_at    TIMESTAMPTZ,
			woken_at          TIMESTAMPTZ,
			follow_ups        JSONB NOT NULL DEFAULT '[]'
		);
		CREATE INDEX IF NOT EXISTS idx_background_tasks_running
			ON background_tasks(session_id, participant) WHERE state = 'running';
		-- The ended tasks whose participant is not woken yet: the sweep's.
		CREATE INDEX IF NOT EXISTS idx_background_tasks_unwoken
			ON background_tasks(ended_at) WHERE state IN ('done', 'failed') AND woken_at IS NULL;
`

const taskColumns = `id, session_id, participant, user_id, user_name, tool, summary, turn_key, call_id, channel, channel_id,
	state, started_at, ended_at, ended_by, result_message_id, cancelled_by, cancel_sent_at, woken_at, follow_ups`

func scanTask(row interface{ Scan(...any) error }) (BackgroundTask, error) {
	var t BackgroundTask
	var ended, cancelSent, woken sql.NullTime
	var followUps []byte
	err := row.Scan(&t.ID, &t.SessionID, &t.Participant, &t.UserID, &t.UserName, &t.Tool, &t.Summary, &t.TurnKey, &t.CallID,
		&t.Channel, &t.ChannelID, &t.State, &t.StartedAt, &ended, &t.EndedBy, &t.ResultMessageID, &t.CancelledBy, &cancelSent, &woken, &followUps)
	if err != nil {
		return t, err
	}
	t.EndedAt, t.CancelSentAt, t.WokenAt = nullTime(ended), nullTime(cancelSent), nullTime(woken)
	if err := json.Unmarshal(followUps, &t.FollowUps); err != nil {
		return t, fmt.Errorf("task %s: decode its instructions: %w", t.ID, err)
	}
	return t, nil
}

// RegisterTask records t running, unless its participant has max tasks
// running already (ErrTooManyTasks). A task registered already, a retry, is
// left as it is and not counted again. A session gone is
// ErrTaskSessionGone. The count and the insert hold a lock on the
// participant: two calls of one step launched together cannot both pass.
func (s *PostgresStore) RegisterTask(ctx context.Context, t BackgroundTask, max int) error {
	followUps, err := json.Marshal(nonNilFollowUps(t.FollowUps))
	if err != nil {
		return err
	}
	err = s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "bg:"+t.SessionID+":"+t.Participant); err != nil {
			return fmt.Errorf("task lock: %w", err)
		}
		var exists bool
		if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM background_tasks WHERE id = $1)", t.ID).Scan(&exists); err != nil || exists {
			return err
		}
		var running int
		if err := tx.QueryRowContext(ctx,
			"SELECT count(*) FROM background_tasks WHERE session_id = $1 AND participant = $2 AND state = 'running'",
			t.SessionID, t.Participant).Scan(&running); err != nil {
			return err
		}
		if running >= max {
			return ErrTooManyTasks
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO background_tasks (id, session_id, participant, user_id, user_name, tool, summary, turn_key, call_id,
				channel, channel_id, follow_ups)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
			t.ID, t.SessionID, t.Participant, t.UserID, t.UserName, t.Tool, t.Summary, t.TurnKey, t.CallID,
			t.Channel, t.ChannelID, string(followUps))
		return err
	})
	if isForeignKeyViolation(err) {
		return ErrTaskSessionGone
	}
	return err
}

func nullTime(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	return &t.Time
}

func nonNilFollowUps(f []TaskFollowUp) []TaskFollowUp {
	if f == nil {
		return []TaskFollowUp{}
	}
	return f
}

// DropTask forgets a task still running that never started: its workflow
// could not be. An ended one is kept.
func (s *PostgresStore) DropTask(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM background_tasks WHERE id = $1 AND state = 'running'", id)
	return err
}

// GetTask returns a task, nil when there is none.
func (s *PostgresStore) GetTask(ctx context.Context, id string) (*BackgroundTask, error) {
	t, err := scanTask(s.db.QueryRowContext(ctx, "SELECT "+taskColumns+" FROM background_tasks WHERE id = $1", id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

// ListRunningTasks returns the tasks running in a session, of one
// participant, or of all when participant is "", oldest first.
func (s *PostgresStore) ListRunningTasks(ctx context.Context, sessionID, participant string) ([]BackgroundTask, error) {
	return s.listTasks(ctx, `SELECT `+taskColumns+` FROM background_tasks
		WHERE session_id = $1 AND ($2 = '' OR participant = $2) AND state = 'running' ORDER BY started_at, id`, sessionID, participant)
}

// ListTasksToWake returns the tasks of every session ended (done or
// failed) before endedBefore whose participant was not woken: the sweep
// wakes them again (a wake lost between the end and the signal).
func (s *PostgresStore) ListTasksToWake(ctx context.Context, endedBefore time.Time) ([]BackgroundTask, error) {
	return s.listTasks(ctx, `SELECT `+taskColumns+` FROM background_tasks
		WHERE state IN ('done', 'failed') AND woken_at IS NULL AND ended_at < $1 ORDER BY ended_at, id`, endedBefore)
}

// ListTasksToCancel returns the tasks running that a member stopped and
// whose workflow was not told: the sweep tells it again.
func (s *PostgresStore) ListTasksToCancel(ctx context.Context) ([]BackgroundTask, error) {
	return s.listTasks(ctx, `SELECT `+taskColumns+` FROM background_tasks
		WHERE state = 'running' AND cancelled_by <> '' AND cancel_sent_at IS NULL ORDER BY started_at, id`)
}

// SetTaskWoken records that the wake of a task's participant is done with.
func (s *PostgresStore) SetTaskWoken(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE background_tasks SET woken_at = NOW() WHERE id = $1 AND woken_at IS NULL", id)
	return err
}

// SetTaskCancelSent records that a stopped task's workflow was told.
func (s *PostgresStore) SetTaskCancelSent(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE background_tasks SET cancel_sent_at = NOW() WHERE id = $1 AND cancel_sent_at IS NULL", id)
	return err
}

// ListTasksRunningSince returns the tasks of every session running since
// before: those the sweep checks.
func (s *PostgresStore) ListTasksRunningSince(ctx context.Context, before time.Time) ([]BackgroundTask, error) {
	return s.listTasks(ctx, `SELECT `+taskColumns+` FROM background_tasks
		WHERE state = 'running' AND started_at < $1 ORDER BY started_at, id`, before)
}

func (s *PostgresStore) listTasks(ctx context.Context, query string, args ...any) ([]BackgroundTask, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []BackgroundTask
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// AddTaskFollowUp attaches an instruction to a task of participant's in a
// session, still running, holding fewer than max. ErrTaskNotFound: none
// such; ErrTaskOver: it ended; ErrTooManyFollowUps: it holds max. Its end
// (EndTask) reads them under the same row lock: an instruction is in its
// message, or refused as over.
func (s *PostgresStore) AddTaskFollowUp(ctx context.Context, sessionID, participant, id string, f TaskFollowUp, max int) error {
	entry, err := json.Marshal([]TaskFollowUp{f})
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE background_tasks SET follow_ups = follow_ups || $5::jsonb
		WHERE id = $1 AND session_id = $2 AND participant = $3 AND state = 'running' AND jsonb_array_length(follow_ups) < $4`,
		id, sessionID, participant, max, string(entry))
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n == 1 {
		return err
	}
	t, err := s.GetTask(ctx, id)
	switch {
	case err != nil:
		return err
	case t == nil || t.SessionID != sessionID || t.Participant != participant:
		return ErrTaskNotFound
	case t.State != BackgroundRunning:
		return ErrTaskOver
	}
	return ErrTooManyFollowUps
}

// SetTaskCancelledBy names the member who stops a running task, before its
// workflow is cancelled: its end's message says who, if it ends cancelled.
// ErrTaskOver when it ended, ErrTaskNotFound when there is none.
func (s *PostgresStore) SetTaskCancelledBy(ctx context.Context, id, name string) error {
	res, err := s.db.ExecContext(ctx, "UPDATE background_tasks SET cancelled_by = $2 WHERE id = $1 AND state = 'running'", id, name)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n == 1 {
		return err
	}
	t, err := s.GetTask(ctx, id)
	if err != nil {
		return err
	}
	if t == nil {
		return ErrTaskNotFound
	}
	return ErrTaskOver
}

// EndTask ends a running task, as by (TaskEndedBy*), in state: its message,
// built from the task as it ends (build), goes into its session under
// TaskResultKey, and the task records its state and its message, in one
// transaction that holds the task's row. The first to end a task wins: the
// message is inserted ON CONFLICT DO NOTHING, the state changes only from
// running. A task ended already is left as it is: Mine says whether by
// itself, an earlier attempt of the same writer, which then wakes the
// participant again (a retried activity). A task that is no more (its
// session deleted) is Gone: nothing is written.
func (s *PostgresStore) EndTask(ctx context.Context, id, by, state string, build func(BackgroundTask) Message) (TaskEnding, error) {
	var out TaskEnding
	err := s.inTx(ctx, func(tx *sql.Tx) error {
		t, err := scanTask(tx.QueryRowContext(ctx, "SELECT "+taskColumns+" FROM background_tasks WHERE id = $1 FOR UPDATE", id))
		if errors.Is(err, sql.ErrNoRows) {
			out.Gone = true
			return nil
		}
		if err != nil {
			return err
		}
		if t.State != BackgroundRunning {
			out = TaskEnding{Task: t, MessageID: t.ResultMessageID, Mine: t.EndedBy == by}
			return nil
		}
		now := time.Now()
		t.State, t.EndedAt, t.EndedBy = state, &now, by
		// Stopped too late, it ended all the same: nobody cancelled it.
		if state != BackgroundCancelled {
			t.CancelledBy = ""
		}
		data, err := json.Marshal(build(t))
		if err != nil {
			return err
		}
		key := TaskResultKey(id)
		var msgID int64
		err = tx.QueryRowContext(ctx, `
			INSERT INTO messages (session_id, msg_key, data) VALUES ($1, $2, $3)
			ON CONFLICT (session_id, msg_key) DO NOTHING RETURNING id`, t.SessionID, key, string(data)).Scan(&msgID)
		if errors.Is(err, sql.ErrNoRows) { // written before the row said so: never by this code
			err = tx.QueryRowContext(ctx, "SELECT id FROM messages WHERE session_id = $1 AND msg_key = $2", t.SessionID, key).Scan(&msgID)
		}
		if err != nil {
			return err
		}
		t.ResultMessageID = msgID
		if _, err := tx.ExecContext(ctx, `
			UPDATE background_tasks SET state = $2, ended_at = $3, ended_by = $4, result_message_id = $5, cancelled_by = $6
			WHERE id = $1 AND state = 'running'`, id, state, now, by, msgID, t.CancelledBy); err != nil {
			return err
		}
		out = TaskEnding{Task: t, MessageID: msgID, Mine: true}
		return nil
	})
	return out, err
}
