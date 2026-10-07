package activity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/tool"
)

// MaxTaskMessageBytes bounds the result a task's end message holds: its
// start; past it, the whole result is published as a file the message
// names. Lower than a tool result's bound: the message is no turn's, and
// every call of every participant of the session reads it whole, for ever
// (docs/design/async-tasks.md §5.1).
const MaxTaskMessageBytes = 16 << 10

// TaskStore is what the background tasks' activities read and write.
type TaskStore interface {
	GetSession(ctx context.Context, sessionID string) (*store.Session, error)
	RegisterTask(ctx context.Context, t store.BackgroundTask, max int) error
	DropTask(ctx context.Context, id string) error
	GetTask(ctx context.Context, id string) (*store.BackgroundTask, error)
	EndTask(ctx context.Context, id, by, state string, build func(store.BackgroundTask) store.Message) (store.TaskEnding, error)
	SetTaskWoken(ctx context.Context, id string) error
}

// TaskPublisher publishes a task's whole result as a file of its call
// (tool.Publisher): the call context ctx carries says where.
type TaskPublisher interface {
	Publish(ctx context.Context, name string, content []byte) (tool.FileRef, error)
}

// TaskActivities keep the background tasks: their rows, and their end.
type TaskActivities struct {
	Store     TaskStore
	Publisher TaskPublisher
}

type RegisterTaskInput struct {
	Task store.BackgroundTask `json:"task"`
}

// RegisterTaskOutput says why a task was not registered: the model reads
// it as the call's error. Empty: registered.
type RegisterTaskOutput struct {
	Refused string `json:"refused,omitempty"`
}

// RegisterTask records a background task about to start, unless its
// participant runs MaxRunningTasks already: the call is refused then.
func (a *TaskActivities) RegisterTask(ctx context.Context, in RegisterTaskInput) (RegisterTaskOutput, error) {
	err := a.Store.RegisterTask(ctx, in.Task, MaxRunningTasks)
	switch {
	case errors.Is(err, store.ErrTooManyTasks):
		return RegisterTaskOutput{Refused: fmt.Sprintf("You have %d background tasks running already, the most you may: "+
			"wait for one of them to end, or run this call without background.", MaxRunningTasks)}, nil
	case errors.Is(err, store.ErrTaskSessionGone):
		return RegisterTaskOutput{Refused: "The session was deleted: nothing was started."}, nil
	}
	return RegisterTaskOutput{}, err
}

// DropTask forgets a task registered whose workflow could not start.
func (a *TaskActivities) DropTask(ctx context.Context, id string) error {
	return a.Store.DropTask(ctx, id)
}

type PostTaskResultInput struct {
	TaskID string `json:"task_id"`
	// State is how it ended (store.BackgroundDone, …Failed, …Cancelled).
	State string `json:"state"`
	// Content is its result, or why it failed; empty when cancelled.
	Content string `json:"content,omitempty"`
}

// PostTaskResultOutput is the task's end, and what waking its participant
// takes.
type PostTaskResultOutput struct {
	// Gone: the task is no more, its session deleted: nothing was written,
	// nobody is woken.
	Gone bool `json:"gone,omitempty"`
	// MessageID is its end's message, when this task ended itself (now or
	// in an earlier attempt); 0 when the sweep did: it told the members
	// and woke the participant, nothing is left to do.
	MessageID int64 `json:"message_id,omitempty"`
	// Wake: this task ended itself, not cancelled: its participant is to
	// be woken by its message (then TaskWoken). A cancelled one wakes
	// nobody.
	Wake bool `json:"wake,omitempty"`
	// The task's, for the wake and the events.
	SessionID   string `json:"session_id,omitempty"`
	Participant string `json:"participant,omitempty"`
	TurnKey     string `json:"turn_key,omitempty"`
	UserID      string `json:"user_id,omitempty"`
	UserName    string `json:"user_name,omitempty"`
	// Channel and ChannelID are where the launching turn answered, which
	// the woken one answers on; SessionChannel and SessionChannelID the
	// session's, a participant's start input.
	Channel          string `json:"channel,omitempty"`
	ChannelID        string `json:"channel_id,omitempty"`
	SessionChannel   string `json:"session_channel,omitempty"`
	SessionChannelID string `json:"session_channel_id,omitempty"`
	// SignReply: the woken turn signs on the channel, its agent not the
	// session's (the rule of a member's message).
	SignReply bool `json:"sign_reply,omitempty"`
	// File is the whole result, published apart; nil when the message
	// holds it all.
	File *tool.FileRef `json:"file,omitempty"`
}

// PostTaskResult ends a background task: a result past MaxTaskMessageBytes
// is published first, as a file of its call (idempotent: a retry finds
// it), then its message goes into its session and its row says how it
// ended, together (store.EndTask). The first to end a task wins: when the
// sweep did, nothing more is done. A cancelled task's message wakes nobody.
func (a *TaskActivities) PostTaskResult(ctx context.Context, in PostTaskResultInput) (PostTaskResultOutput, error) {
	t, err := a.Store.GetTask(ctx, in.TaskID)
	if err != nil {
		return PostTaskResultOutput{}, fmt.Errorf("read task: %w", err)
	}
	if t == nil {
		return PostTaskResultOutput{Gone: true}, nil
	}
	if t.State != store.BackgroundRunning && t.EndedBy != store.TaskEndedByTask {
		return PostTaskResultOutput{}, nil // the sweep ended it, told and woke
	}
	content, file := in.Content, (*tool.FileRef)(nil)
	if len(content) > MaxTaskMessageBytes && in.State != store.BackgroundCancelled {
		f, err := a.publish(ctx, *t, content)
		if err != nil {
			if gone, _ := a.Store.GetTask(ctx, in.TaskID); gone == nil {
				return PostTaskResultOutput{Gone: true}, nil
			}
			return PostTaskResultOutput{}, fmt.Errorf("publish the result: %w", err)
		}
		file = &f
		content = clipBytes(content, MaxTaskMessageBytes)
	}
	var taskFile *store.TaskFile
	if file != nil {
		taskFile = &store.TaskFile{ID: file.ID, Name: file.Name, Size: file.Size}
	}
	end, err := a.Store.EndTask(ctx, in.TaskID, store.TaskEndedByTask, in.State, func(t store.BackgroundTask) store.Message {
		return TaskResultMessage(t, content, taskFile)
	})
	if err != nil {
		return PostTaskResultOutput{}, fmt.Errorf("end task: %w", err)
	}
	if end.Gone {
		return PostTaskResultOutput{Gone: true}, nil
	}
	if !end.Mine {
		return PostTaskResultOutput{}, nil // the sweep came first
	}
	ended := end.Task
	out := PostTaskResultOutput{
		MessageID: end.MessageID, SessionID: ended.SessionID, Participant: ended.Participant, TurnKey: ended.TurnKey,
		UserID: ended.UserID, UserName: ended.UserName, Channel: ended.Channel, ChannelID: ended.ChannelID,
		Wake: ended.State != store.BackgroundCancelled, File: file,
	}
	if !out.Wake {
		return out, nil
	}
	sess, err := a.Store.GetSession(ctx, ended.SessionID)
	if err != nil {
		return PostTaskResultOutput{}, fmt.Errorf("read session: %w", err)
	}
	if sess == nil {
		out.Wake = false
		return out, nil
	}
	out.SessionChannel, out.SessionChannelID = sess.Channel, sess.ChannelID
	out.SignReply = TaskSignsReply(*sess, ended.Participant)
	return out, nil
}

// TaskWoken records that the wake of a task's participant is done with:
// delivered, or given up with an end of the turn saying so. Until then,
// the sweep wakes it again.
func (a *TaskActivities) TaskWoken(ctx context.Context, id string) error {
	return a.Store.SetTaskWoken(ctx, id)
}

// TaskSignsReply tells whether the turn a task's end wakes signs its answer
// on the channel: as for a member's message, when its agent is not the
// session's.
func TaskSignsReply(sess store.Session, participant string) bool {
	return participant != sess.AgentID
}

// publish publishes a task's whole result as a file of the call that
// launched it.
func (a *TaskActivities) publish(ctx context.Context, t store.BackgroundTask, content string) (tool.FileRef, error) {
	if a.Publisher == nil {
		return tool.FileRef{}, temporal.NewNonRetryableApplicationError("this worker publishes no file", "NoPublisher", nil)
	}
	ctx = tool.WithCall(ctx, tool.CallContext{Turn: &tool.TurnRef{SessionID: t.SessionID, TurnKey: t.TurnKey}, CallID: t.CallID, UserID: t.UserID})
	ctx = tool.WithAgentID(ctx, t.Participant)
	ctx = tool.WithUserID(ctx, t.UserID)
	return a.Publisher.Publish(ctx, TaskResultFileName(t.Tool), []byte(content))
}

// TaskResultFileName is the file a task's whole result is published as.
func TaskResultFileName(toolName string) string {
	return "resultat-" + toolName + ".md"
}

// TaskResultMessage is the message that ends task t, as it ends: its
// result's start (content), the file holding all of it if any. A message
// of nobody: addressed to the task's agent, for the user who asked.
func TaskResultMessage(t store.BackgroundTask, content string, file *store.TaskFile) store.Message {
	ref := t.Ref()
	ref.File = file
	m := store.Message{Role: store.RoleUser, Kind: store.KindTaskResult, UserID: t.UserID, AgentID: t.Participant, Task: &ref}
	if content != "" {
		b, _ := json.Marshal(content)
		m.Content = string(b)
	}
	return m
}

// clipBytes is s cut to at most n bytes, on a rune boundary.
func clipBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// TaskSummary is a call's input as a task shows it: its text values, in
// the order they come, on one line, clipped. The model wrote it; the
// members and the prompt read it.
func TaskSummary(input json.RawMessage) string {
	dec := json.NewDecoder(bytes.NewReader(input))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return ""
	}
	var parts []string
	for dec.More() {
		if _, err := dec.Token(); err != nil { // the key
			break
		}
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			break
		}
		var text string
		if json.Unmarshal(v, &text) == nil {
			if text = strings.Join(strings.Fields(text), " "); text != "" {
				parts = append(parts, text)
			}
		}
	}
	summary := strings.Join(parts, " · ")
	if r := []rune(summary); len(r) > maxTaskSummaryRunes {
		summary = strings.TrimRight(string(r[:maxTaskSummaryRunes-1]), " ") + "…"
	}
	return summary
}

// maxTaskSummaryRunes bounds a task's summary.
const maxTaskSummaryRunes = 160
