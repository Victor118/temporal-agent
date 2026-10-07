package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/victor/temporal-agent/store"
)

// MaxFollowUpRunes bounds an instruction attached to a background task.
const MaxFollowUpRunes = 1000

// maxFollowUps bounds the instructions one task holds.
const maxFollowUps = 10

// TaskFollowUps is what when_task_done reads and writes of the store.
type TaskFollowUps interface {
	AddTaskFollowUp(ctx context.Context, sessionID, participant, id string, f store.TaskFollowUp, max int) error
	GetUser(ctx context.Context, id string) (*store.User, error)
}

// RegisterTaskTools registers when_task_done: an instruction attached to
// one of the agent's background tasks, which its end's message starts
// with (docs/design/async-tasks.md §7). Safer than the model's memory of
// what it was asked meanwhile.
func RegisterTaskTools(registry *Registry, st TaskFollowUps) {
	registry.Register(&Tool{
		Name: "when_task_done",
		Description: "Attach an instruction to one of your background tasks still running: when the task ends, the message " +
			"that brings you its result starts with it, for you to apply then. Use it when the user says what to do once a " +
			"task of yours is done (\"when you have finished, …\") while it runs. The IDs of your tasks running are in your prompt.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"task_id": {"type": "string", "description": "The ID of the task, as your prompt lists it"},
				"instruction": {"type": "string", "description": "What to do when it ends, as the user asked it (1000 characters at most)"}
			},
			"required": ["task_id", "instruction"]
		}`),
		Kind:             ToolKindActivity,
		NeedsCallContext: true,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			return whenTaskDone(ctx, st, input)
		},
	})
}

func whenTaskDone(ctx context.Context, st TaskFollowUps, input json.RawMessage) (string, error) {
	var in struct {
		TaskID      string `json:"task_id"`
		Instruction string `json:"instruction"`
	}
	if err := json.Unmarshal(input, &in); err != nil {
		return "", fmt.Errorf("invalid input: %w", err)
	}
	in.TaskID, in.Instruction = strings.TrimSpace(in.TaskID), strings.TrimSpace(in.Instruction)
	switch {
	case in.TaskID == "":
		return "", errors.New("task_id is required: the ID of one of your background tasks, as your prompt lists it")
	case in.Instruction == "":
		return "", errors.New("instruction is required: what to do when the task ends")
	case utf8.RuneCountInString(in.Instruction) > MaxFollowUpRunes:
		return "", fmt.Errorf("the instruction is too long: %d characters at most", MaxFollowUpRunes)
	}
	call, ok := CallFromContext(ctx)
	if !ok {
		return "", errNoCall
	}
	// The participant's own turn only: a sub-agent launches no task, and
	// attaches to none.
	if call.Turn == nil || call.Turn.SessionID == "" {
		return "", errors.New("only a session's turn has background tasks")
	}
	participant := store.TurnParticipant(call.Turn.TurnKey)
	if participant == "" || AgentIDFromContext(ctx) != participant {
		return "", errors.New("only the agent that launched a background task attaches instructions to it")
	}
	f := store.TaskFollowUp{Text: in.Instruction, UserID: UserIDFromContext(ctx), At: time.Now().UTC()}
	if f.UserID != "" {
		if u, err := st.GetUser(ctx, f.UserID); err == nil && u != nil {
			f.UserName = u.Name()
		}
	}
	err := st.AddTaskFollowUp(ctx, call.Turn.SessionID, participant, in.TaskID, f, maxFollowUps)
	switch {
	case errors.Is(err, store.ErrTaskNotFound):
		return "", fmt.Errorf("no background task of yours has the ID %q in this session: your prompt lists those running", in.TaskID)
	case errors.Is(err, store.ErrTaskOver):
		return "", errors.New("that task has ended: its result is in the conversation, or about to be; apply the instruction when you read it")
	case errors.Is(err, store.ErrTooManyFollowUps):
		return "", fmt.Errorf("that task holds %d instructions already, the most it may", maxFollowUps)
	case err != nil:
		return "", fmt.Errorf("attach the instruction: %w", err)
	}
	return fmt.Sprintf("Attached to task %s: the message that brings you its result will start with it.", in.TaskID), nil
}
