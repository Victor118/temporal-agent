package tool

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/victor/temporal-agent/store"
)

// fakeFollowUps records the instructions attached, and answers as the
// store would for a task it does not hold, or one ended.
type fakeFollowUps struct {
	added []struct {
		session, participant, task string
		f                          store.TaskFollowUp
	}
	err error
}

func (f *fakeFollowUps) AddTaskFollowUp(_ context.Context, session, participant, id string, fu store.TaskFollowUp, _ int) error {
	if f.err != nil {
		return f.err
	}
	f.added = append(f.added, struct {
		session, participant, task string
		f                          store.TaskFollowUp
	}{session, participant, id, fu})
	return nil
}
func (f *fakeFollowUps) GetUser(_ context.Context, id string) (*store.User, error) {
	return &store.User{ID: id, DisplayName: "Alice"}, nil
}

func TestWhenTaskDone(t *testing.T) {
	st := &fakeFollowUps{}
	r := NewRegistry()
	RegisterTaskTools(r, st)
	call := func(ctx context.Context, input string) (string, error) {
		return r.Execute(ctx, "when_task_done", json.RawMessage(input))
	}
	turn := WithCall(context.Background(), CallContext{Turn: &TurnRef{SessionID: "s1", TurnKey: "m7.jarvis"}, CallID: "c2"})
	turn = WithUserID(WithAgentID(turn, "jarvis"), "u-alice")

	out, err := call(turn, `{"task_id":" s1:p:jarvis:m4:bg:c1 ","instruction":"then open a PR"}`)
	if err != nil || !strings.Contains(out, "Attached") {
		t.Fatalf("%q %v", out, err)
	}
	if a := st.added[0]; a.session != "s1" || a.participant != "jarvis" || a.task != "s1:p:jarvis:m4:bg:c1" ||
		a.f.Text != "then open a PR" || a.f.UserID != "u-alice" || a.f.UserName != "Alice" {
		t.Errorf("added %+v", a)
	}

	for _, c := range []struct {
		name  string
		ctx   context.Context
		input string
		err   error
		want  string
	}{
		{"no instruction", turn, `{"task_id":"x","instruction":" "}`, nil, "instruction is required"},
		{"too long", turn, `{"task_id":"x","instruction":"` + strings.Repeat("a", MaxFollowUpRunes+1) + `"}`, nil, "too long"},
		{"a sub-agent", WithAgentID(turn, "smith"), `{"task_id":"x","instruction":"y"}`, nil, "only the agent that launched"},
		{"no turn", WithAgentID(WithCall(context.Background(), CallContext{CallID: "c"}), "jarvis"), `{"task_id":"x","instruction":"y"}`, nil, "only a session's turn"},
		{"unknown", turn, `{"task_id":"x","instruction":"y"}`, store.ErrTaskNotFound, "no background task of yours"},
		{"over", turn, `{"task_id":"x","instruction":"y"}`, store.ErrTaskOver, "has ended"},
	} {
		st.err = c.err
		if _, err := call(c.ctx, c.input); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}
