package activity

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/store"
)

// turnStore is the store of a participant's turns in memory.
type turnStore struct {
	session *store.Session
	agents  map[string]store.Agent
	ends    map[string]store.Message
	err     error
}

func (s *turnStore) GetSession(context.Context, string) (*store.Session, error) {
	return s.session, s.err
}

func (s *turnStore) GetAgent(_ context.Context, id string) (*store.Agent, error) {
	if a, ok := s.agents[id]; ok {
		return &a, s.err
	}
	return nil, s.err
}

func (s *turnStore) HasTurnEnd(_ context.Context, _ string, turnKey string) (bool, error) {
	_, ok := s.ends[turnKey]
	return ok, s.err
}

func (s *turnStore) AppendTurnEnd(_ context.Context, _ string, turnKey string, msg store.Message) (int64, error) {
	if _, ok := s.ends[turnKey]; !ok {
		s.ends[turnKey] = msg
	}
	return int64(len(s.ends)), s.err
}

func errorType(err error) string {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) && appErr.NonRetryable() {
		return appErr.Type()
	}
	return ""
}

// A turn runs when its session is there, its message not answered yet, and
// its agent in the store: one created a moment ago included, which the
// worker's catalog would not know yet. A missing agent or session is final.
func TestCheckTurn(t *testing.T) {
	in := CheckTurnInput{SessionID: "s1", AgentID: "jarvis", TurnKey: "m3.jarvis"}
	newStore := func() *turnStore {
		return &turnStore{session: &store.Session{SessionID: "s1"}, agents: map[string]store.Agent{"jarvis": {ID: "jarvis", Name: "Jarvis"}}, ends: map[string]store.Message{}}
	}
	a := &TurnActivities{Store: newStore()}
	if out, err := a.CheckTurn(context.Background(), in); err != nil || out.Answered || out.AgentName != "Jarvis" {
		t.Errorf("a new turn: %+v, %v", out, err)
	}

	st := newStore()
	st.ends["m3.jarvis"] = store.TurnEnd("jarvis", "")
	delete(st.agents, "jarvis") // answered before the agent went: nothing to say
	if out, err := (&TurnActivities{Store: st}).CheckTurn(context.Background(), in); err != nil || !out.Answered {
		t.Errorf("an answered message: %+v, %v", out, err)
	}

	st = newStore()
	delete(st.agents, "jarvis")
	if _, err := (&TurnActivities{Store: st}).CheckTurn(context.Background(), in); errorType(err) != ErrTypeAgentNotFound {
		t.Errorf("a deleted agent: %v", err)
	}
	st = newStore()
	st.session = nil
	if _, err := (&TurnActivities{Store: st}).CheckTurn(context.Background(), in); errorType(err) != ErrTypeSessionGone {
		t.Errorf("a deleted session: %v", err)
	}
	st = newStore()
	st.err = errors.New("database away")
	if _, err := (&TurnActivities{Store: st}).CheckTurn(context.Background(), in); err == nil || errorType(err) != "" {
		t.Errorf("a store away: %v, want an error retried", err)
	}
}

// signalStarter records a SignalWithStart.
type signalStarter struct {
	workflowID, signal string
	arg                any
	options            client.StartWorkflowOptions
	workflow           any
	args               []any
	err                error
}

func (s *signalStarter) SignalWithStartWorkflow(_ context.Context, workflowID, signalName string, signalArg interface{}, options client.StartWorkflowOptions, workflow interface{}, workflowArgs ...interface{}) (client.WorkflowRun, error) {
	s.workflowID, s.signal, s.arg, s.options, s.workflow, s.args = workflowID, signalName, signalArg, options, workflow, workflowArgs
	return nil, s.err
}

// A relay is a SignalWithStart on the next participant, with what the
// relaying workflow encoded.
func TestRelay(t *testing.T) {
	c := &signalStarter{}
	in := RelayInput{WorkflowID: "s1:p:smith", WorkflowType: "ParticipantWorkflow", TaskQueue: "agent", Signal: "message",
		Message: json.RawMessage(`{"message_id":3}`), Start: json.RawMessage(`{"session_id":"s1","agent_id":"smith"}`)}
	if err := (&RelayActivities{Client: c}).Relay(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if c.workflowID != "s1:p:smith" || c.signal != "message" || c.options.ID != "s1:p:smith" || c.options.TaskQueue != "agent" || c.workflow != "ParticipantWorkflow" {
		t.Errorf("relayed %+v", c)
	}
	if string(c.arg.(json.RawMessage)) != `{"message_id":3}` || len(c.args) != 1 || string(c.args[0].(json.RawMessage)) != string(in.Start) {
		t.Errorf("arguments %v %v", c.arg, c.args)
	}
	c.err = errors.New("unavailable")
	if err := (&RelayActivities{Client: c}).Relay(context.Background(), in); err == nil {
		t.Error("a failed delivery reported none")
	}
}
