package activity

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/store"
)

type fakeMachineStore struct {
	pick    store.PickRequest
	pickErr error
	// existing is the call's directive an earlier attempt made.
	existing *store.Directive
	start    store.Directive
	startOK  error
	token    []byte
	closed   []string
}

func (f *fakeMachineStore) PickMachine(_ context.Context, req store.PickRequest) (store.Directive, store.Machine, error) {
	f.pick = req
	if f.existing != nil {
		return *f.existing, store.Machine{ID: "m-1", Name: "maison"}, nil
	}
	return store.Directive{ID: req.DirectiveID}, store.Machine{ID: "m-1", Name: "maison"}, f.pickErr
}

func (f *fakeMachineStore) StartDirective(_ context.Context, id string, token []byte, _ string, _ time.Time) (store.Directive, error) {
	f.token = token
	d := f.start
	d.ID = id
	return d, f.startOK
}

func (f *fakeMachineStore) ChooseLLMMachine(context.Context, string, []string, time.Time) (store.Machine, error) {
	return store.Machine{}, store.ErrNoMachine
}

func (f *fakeMachineStore) SetMachineAside(context.Context, string) error { return nil }

func (f *fakeMachineStore) CreateLLMDirective(_ context.Context, req store.LLMDirectiveRequest) (store.Directive, store.Machine, error) {
	return store.Directive{ID: req.DirectiveID}, store.Machine{ID: req.MachineID}, nil
}

func (f *fakeMachineStore) CloseDirective(_ context.Context, id, state, _ string) (bool, error) {
	f.closed = append(f.closed, id+":"+state)
	return true, nil
}

type handoffFunc func(ctx context.Context, id string) error

func (h handoffFunc) Deliver(ctx context.Context, id string) error { return h(ctx, id) }

func (h handoffFunc) DeliverLLM(ctx context.Context, id string, _ json.RawMessage) error {
	return h(ctx, id)
}

func TestPickMachine(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	st := &fakeMachineStore{}
	env.RegisterActivity(&MachineActivities{Store: st})
	v, err := env.ExecuteActivity((&MachineActivities{}).PickMachine, PickMachineInput{UserID: "u-1", Capabilities: []string{"echo"}, Kind: "echo",
		Input: json.RawMessage(`{}`), CallKey: "c", Timeout: time.Hour})
	var out PickMachineOutput
	if err != nil || v.Get(&out) != nil || out.MachineName != "maison" || out.DirectiveID == "" {
		t.Fatalf("pick: %+v %v", out, err)
	}
	if st.pick.UserID != "u-1" || st.pick.CallKey != "c" || st.pick.RunID == "" || !st.pick.Deadline.After(st.pick.HandoffBy) ||
		time.Since(st.pick.SeenAfter) < DefaultMachineOnlineWindow-time.Second {
		t.Errorf("request %+v", st.pick)
	}

	st.pickErr = store.ErrNoMachine
	v, err = env.ExecuteActivity((&MachineActivities{}).PickMachine, PickMachineInput{UserID: "u-1", Capabilities: []string{"echo"}})
	out = PickMachineOutput{}
	if err != nil || v.Get(&out) != nil || out.NoMachine == "" || out.DirectiveID != "" {
		t.Errorf("no machine: %+v %v", out, err)
	}
}

func TestRunOnMachine(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	st := &fakeMachineStore{}
	var delivered []string
	acts := &MachineActivities{Store: st, Handoff: handoffFunc(func(_ context.Context, id string) error {
		delivered = append(delivered, id)
		return nil
	})}
	env.RegisterActivity(acts)

	// Handed over, then pending: the gateway completes it.
	_, err := env.ExecuteActivity(acts.RunOnMachine, RunOnMachineInput{DirectiveID: "d-1"})
	if !errors.Is(err, activity.ErrResultPending) || len(delivered) != 1 || delivered[0] != "d-1" {
		t.Errorf("pending: %v (delivered %v)", err, delivered)
	}

	// Revoked before it started: said so, not run.
	st.start, st.startOK = store.Directive{State: store.DirectiveRevoked}, store.ErrDirectiveClosed
	_, err = env.ExecuteActivity(acts.RunOnMachine, RunOnMachineInput{DirectiveID: "d-2"})
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != machine.ErrTypeRevoked || len(delivered) != 1 {
		t.Errorf("revoked: %v", err)
	}

	// The gateway refuses it for good: closed, not retried.
	st.start, st.startOK = store.Directive{}, nil
	acts.Handoff = handoffFunc(func(context.Context, string) error { return errHandoffRefused })
	if _, err := env.ExecuteActivity(acts.RunOnMachine, RunOnMachineInput{DirectiveID: "d-3"}); err == nil || len(st.closed) != 1 || st.closed[0] != "d-3:failed" {
		t.Errorf("refused handoff: %v %v", err, st.closed)
	}
}

func TestHTTPDirectiveHandoff(t *testing.T) {
	status := http.StatusNoContent
	var got DirectiveHandoffInput
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		json.NewDecoder(r.Body).Decode(&got)
		if r.URL.Path != DirectivePath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	h := NewHTTPDirectiveHandoff(srv.URL, "k")
	if err := h.Deliver(context.Background(), "d-1"); err != nil || got.DirectiveID != "d-1" || auth != "Bearer k" {
		t.Errorf("deliver: %v %+v %q", err, got, auth)
	}
	for _, s := range []int{http.StatusUnauthorized, http.StatusNotFound, http.StatusConflict} {
		status = s
		if err := h.Deliver(context.Background(), "d-1"); !errors.Is(err, errHandoffRefused) {
			t.Errorf("%d: %v", s, err)
		}
	}
	status = http.StatusBadGateway
	if err := h.Deliver(context.Background(), "d-1"); err == nil || errors.Is(err, errHandoffRefused) {
		t.Errorf("502: %v", err)
	}
}

// skillReader is a worker's skills, by name.
type skillReader map[string]machine.RunSkill

func (r skillReader) RunSkills(names []string) RunSkillSet {
	set := RunSkillSet{Version: "abc123"}
	for _, n := range names {
		if s, ok := r[n]; ok {
			set.Skills = append(set.Skills, s)
		} else {
			set.Missing = append(set.Missing, n)
		}
	}
	return set
}

// The directive's input is final before the machine is reserved: the
// skills named are read, and added to it; a name not found is said; skills
// a run cannot take are a refusal, with nothing reserved.
func TestPickMachine_Skills(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	st := &fakeMachineStore{}
	reader := skillReader{"tdd": {Name: "tdd", Description: "Test first", Content: "RED, GREEN, REFACTOR."}, "BAD": {Name: "BAD", Content: "x"}}
	env.RegisterActivity(&MachineActivities{Store: st, Skills: reader})
	pick := func(names ...string) (PickMachineOutput, error) {
		st.pick = store.PickRequest{}
		v, err := env.ExecuteActivity((&MachineActivities{}).PickMachine, PickMachineInput{UserID: "u-1", Capabilities: []string{machine.CapClaudeCode},
			Kind: machine.KindAnalyzeRepo, Input: json.RawMessage(`{"repo":"r","task":"t"}`), CallKey: "c", Timeout: time.Hour, Skills: names})
		var out PickMachineOutput
		if err == nil {
			err = v.Get(&out)
		}
		return out, err
	}

	out, err := pick("tdd", "ghost")
	if err != nil || out.DirectiveID == "" || len(out.Skills) != 1 || out.Skills[0] != "tdd" || out.SkillsVersion != "abc123" ||
		len(out.SkillsMissing) != 1 || out.SkillsMissing[0] != "ghost: "+machine.SkillNotFound {
		t.Fatalf("pick: %+v %v", out, err)
	}
	var in machine.AnalyzeInput
	if err := json.Unmarshal(st.pick.Input, &in); err != nil || in.Repo != "r" || in.Task != "t" || len(in.Skills) != 1 ||
		in.Skills[0] != reader["tdd"] || in.SkillsVersion != "abc123" {
		t.Errorf("directive input %s %v", st.pick.Input, err)
	}
	// The machine must load them: run-skills, besides the kind's.
	if !slices.Equal(st.pick.Capabilities, []string{machine.CapClaudeCode}) || !slices.Equal(st.pick.Extra, []string{machine.CapRunSkills}) {
		t.Errorf("capabilities %v, extra %v", st.pick.Capabilities, st.pick.Extra)
	}

	// None has run-skills, one has Claude Code: no machine, saying which
	// and why.
	st.pickErr = &store.MachineLacksError{Machine: "vieille", Missing: []string{machine.CapRunSkills}}
	out, err = pick("tdd")
	if err != nil || out.DirectiveID != "" || !strings.Contains(out.NoMachine, `your machine "vieille" has Claude Code but its CLI lacks --plugin-dir`) ||
		out.Lacks != out.NoMachine {
		t.Errorf("lacks: %+v %v", out, err)
	}
	st.pickErr = nil

	// Made again for the same call: what the directive holds, not what the
	// skills are now.
	stored, _ := json.Marshal(machine.AnalyzeInput{Repo: "r", Task: "t", Skills: []machine.RunSkill{{Name: "old", Content: "x"}}, SkillsVersion: "first"})
	st.existing = &store.Directive{ID: "d-first", Input: stored}
	out, err = pick("old", "tdd")
	if err != nil || out.DirectiveID != "d-first" || !slices.Equal(out.Skills, []string{"old"}) || out.SkillsVersion != "first" ||
		!slices.Equal(out.SkillsMissing, []string{"tdd: " + machine.SkillNotFound}) {
		t.Errorf("replayed: %+v %v", out, err)
	}
	st.existing = nil

	out, err = pick("BAD")
	if err != nil || out.Refused == "" || out.DirectiveID != "" || st.pick.UserID != "" {
		t.Errorf("refused: %+v %v, reserved %+v", out, err, st.pick)
	}

	// None found: the run goes without, said so; the input is the model's,
	// and any machine with Claude Code takes it.
	out, err = pick("ghost")
	if err != nil || out.DirectiveID == "" || len(out.Skills) != 0 || len(out.SkillsMissing) != 1 || string(st.pick.Input) != `{"repo":"r","task":"t"}` ||
		len(st.pick.Extra) != 0 {
		t.Errorf("none found: %+v %v %s", out, err, st.pick.Input)
	}
}
