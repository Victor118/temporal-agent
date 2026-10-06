package activity

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
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
	start   store.Directive
	startOK error
	token   []byte
	closed  []string
}

func (f *fakeMachineStore) PickMachine(_ context.Context, req store.PickRequest) (store.Directive, store.Machine, error) {
	f.pick = req
	return store.Directive{ID: req.DirectiveID}, store.Machine{ID: "m-1", Name: "maison"}, f.pickErr
}

func (f *fakeMachineStore) StartDirective(_ context.Context, id string, token []byte, _ string, _ time.Time) (store.Directive, error) {
	f.token = token
	d := f.start
	d.ID = id
	return d, f.startOK
}

func (f *fakeMachineStore) CloseDirective(_ context.Context, id, state, _ string) (bool, error) {
	f.closed = append(f.closed, id+":"+state)
	return true, nil
}

type handoffFunc func(ctx context.Context, id string) error

func (h handoffFunc) Deliver(ctx context.Context, id string) error { return h(ctx, id) }

func TestPickMachine(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	st := &fakeMachineStore{}
	env.RegisterActivity(&MachineActivities{Store: st})
	v, err := env.ExecuteActivity((&MachineActivities{}).PickMachine, PickMachineInput{UserID: "u-1", Capability: "echo", Kind: "echo",
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
	v, err = env.ExecuteActivity((&MachineActivities{}).PickMachine, PickMachineInput{UserID: "u-1", Capability: "echo"})
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
