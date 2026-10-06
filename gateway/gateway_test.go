package gateway

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/store"
)

func ids(ds []store.Directive) []string {
	var out []string
	for _, d := range ds {
		out = append(out, d.ID)
	}
	return out
}

// A hello carries on what the database holds open for the machine, and
// brings nothing else back.
func TestReconcile(t *testing.T) {
	open := []store.Directive{
		{ID: "listed-running", State: store.DirectiveRunning, SentConn: "c-old"},
		{ID: "listed-finished", State: store.DirectiveRunning, SentConn: "c-old"},
		{ID: "never-sent", State: store.DirectiveRunning},
		{ID: "lost", State: store.DirectiveRunning, SentConn: "c-old"},
		{ID: "sent-here", State: store.DirectiveRunning, SentConn: "c-now"},
		{ID: "reserved", State: store.DirectiveReserved},
	}
	p := reconcile(open, []string{"listed-running", "closed-running"}, []string{"listed-finished", "closed-finished"}, "c-now")
	for _, c := range []struct {
		name      string
		got, want []string
	}{
		{"attach", ids(p.attach), []string{"listed-running", "listed-finished"}},
		{"send", ids(p.send), []string{"never-sent"}},
		{"lost", ids(p.lost), []string{"lost"}},
		{"cancel", p.cancel, []string{"closed-running"}},
		{"ack", p.ack, []string{"closed-finished"}},
	} {
		if !slices.Equal(c.got, c.want) {
			t.Errorf("%s: %v, want %v", c.name, c.got, c.want)
		}
	}

	// A machine that lists what it was never given: cancelled, not carried.
	p = reconcile(nil, []string{"someone-elses"}, nil, "c")
	if len(p.attach) != 0 || !slices.Equal(p.cancel, []string{"someone-elses"}) {
		t.Errorf("foreign directive: %+v", p)
	}
}

func TestCompletion(t *testing.T) {
	out := json.RawMessage(`{"text":"hi"}`)
	res, err := completion(machine.Message{Status: machine.StatusOK, Output: out, Text: "done"})
	if err != nil || string(res.Output) != string(out) || res.Progress != "done" {
		t.Errorf("ok: %+v %v", res, err)
	}
	if _, err := completion(machine.Message{Status: machine.StatusCanceled}); !temporal.IsCanceledError(err) {
		t.Errorf("canceled: %v", err)
	}
	for status, typ := range map[string]string{machine.StatusStopping: machine.ErrTypeStopping, machine.StatusError: machine.ErrTypeFailed} {
		_, err := completion(machine.Message{Status: status, Error: "boom"})
		var appErr *temporal.ApplicationError
		if !errors.As(err, &appErr) || appErr.Type() != typ || !appErr.NonRetryable() {
			t.Errorf("%s: %v", status, err)
		}
	}
	for status, want := range map[string]string{
		machine.StatusOK: store.DirectiveCompleted, machine.StatusError: store.DirectiveFailed,
		machine.StatusCanceled: store.DirectiveCanceled, machine.StatusStopping: store.DirectiveStopping,
	} {
		if got := closedState(status); got != want {
			t.Errorf("%s closes as %s, want %s", status, got, want)
		}
	}
}

func TestMachineInfo(t *testing.T) {
	info, err := machineInfo(machine.EnrollRequest{Name: "  maison‮\x07  ", OS: "linux", Capabilities: []string{"echo"}, MaxDirectives: 99})
	if err != nil || info.Name != "maison" || info.MaxDirectives != 1 {
		t.Errorf("cleaned: %+v %v", info, err)
	}
	if info, _ := machineInfo(machine.EnrollRequest{}); info.Name != "machine" {
		t.Errorf("no name: %q", info.Name)
	}
	if info, _ := machineInfo(machine.EnrollRequest{Name: strings.Repeat("é", 100)}); len(info.Name) > 64 {
		t.Errorf("long name: %d bytes", len(info.Name))
	}
	if _, err := machineInfo(machine.EnrollRequest{Capabilities: []string{"<script>"}}); err == nil {
		t.Error("bad capability accepted")
	}
}
