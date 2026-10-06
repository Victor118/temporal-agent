package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
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

// connStore admits one machine, its rotation failing with rotateErr.
type connStore struct {
	Store
	rotateErr error
	sent      []string
	directive store.Directive
}

func (f *connStore) ResetMachineConnections(context.Context) error { return nil }
func (f *connStore) MachineByToken(context.Context, string) (*store.Machine, store.TokenUse, error) {
	return &store.Machine{ID: "m-1", UserID: "u-1", Name: "maison"}, store.TokenCurrent, nil
}
func (f *connStore) RotateMachineToken(context.Context, string, string, string) error {
	return f.rotateErr
}
func (f *connStore) MachineDisconnected(context.Context, string, string) error { return nil }
func (f *connStore) GetDirective(context.Context, string) (*store.Directive, error) {
	return &f.directive, nil
}
func (f *connStore) MarkDirectiveSent(_ context.Context, id, _ string) (bool, error) {
	f.sent = append(f.sent, id)
	return true, nil
}

// The database away is no refusal: the machine is told to come back (1011),
// not to stop for good (4001), which only a refused token gets.
func TestServeConnect_StoreErrorIsNoRefusal(t *testing.T) {
	for _, c := range []struct {
		err  error
		want websocket.StatusCode
	}{
		{errors.New("connection refused"), websocket.StatusInternalError},
		{fmt.Errorf("rotate: %w", store.ErrTokenRefused), machine.CloseRevoked},
	} {
		g := &Gateway{Store: &connStore{rotateErr: c.err}}
		if err := g.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(http.HandlerFunc(g.ServeConnect))
		ws, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(srv.URL, "http"),
			&websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer agm_x"}}})
		if err != nil {
			t.Fatal(err)
		}
		wsjson.Write(context.Background(), ws, machine.Message{Type: machine.TypeHello, Protocol: machine.Protocol, MaxDirectives: 1})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		var status websocket.StatusCode = -1
		for {
			var m machine.Message
			if err := wsjson.Read(ctx, ws, &m); err != nil {
				status = websocket.CloseStatus(err)
				break
			}
		}
		cancel()
		if status != c.want {
			t.Errorf("%v: closed with %d, want %d", c.err, status, c.want)
		}
		ws.CloseNow()
		srv.Close()
		g.Close()
	}
}

// A connection not welcomed yet gets no directive from Deliver: its
// reconcile sends what is unsent once its machine can answer.
func TestDeliver_NotBeforeTheWelcome(t *testing.T) {
	st := &connStore{directive: store.Directive{ID: "d-1", MachineID: "m-1", State: store.DirectiveRunning}}
	g := &Gateway{Store: st}
	if err := g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer g.stop()
	g.conns["m-1"] = &conn{g: g, m: store.Machine{ID: "m-1"}}
	if err := g.Deliver(context.Background(), "d-1"); err != nil || len(st.sent) != 0 {
		t.Errorf("delivered before the welcome: %v %v", err, st.sent)
	}
}

func TestFirstDuplicate_OncePerHour(t *testing.T) {
	g := &Gateway{dupAlerts: map[string]time.Time{}}
	now := time.Now()
	if !g.firstDuplicate("m-1", now) || g.firstDuplicate("m-1", now.Add(30*time.Minute)) || !g.firstDuplicate("m-2", now) {
		t.Error("within the hour")
	}
	if !g.firstDuplicate("m-1", now.Add(61*time.Minute)) {
		t.Error("after the hour")
	}
}

// The limit lets a full machine send its progresses at agent connect's
// pace, and more.
func TestMessageLimit(t *testing.T) {
	for _, max := range []int{1, machine.MaxDirectives} {
		perSecond, burst := messageLimit(max)
		if need := float64(max) * float64(time.Second/machine.ProgressInterval); float64(perSecond) < need+5 || burst < int(perSecond) {
			t.Errorf("%d directives: %v/s burst %d", max, perSecond, burst)
		}
	}
}
