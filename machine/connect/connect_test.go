package connect

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/victor/temporal-agent/machine"
)

// agent connect reaches neither the database nor Temporal: it does not even
// link them.
func TestConnect_LinksNoDatabaseNorTemporal(t *testing.T) {
	gobin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go tool")
	}
	out, err := exec.Command(gobin, "list", "-deps", ".").Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, dep := range strings.Fields(string(out)) {
		for _, banned := range []string{"go.temporal.io/", "github.com/jackc/", "database/sql", "github.com/victor/temporal-agent/store",
			"github.com/victor/temporal-agent/activity", "github.com/victor/temporal-agent/workflow"} {
			if strings.HasPrefix(dep, banned) {
				t.Errorf("agent connect depends on %s", dep)
			}
		}
	}
}

func TestCheckServer(t *testing.T) {
	for in, want := range map[string]string{
		"https://agent.example.com":      "https://agent.example.com",
		"https://agent.example.com/":     "https://agent.example.com",
		"https://agent.example.com:8443": "https://agent.example.com:8443",
		"http://localhost:8888":          "http://localhost:8888",
		"http://127.0.0.1:8888":          "http://127.0.0.1:8888",
		"http://[::1]:8888":              "http://[::1]:8888",
	} {
		if got, err := CheckServer(in); err != nil || got != want {
			t.Errorf("%s: %q %v", in, got, err)
		}
	}
	for _, in := range []string{"http://agent.example.com", "ftp://x", "agent.example.com", "https://x/path", "https://u:p@x", ""} {
		if _, err := CheckServer(in); err == nil {
			t.Errorf("%s accepted", in)
		}
	}
	if got := wsURL("https://x:1"); got != "wss://x:1/machines/connect" {
		t.Errorf("ws URL %q", got)
	}
}

func TestState(t *testing.T) {
	s := State{Dir: filepath.Join(t.TempDir(), "machine")}
	if _, err := s.Load(); !errors.Is(err, ErrNotEnrolled) {
		t.Errorf("before enrollment: %v", err)
	}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(Config{Server: "https://x", MachineID: "m", Token: "agm_secret"}); err != nil {
		t.Fatal(err)
	}
	c, err := s.Load()
	if err != nil || c.Token != "agm_secret" {
		t.Fatalf("load: %+v %v", c, err)
	}
	for path, want := range map[string]os.FileMode{s.Dir: 0o700, filepath.Join(s.Dir, configFile): 0o600} {
		if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != want {
			t.Errorf("%s: %v %v, want %v", path, fi.Mode().Perm(), err, want)
		}
	}

	if err := s.SaveResult(machine.Message{Type: machine.TypeResult, ID: "d-1", Status: machine.StatusOK}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveResult(machine.Message{ID: "../escape"}); err == nil {
		t.Error("a result named out of the directory")
	}
	if rs, err := s.Results(); err != nil || len(rs) != 1 || !s.HasResult("d-1") {
		t.Errorf("results: %+v %v", rs, err)
	}
	if err := s.DropResult("d-1"); err != nil || s.HasResult("d-1") {
		t.Errorf("drop: %v", err)
	}
	if err := s.DropResult("d-1"); err != nil {
		t.Errorf("drop twice: %v", err)
	}
}

// A directive running when agent connect died is reported as failed, never
// run again.
func TestClient_RecoverReportsWhatACrashLeft(t *testing.T) {
	s := State{Dir: t.TempDir()}
	s.Init()
	s.MarkRunning("d-crashed")
	s.MarkRunning("d-done")
	s.SaveResult(machine.Message{Type: machine.TypeResult, ID: "d-done", Status: machine.StatusOK})
	c := &Client{State: s}
	if err := c.recover(); err != nil {
		t.Fatal(err)
	}
	if marked, _ := s.Marked(); len(marked) != 0 {
		t.Errorf("still marked: %v", marked)
	}
	rs, _ := s.Results()
	status := map[string]string{}
	for _, r := range rs {
		status[r.ID] = r.Status
	}
	if status["d-crashed"] != machine.StatusError || status["d-done"] != machine.StatusOK {
		t.Errorf("results: %v", status)
	}
}

func TestEcho(t *testing.T) {
	in, _ := json.Marshal(machine.EchoInput{Text: "hi", DurationMS: 350, ProgressEveryMS: 100})
	var progresses []string
	out, err := Echo(context.Background(), in, func(p string) { progresses = append(progresses, p) })
	var echoed machine.EchoOutput
	if err != nil || json.Unmarshal(out, &echoed) != nil || echoed.Text != "hi" || echoed.Progresses != len(progresses) || len(progresses) < 2 {
		t.Errorf("echo: %s %v %v", out, err, progresses)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errCanceled)
	in, _ = json.Marshal(machine.EchoInput{Text: "hi", DurationMS: 10000})
	if _, err := Echo(ctx, in, func(string) {}); !errors.Is(err, errCanceled) {
		t.Errorf("cancelled echo: %v", err)
	}
	in, _ = json.Marshal(machine.EchoInput{DurationMS: -5})
	if _, err := Echo(context.Background(), in, func(string) {}); err == nil {
		t.Error("bad echo accepted")
	}
}

// fakeGateway is the gateway's side of one connection, scripted by the test.
type fakeGateway struct {
	conns chan *websocket.Conn
	auth  chan string
}

func newFakeGateway(t *testing.T) (*fakeGateway, string) {
	f := &fakeGateway{conns: make(chan *websocket.Conn, 4), auth: make(chan string, 4)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.auth <- r.Header.Get("Authorization")
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		f.conns <- ws
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	return f, srv.URL
}

func read(t *testing.T, ws *websocket.Conn, typ string) machine.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		var m machine.Message
		if err := wsjson.Read(ctx, ws, &m); err != nil {
			t.Fatalf("waiting for %s: %v", typ, err)
		}
		if m.Type == typ {
			return m
		}
	}
}

func write(t *testing.T, ws *websocket.Conn, m machine.Message) {
	t.Helper()
	if err := wsjson.Write(context.Background(), ws, m); err != nil {
		t.Fatal(err)
	}
}

// One connection's life: hello, rotation, a directive and its result kept
// until the ack, a cancellation, and the stop's machine_stopping.
func TestClient_Session(t *testing.T) {
	f, url := newFakeGateway(t)
	s := State{Dir: t.TempDir()}
	s.Init()
	s.Save(Config{Server: url, MachineID: "m", Token: "agm_one"})
	s.SaveResult(machine.Message{Type: machine.TypeResult, ID: "d-old", Status: machine.StatusOK})
	c := &Client{State: s, Executors: map[string]Executor{machine.KindEcho: Echo}, MaxDirectives: 2, OS: "linux",
		MinBackoff: 50 * time.Millisecond, StopWait: 500 * time.Millisecond}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	ws := <-f.conns
	if a := <-f.auth; a != "Bearer agm_one" {
		t.Errorf("authorization %q", a)
	}
	hello := read(t, ws, machine.TypeHello)
	if hello.Protocol != machine.Protocol || hello.MaxDirectives != 2 || !slicesEqual(hello.Capabilities, []string{"echo"}) ||
		!slicesEqual(hello.Finished, []string{"d-old"}) {
		t.Errorf("hello: %+v", hello)
	}
	write(t, ws, machine.Message{Type: machine.TypeWelcome, Name: "maison"})
	if r := read(t, ws, machine.TypeResult); r.ID != "d-old" {
		t.Errorf("kept result: %+v", r)
	}
	write(t, ws, machine.Message{Type: machine.TypeAck, ID: "d-old"})

	write(t, ws, machine.Message{Type: machine.TypeRotate, Token: "agm_two"})
	read(t, ws, machine.TypeRotated)
	if cfg, _ := s.Load(); cfg.Token != "agm_two" {
		t.Errorf("token after rotation: %q", cfg.Token)
	}

	in, _ := json.Marshal(machine.EchoInput{Text: "hi", DurationMS: 200, ProgressEveryMS: 100})
	write(t, ws, machine.Message{Type: machine.TypeDirective, ID: "d-1", Kind: machine.KindEcho, Input: in})
	write(t, ws, machine.Message{Type: machine.TypeDirective, ID: "d-1", Kind: machine.KindEcho, Input: in}) // twice: run once
	if p := read(t, ws, machine.TypeProgress); p.ID != "d-1" || !strings.HasPrefix(p.Text, "echo:") {
		t.Errorf("progress: %+v", p)
	}
	r := read(t, ws, machine.TypeResult)
	if r.ID != "d-1" || r.Status != machine.StatusOK || !strings.Contains(string(r.Output), `"hi"`) {
		t.Errorf("result: %+v", r)
	}
	if !s.HasResult("d-1") {
		t.Error("result not kept until the ack")
	}
	write(t, ws, machine.Message{Type: machine.TypeAck, ID: "d-1"})

	write(t, ws, machine.Message{Type: machine.TypeDirective, ID: "d-2", Kind: "claude-code"})
	if r := read(t, ws, machine.TypeResult); r.ID != "d-2" || r.Status != machine.StatusError {
		t.Errorf("unknown kind: %+v", r)
	}
	in, _ = json.Marshal(machine.EchoInput{Text: "long", DurationMS: 60000})
	write(t, ws, machine.Message{Type: machine.TypeDirective, ID: "d-3", Kind: machine.KindEcho, Input: in})
	write(t, ws, machine.Message{Type: machine.TypeCancel, ID: "d-3"})
	if r := read(t, ws, machine.TypeResult); r.ID != "d-3" || r.Status != machine.StatusCanceled {
		t.Errorf("cancelled: %+v", r)
	}

	write(t, ws, machine.Message{Type: machine.TypeDirective, ID: "d-4", Kind: machine.KindEcho, Input: in})
	time.Sleep(100 * time.Millisecond)
	stop()
	if r := read(t, ws, machine.TypeResult); r.ID != "d-4" || r.Status != machine.StatusStopping {
		t.Errorf("stopping: %+v", r)
	}
	go func() { // answers the close, as the gateway does
		for {
			var m machine.Message
			if wsjson.Read(context.Background(), ws, &m) != nil {
				return
			}
		}
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the client did not stop")
	}
	if !s.HasResult("d-4") || s.HasResult("d-1") {
		t.Error("what is kept after the stop")
	}
}

// Refused for good: the client stops instead of retrying.
func TestClient_RefusedStops(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unknown machine token", http.StatusUnauthorized)
	}))
	defer srv.Close()
	s := State{Dir: t.TempDir()}
	s.Init()
	s.Save(Config{Server: srv.URL, Token: "agm_x"})
	c := &Client{State: s, Executors: map[string]Executor{machine.KindEcho: Echo}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Run(ctx); !errors.Is(err, ErrRefused) {
		t.Errorf("run: %v", err)
	}
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
