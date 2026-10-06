package gateway

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	_ "github.com/jackc/pgx/v5/stdlib"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/machine/connect"
	"github.com/victor/temporal-agent/store"
	"github.com/victor/temporal-agent/workflow"
)

// The machines' mechanism against a real Temporal server and a throwaway
// database (phase 0 of docs/design/machines.md): gateway, worker and
// machines in this process, over a real WebSocket, timings shortened.
// Skipped without TEMPORAL_SMOKE_HOST (host:port) and TEST_DATABASE_URL.
// See CLAUDE.md for the command.

const (
	smokeHeartbeatEvery = time.Second
	smokeInternalKey    = "smoke-internal-key"
	smokeGatewayID      = "machines-gateway-smoke"
)

// smokeEnv is the test's world: the database, Temporal, a worker, and a
// gateway served on a fixed address, which can be restarted.
type smokeEnv struct {
	t     *testing.T
	st    *store.PostgresStore
	db    *sql.DB       // removes the test's users
	tc    client.Client // the worker's and the test's
	gwTC  client.Client // the gateway's: completions carry its identity
	queue string
	addr  string
	base  string

	mu  sync.Mutex
	g   *Gateway
	srv *http.Server
}

func smokeRandom(t *testing.T) string {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func newSmokeEnv(t *testing.T) *smokeEnv {
	host := os.Getenv("TEMPORAL_SMOKE_HOST")
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if host == "" || dbURL == "" {
		t.Skip("TEMPORAL_SMOKE_HOST and TEST_DATABASE_URL are needed: a real Temporal server and a throwaway database")
	}
	namespace := os.Getenv("TEMPORAL_SMOKE_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}
	st, err := store.NewPostgresStore(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	tc, err := client.Dial(client.Options{HostPort: host, Namespace: namespace})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tc.Close)
	gwTC, err := client.Dial(client.Options{HostPort: host, Namespace: namespace, Identity: smokeGatewayID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(gwTC.Close)

	e := &smokeEnv{t: t, st: st, db: db, tc: tc, gwTC: gwTC, queue: "smoke-machines-" + smokeRandom(t)}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e.addr = ln.Addr().String()
	e.base = "http://" + e.addr
	e.serve(ln)
	t.Cleanup(e.stopGateway)

	w := worker.New(tc, e.queue, worker.Options{})
	w.RegisterWorkflow(workflow.MachineEchoWorkflow)
	w.RegisterActivity(&activity.MachineActivities{Store: st, Handoff: activity.NewHTTPDirectiveHandoff(e.base, smokeInternalKey)})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)
	return e
}

// serve starts a gateway on ln.
func (e *smokeEnv) serve(ln net.Listener) {
	g := &Gateway{Store: e.st, Temporal: e.gwTC, HeartbeatEvery: smokeHeartbeatEvery, PingTimeout: time.Second, SweepEvery: 2 * time.Second,
		ClientAddr: func(r *http.Request) string { h, _, _ := net.SplitHostPort(r.RemoteAddr); return h }, AddrsKnown: true}
	if err := g.Start(context.Background()); err != nil {
		e.t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /machines/connect", g.ServeConnect)
	mux.HandleFunc("POST /machines/device", g.ServeDevice)
	mux.HandleFunc("POST /machines/device/token", g.ServeDeviceToken)
	mux.HandleFunc("POST /machines/enroll", g.ServeEnroll)
	mux.HandleFunc("POST "+activity.DirectivePath, g.ServeDirectives(smokeInternalKey))
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	e.mu.Lock()
	e.g, e.srv = g, srv
	e.mu.Unlock()
}

func (e *smokeEnv) gateway() *Gateway {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.g
}

func (e *smokeEnv) stopGateway() {
	e.mu.Lock()
	g, srv := e.g, e.srv
	e.g, e.srv = nil, nil
	e.mu.Unlock()
	if g == nil {
		return
	}
	srv.Close()
	g.Close()
}

// restartGateway serves a new gateway on the same address, as a server
// started again after a deploy.
func (e *smokeEnv) restartGateway() {
	var ln net.Listener
	var err error
	for range 50 {
		if ln, err = net.Listen("tcp", e.addr); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		e.t.Fatalf("listen again on %s: %v", e.addr, err)
	}
	e.serve(ln)
}

func (e *smokeEnv) user(name string) string {
	id := "zz-gw-" + name + "-" + smokeRandom(e.t)
	if err := e.st.CreateUser(context.Background(), store.User{ID: id, Email: id + "@example.com", Role: store.UserRoleStandard, PasswordHash: "h"}); err != nil {
		e.t.Fatal(err)
	}
	// Its machines, directives and enrollments go with it.
	e.t.Cleanup(func() { e.db.Exec("DELETE FROM users WHERE id = $1", id) })
	return id
}

// enrollToken enrolls a machine for userID with an enrollment token, and
// returns its ID and token.
func (e *smokeEnv) enrollToken(userID, name string, caps []string, maxDirectives int) (string, string) {
	ctx := context.Background()
	tok, _, err := e.gateway().CreateEnrollmentToken(ctx, userID)
	if err != nil {
		e.t.Fatal(err)
	}
	en := &connect.Enroller{Server: e.base, Out: io.Discard}
	grant, err := en.ByToken(ctx, machine.EnrollRequest{Name: name, OS: "linux", Capabilities: caps, MaxDirectives: maxDirectives, EnrollmentToken: tok})
	if err != nil {
		e.t.Fatal(err)
	}
	if _, err := en.ByToken(ctx, machine.EnrollRequest{Name: name, EnrollmentToken: tok}); !errors.Is(err, connect.ErrEnrollment) {
		e.t.Errorf("an enrollment token used twice: %v", err)
	}
	return grant.MachineID, grant.Token
}

// smokeMachine is a real `agent connect` client in process, its echo
// watched.
type smokeMachine struct {
	client  *connect.Client
	started chan string
	ended   chan echoEnd
	cancel  context.CancelFunc
	runErr  chan error
}

type echoEnd struct {
	at    time.Time
	cause error
}

func (e *smokeEnv) startMachine(dir string, maxDirectives int) *smokeMachine {
	m := &smokeMachine{started: make(chan string, 16), ended: make(chan echoEnd, 16), runErr: make(chan error, 1)}
	echo := func(ctx context.Context, in json.RawMessage, progress func(string)) (json.RawMessage, error) {
		m.started <- string(in)
		out, err := connect.Echo(ctx, in, progress)
		m.ended <- echoEnd{at: time.Now(), cause: context.Cause(ctx)}
		return out, err
	}
	m.client = &connect.Client{State: connect.State{Dir: dir}, Executors: map[string]connect.Executor{machine.KindEcho: echo},
		MaxDirectives: maxDirectives, OS: "linux", Version: "smoke", MinBackoff: 100 * time.Millisecond, MaxBackoff: 500 * time.Millisecond,
		StopWait: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go func() { m.runErr <- m.client.Run(ctx) }()
	e.t.Cleanup(func() {
		cancel()
		select {
		case <-m.runErr:
		case <-time.After(10 * time.Second):
		}
	})
	return m
}

// drain forgets what earlier directives left in the channels.
func (m *smokeMachine) drain() {
	for {
		select {
		case <-m.started:
		case <-m.ended:
		default:
			return
		}
	}
}

func (m *smokeMachine) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-m.started:
	case <-time.After(20 * time.Second):
		t.Fatal("the machine never started the directive")
	}
}

func (e *smokeEnv) echo(userID, text string, d, every, heartbeat time.Duration) client.WorkflowRun {
	run, err := e.tc.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{ID: "smoke-echo-" + smokeRandom(e.t), TaskQueue: e.queue},
		workflow.MachineEchoWorkflow, workflow.MachineEchoInput{UserID: userID, Text: text, Duration: d, ProgressEvery: every, HeartbeatTimeout: heartbeat})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { e.tc.TerminateWorkflow(context.Background(), run.GetID(), "", "smoke test over") })
	return run
}

func result(t *testing.T, run client.WorkflowRun, within time.Duration) (workflow.MachineEchoOutput, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	var out workflow.MachineEchoOutput
	err := run.Get(ctx, &out)
	if ctx.Err() != nil {
		t.Fatalf("no result within %s", within)
	}
	return out, err
}

// waitFor polls cond until it holds.
func waitFor(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %s", what, within)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func resultFiles(dir string) int {
	entries, _ := os.ReadDir(filepath.Join(dir, "results"))
	return len(entries)
}

func TestMachines_RealServer(t *testing.T) {
	e := newSmokeEnv(t)
	ctx := context.Background()

	// --- Enrollment by code: the machine asks, the user types its code. ---
	alice := e.user("alice")
	dir := t.TempDir()
	codes := make(chan string, 1)
	en := &connect.Enroller{Server: e.base, Out: codeCatcher(codes)}
	type enrolled struct {
		grant machine.TokenGrant
		err   error
	}
	got := make(chan enrolled, 1)
	go func() {
		g, err := en.ByCode(ctx, machine.EnrollRequest{Name: "maison", OS: "linux", Capabilities: []string{machine.KindEcho}, MaxDirectives: 1})
		got <- enrolled{g, err}
	}()
	var code string
	select {
	case code = <-codes:
	case <-time.After(10 * time.Second):
		t.Fatal("no user code shown")
	}
	if _, err := e.gateway().FindRequest(ctx, alice, "AAA-AAA"); !errors.Is(err, ErrUnknownCode) {
		t.Errorf("a wrong code: %v", err)
	}
	req, err := e.gateway().FindRequest(ctx, alice, strings.ToLower(code))
	if err != nil || req.Info.Name != "maison" || req.ClientAddr != "127.0.0.1" {
		t.Fatalf("find the request: %+v %v", req, err)
	}
	if err := e.gateway().Approve(ctx, alice, req.ID, code); err != nil {
		t.Fatal(err)
	}
	var res enrolled
	select {
	case res = <-got:
	case <-time.After(20 * time.Second):
		t.Fatal("no token after the approval")
	}
	if res.err != nil || res.grant.Token == "" {
		t.Fatalf("enrollment: %+v", res)
	}
	j1 := res.grant.Token
	if err := (connect.State{Dir: dir}).Save(connect.Config{Server: e.base, MachineID: res.grant.MachineID, Name: res.grant.Name, Token: j1}); err != nil {
		t.Fatal(err)
	}
	m1 := e.startMachine(dir, 1)
	waitFor(t, "machine online", 10*time.Second, func() bool { return e.gateway().Online(res.grant.MachineID) })
	// The rotation: the machine holds another token, written and confirmed.
	waitFor(t, "token rotated", 5*time.Second, func() bool {
		cfg, err := (connect.State{Dir: dir}).Load()
		if err != nil || cfg.Token == j1 {
			return false
		}
		_, use, _ := e.st.MachineByToken(ctx, machine.HashToken(j1))
		return use == store.TokenRetired
	})

	t.Run("directive, heartbeats and completion by the gateway", func(t *testing.T) {
		m1.drain()
		// 4 s of work under a 2.5 s heartbeat timeout: it ends only if the
		// gateway's heartbeats reach the activity.
		run := e.echo(alice, "bonjour", 4*time.Second, 300*time.Millisecond, 2500*time.Millisecond)
		m1.waitStarted(t)
		var details string
		waitFor(t, "a heartbeat with the machine's progress", 5*time.Second, func() bool {
			d, err := e.tc.DescribeWorkflowExecution(ctx, run.GetID(), "")
			if err != nil {
				return false
			}
			for _, pa := range d.PendingActivities {
				if pa.ActivityType.GetName() == "RunOnMachine" && pa.HeartbeatDetails != nil {
					var hb machine.Heartbeat
					if err := json.Unmarshal(pa.HeartbeatDetails.Payloads[0].Data, &hb); err == nil && strings.HasPrefix(hb.Progress, "echo:") {
						details = hb.Progress
						return true
					}
				}
			}
			return false
		})
		t.Logf("heartbeat details: %q", details)
		out, err := result(t, run, 20*time.Second)
		if err != nil || out.Text != "bonjour" || out.Progresses < 5 || out.Machine != "maison" {
			t.Fatalf("result: %+v %v", out, err)
		}
		// Completed by the gateway's client, not by a worker: the last
		// activity completed is RunOnMachine (PickMachine's is the worker's).
		completedBy := ""
		it := e.tc.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for it.HasNext() {
			ev, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if a := ev.GetActivityTaskCompletedEventAttributes(); a != nil {
				completedBy = a.GetIdentity()
			}
		}
		if completedBy != smokeGatewayID {
			t.Errorf("RunOnMachine completed by %q, want the gateway's client", completedBy)
		}
		waitFor(t, "result acked", 5*time.Second, func() bool { return resultFiles(dir) == 0 })
	})

	t.Run("cancel reaches the machine within a heartbeat", func(t *testing.T) {
		m1.drain()
		run := e.echo(alice, "long", 30*time.Second, 0, 5*time.Second)
		m1.waitStarted(t)
		asked := time.Now()
		if err := e.tc.CancelWorkflow(ctx, run.GetID(), ""); err != nil {
			t.Fatal(err)
		}
		select {
		case end := <-m1.ended:
			if lag := end.at.Sub(asked); lag > smokeHeartbeatEvery+2*time.Second {
				t.Errorf("the machine stopped %s after the cancellation", lag)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the machine never stopped")
		}
		_, err := result(t, run, 10*time.Second)
		var canceled *temporal.CanceledError
		if !errors.As(err, &canceled) {
			t.Errorf("workflow: %v, want cancelled", err)
		}
		waitFor(t, "result acked", 5*time.Second, func() bool { return resultFiles(dir) == 0 })
	})

	t.Run("gateway restarted mid-run: reconnected, carried on", func(t *testing.T) {
		m1.drain()
		run := e.echo(alice, "pendant", 6*time.Second, 300*time.Millisecond, 5*time.Second)
		m1.waitStarted(t)
		time.Sleep(time.Second)
		e.stopGateway()
		time.Sleep(2 * time.Second)
		e.restartGateway()
		out, err := result(t, run, 20*time.Second)
		if err != nil || out.Text != "pendant" {
			t.Fatalf("result: %+v %v", out, err)
		}
	})

	t.Run("result produced while the gateway is down: given at the reconnection", func(t *testing.T) {
		m1.drain()
		run := e.echo(alice, "coupure", 2*time.Second, 0, 10*time.Second)
		m1.waitStarted(t)
		e.stopGateway()
		select {
		case <-m1.ended:
		case <-time.After(10 * time.Second):
			t.Fatal("the echo never ended")
		}
		waitFor(t, "result kept on disk", 5*time.Second, func() bool { return resultFiles(dir) == 1 })
		time.Sleep(time.Second)
		e.restartGateway()
		out, err := result(t, run, 20*time.Second)
		if err != nil || out.Text != "coupure" {
			t.Fatalf("result: %+v %v", out, err)
		}
		waitFor(t, "result acked", 5*time.Second, func() bool { return resultFiles(dir) == 0 })
	})

	t.Run("two reservations at once on a full machine: its cap holds", func(t *testing.T) {
		m1.drain()
		waitFor(t, "machine online", 10*time.Second, func() bool { return e.gateway().Online(res.grant.MachineID) })
		a := e.echo(alice, "un", time.Second, 0, 5*time.Second)
		b := e.echo(alice, "deux", time.Second, 0, 5*time.Second)
		outA, errA := result(t, a, 20*time.Second)
		outB, errB := result(t, b, 20*time.Second)
		if errA != nil || errB != nil {
			t.Fatalf("results: %v %v", errA, errB)
		}
		ran, refused := 0, 0
		for _, o := range []workflow.MachineEchoOutput{outA, outB} {
			if o.NoMachine != "" {
				refused++
			} else if o.Text != "" {
				ran++
			}
		}
		if ran != 1 || refused != 1 {
			t.Errorf("%d ran, %d found no machine: %+v %+v", ran, refused, outA, outB)
		}
	})

	t.Run("machine lost past the heartbeat timeout: a clear failure, not run again", func(t *testing.T) {
		bob := e.user("bob")
		_, token := e.enrollToken(bob, "portable", []string{machine.KindEcho}, 1)
		raw := dialRaw(t, e.base, token)
		raw.hello(t, 1)
		raw.expect(t, machine.TypeWelcome)
		run := e.echo(bob, "perdu", time.Minute, 0, 3*time.Second)
		raw.expect(t, machine.TypeDirective)
		sent := time.Now()
		raw.ws.CloseNow() // gone without a word, never back
		_, err := result(t, run, 30*time.Second)
		if err == nil || !strings.Contains(err.Error(), "stopped answering") || !strings.Contains(err.Error(), "not run again") {
			t.Fatalf("workflow: %v", err)
		}
		var timeoutErr *temporal.TimeoutError
		if !errors.As(err, &timeoutErr) || timeoutErr.TimeoutType() != enumspb.TIMEOUT_TYPE_HEARTBEAT {
			t.Errorf("cause: %v", err)
		}
		if took := time.Since(sent); took > 15*time.Second {
			t.Errorf("failed %s after the machine left", took)
		}
		started := 0
		it := e.tc.GetWorkflowHistory(ctx, run.GetID(), run.GetRunID(), false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for it.HasNext() {
			ev, _ := it.Next()
			if a := ev.GetActivityTaskStartedEventAttributes(); a != nil && a.GetAttempt() > 1 {
				t.Errorf("attempt %d", a.GetAttempt())
			}
			if ev.GetActivityTaskScheduledEventAttributes().GetActivityType().GetName() == "RunOnMachine" {
				started++
			}
		}
		if started != 1 {
			t.Errorf("RunOnMachine scheduled %d times", started)
		}
	})

	t.Run("revocation: cut, and the activity ends at once", func(t *testing.T) {
		carol := e.user("carol")
		id, token := e.enrollToken(carol, "serveur", []string{machine.KindEcho}, 1)
		cdir := t.TempDir()
		if err := (connect.State{Dir: cdir}).Save(connect.Config{Server: e.base, MachineID: id, Name: "serveur", Token: token}); err != nil {
			t.Fatal(err)
		}
		m := e.startMachine(cdir, 1)
		waitFor(t, "machine online", 10*time.Second, func() bool { return e.gateway().Online(id) })
		run := e.echo(carol, "révoqué", time.Minute, 0, 30*time.Second)
		m.waitStarted(t)
		revoked := time.Now()
		if err := e.gateway().Revoke(ctx, "someone-else", id); !errors.Is(err, ErrMachineNotFound) {
			t.Errorf("revoked by another user: %v", err)
		}
		if err := e.gateway().Revoke(ctx, carol, id); err != nil {
			t.Fatal(err)
		}
		_, err := result(t, run, 10*time.Second)
		if err == nil || !strings.Contains(err.Error(), "revoked") {
			t.Fatalf("workflow: %v", err)
		}
		if took := time.Since(revoked); took > 3*time.Second {
			t.Errorf("ended %s after the revocation", took)
		}
		select {
		case err := <-m.runErr:
			if !errors.Is(err, connect.ErrRefused) {
				t.Errorf("machine: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("the revoked machine still runs")
		}
		if e.gateway().Online(id) {
			t.Error("still connected")
		}
	})

	t.Run("rotation: a replaced token presented again revokes the machine", func(t *testing.T) {
		dave := e.user("dave")
		id, j1 := e.enrollToken(dave, "vps", []string{machine.KindEcho}, 1)
		// A crash before the next token is written: the old one still connects.
		raw := dialRaw(t, e.base, j1)
		raw.hello(t, 1)
		raw.expect(t, machine.TypeWelcome)
		raw.expect(t, machine.TypeRotate)
		raw.ws.CloseNow()
		waitFor(t, "disconnected", 5*time.Second, func() bool { return !e.gateway().Online(id) })
		raw = dialRaw(t, e.base, j1)
		raw.hello(t, 1)
		raw.expect(t, machine.TypeWelcome)
		j2 := raw.expect(t, machine.TypeRotate).Token
		raw.send(t, machine.Message{Type: machine.TypeRotated})
		closed := raw.readInBackground()
		waitFor(t, "rotation confirmed", 5*time.Second, func() bool {
			_, use, _ := e.st.MachineByToken(ctx, machine.HashToken(j1))
			return use == store.TokenRetired
		})
		// A copy of j1 comes back: refused, and the machine is revoked, its
		// connection cut.
		if _, resp, err := websocket.Dial(ctx, "ws://"+e.addr+"/machines/connect",
			&websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + j1}}}); err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("j1 again: %v", err)
		}
		mm, err := e.st.GetMachine(ctx, id)
		if err != nil || mm.RevokedAt == nil || mm.RevokedReason != ReasonTokenReused {
			t.Fatalf("machine: %+v %v", mm, err)
		}
		select {
		case code := <-closed:
			if code != machine.CloseRevoked {
				t.Errorf("connection ended with %d", code)
			}
		case <-time.After(5 * time.Second):
			t.Error("the revoked machine's connection is still open")
		}
		if _, resp, err := websocket.Dial(ctx, "ws://"+e.addr+"/machines/connect",
			&websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + j2}}}); err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("j2 after the revocation: %v", err)
		}
	})

	t.Run("two live connections of one machine: both cut", func(t *testing.T) {
		erin := e.user("erin")
		_, token := e.enrollToken(erin, "double", []string{machine.KindEcho}, 1)
		first := dialRaw(t, e.base, token)
		first.hello(t, 1)
		first.expect(t, machine.TypeWelcome)
		// first answers pings: its read goes on in the background.
		closed := first.readInBackground()
		second := dialRaw(t, e.base, token) // the pending token: still valid
		second.hello(t, 1)
		if m := second.expect(t, machine.TypeError); m.Code != machine.CodeDuplicate {
			t.Errorf("second: %+v", m)
		}
		select {
		case code := <-closed:
			if code != machine.CloseDuplicate {
				t.Errorf("first closed with %d", code)
			}
		case <-time.After(5 * time.Second):
			t.Error("first still connected")
		}
	})
}

// codeCatcher passes on the user code `agent connect` shows.
func codeCatcher(codes chan<- string) io.Writer {
	re := regexp.MustCompile(`code ([A-Z0-9]{3}-[A-Z0-9]{3})`)
	return writerFunc(func(p []byte) (int, error) {
		if m := re.FindSubmatch(p); m != nil {
			select {
			case codes <- string(m[1]):
			default:
			}
		}
		return len(p), nil
	})
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// rawMachine speaks the protocol by hand: a machine the test controls to
// the frame.
type rawMachine struct {
	ws *websocket.Conn
}

func dialRaw(t *testing.T, base, token string) *rawMachine {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(base, "http")+"/machines/connect",
		&websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + token}}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { ws.CloseNow() })
	return &rawMachine{ws: ws}
}

func (r *rawMachine) send(t *testing.T, m machine.Message) {
	t.Helper()
	if err := wsjson.Write(context.Background(), r.ws, m); err != nil {
		t.Fatal(err)
	}
}

func (r *rawMachine) hello(t *testing.T, maxDirectives int) {
	r.send(t, machine.Message{Type: machine.TypeHello, Protocol: machine.Protocol, OS: "linux",
		Capabilities: []string{machine.KindEcho}, MaxDirectives: maxDirectives})
}

// readInBackground reads (and so answers pings) until the connection ends,
// and says with what status.
func (r *rawMachine) readInBackground() <-chan websocket.StatusCode {
	closed := make(chan websocket.StatusCode, 1)
	go func() {
		for {
			var m machine.Message
			if err := wsjson.Read(context.Background(), r.ws, &m); err != nil {
				closed <- websocket.CloseStatus(err)
				return
			}
		}
	}()
	return closed
}

// expect reads until a message of type typ, skipping the others.
func (r *rawMachine) expect(t *testing.T, typ string) machine.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		var m machine.Message
		if err := wsjson.Read(ctx, r.ws, &m); err != nil {
			t.Fatalf("waiting for %s: %v", typ, err)
		}
		if m.Type == typ {
			return m
		}
		if m.Type == machine.TypeError {
			t.Logf("error from the gateway: %+v", m)
			return m
		}
	}
}
