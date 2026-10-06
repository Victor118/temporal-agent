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
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sdkactivity "go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/victor/temporal-agent/activity"
	"github.com/victor/temporal-agent/claudecode"
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
	t    *testing.T
	st   *store.PostgresStore
	db   *sql.DB       // removes the test's users
	tc   client.Client // the worker's and the test's
	gwTC client.Client // the gateway's: completions carry its identity
	// flaky wraps it: its next completions can be made to fail.
	flaky *flakyTemporal
	queue string
	// fallback is analyze_repo's fallback queue, served by a stand-in.
	fallback string
	// notes are the notes the gateway put on turns' lines.
	notes chan smokeNote
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

	e := &smokeEnv{t: t, st: st, db: db, tc: tc, gwTC: gwTC, flaky: &flakyTemporal{Temporal: gwTC}, queue: "smoke-machines-" + smokeRandom(t),
		fallback: "smoke-fallback-" + smokeRandom(t), notes: make(chan smokeNote, 256)}
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
	w.RegisterWorkflow(workflow.CodingRunWorkflow)
	w.RegisterActivity(&activity.MachineActivities{Store: st, Handoff: activity.NewHTTPDirectiveHandoff(e.base, smokeInternalKey),
		Routing: activity.CodingRouting{Machines: true, AnalyzeQueue: e.fallback}})
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)

	// The fallback queue: a stand-in for the coding containers, which
	// answer the probe and run AnalyzeFallbackWorkflow.
	fw := worker.New(tc, e.fallback, worker.Options{})
	fw.RegisterActivityWithOptions(func(context.Context) (activity.ProbeRunWorkerOutput, error) {
		return activity.ProbeRunWorkerOutput{}, nil
	}, sdkactivity.RegisterOptions{Name: "ProbeRunWorker"})
	fw.RegisterWorkflowWithOptions(func(ctx sdkworkflow.Context, fin workflow.AnalyzeFallbackInput) (workflow.ClaudeCodeOutput, error) {
		var in workflow.AnalyzeRepoInput
		json.Unmarshal(fin.Input, &in)
		return workflow.ClaudeCodeOutput{Repo: in.Repo, Report: "from the fallback, for " + in.UserID + ", as " + sdkworkflow.GetInfo(ctx).WorkflowExecution.ID}, nil
	}, sdkworkflow.RegisterOptions{Name: "AnalyzeFallbackWorkflow"})
	if err := fw.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fw.Stop)
	return e
}

// serve starts a gateway on ln.
func (e *smokeEnv) serve(ln net.Listener) {
	g := &Gateway{Store: e.st, Temporal: e.flaky, completeWait: 50 * time.Millisecond, NoteEvery: 100 * time.Millisecond,
		Notice: func(session, participant, agent, text string) {
			select {
			case e.notes <- smokeNote{session, participant, agent, text}:
			default:
			}
		},
		HeartbeatEvery: smokeHeartbeatEvery, PingTimeout: time.Second, SweepEvery: 2 * time.Second,
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

	t.Run("activity gone (NotFound at a heartbeat): the machine is told to stop, the directive closed", func(t *testing.T) {
		m1.drain()
		run := e.echo(alice, "terminé", 30*time.Second, 0, 5*time.Second)
		m1.waitStarted(t)
		asked := time.Now()
		if err := e.tc.TerminateWorkflow(ctx, run.GetID(), "", "smoke: gone"); err != nil {
			t.Fatal(err)
		}
		select {
		case end := <-m1.ended:
			if lag := end.at.Sub(asked); lag > smokeHeartbeatEvery+2*time.Second {
				t.Errorf("the machine stopped %s after the termination", lag)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("the machine never stopped")
		}
		waitFor(t, "directive closed as gone", 5*time.Second, func() bool { _, s := e.directiveOf(run.GetID()); return s == store.DirectiveGone })
		waitFor(t, "result dropped and acked", 5*time.Second, func() bool { return resultFiles(dir) == 0 })
	})

	t.Run("completion failing with the connection sound: the sweep completes it, the connection stays", func(t *testing.T) {
		m1.drain()
		before, _ := (connect.State{Dir: dir}).Load()
		run := e.echo(alice, "encore", time.Second, 0, 10*time.Second)
		e.flaky.failures.Store(completeTries) // every try of the first completion
		out, err := result(t, run, 30*time.Second)
		if err != nil || out.Text != "encore" {
			t.Fatalf("result: %+v %v", out, err)
		}
		if n := e.flaky.failures.Load(); n > 0 {
			t.Errorf("%d failures left: the completion never failed", n)
		}
		waitFor(t, "result acked", 5*time.Second, func() bool { return resultFiles(dir) == 0 })
		// No reconnection: the token did not rotate.
		if after, _ := (connect.State{Dir: dir}).Load(); after.Token != before.Token {
			t.Error("the machine reconnected (its token rotated)")
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

	t.Run("completion failing, machine gone: the sweep completes it from the database", func(t *testing.T) {
		frank := e.user("frank")
		_, token := e.enrollToken(frank, "lointaine", []string{machine.KindEcho}, 1)
		raw := dialRaw(t, e.base, token)
		raw.hello(t, 1)
		raw.expect(t, machine.TypeWelcome)
		run := e.echo(frank, "balayé", time.Minute, 0, 15*time.Second)
		d := raw.expect(t, machine.TypeDirective)
		e.flaky.failures.Store(completeTries)
		out, _ := json.Marshal(machine.EchoOutput{Text: "balayé"})
		raw.send(t, machine.Message{Type: machine.TypeResult, ID: d.ID, Status: machine.StatusOK, Output: out})
		// Gone at once, never back: its result is the database's alone.
		raw.ws.CloseNow()
		res, err := result(t, run, 30*time.Second)
		if err != nil || res.Text != "balayé" {
			t.Fatalf("result: %+v %v", res, err)
		}
		if _, s := e.directiveOf(run.GetID()); s != store.DirectiveCompleted {
			t.Errorf("directive %s", s)
		}
	})

	t.Run("directive unknown to its machine after a reconnection: lost, not sent again", func(t *testing.T) {
		gina := e.user("gina")
		_, token := e.enrollToken(gina, "amnésique", []string{machine.KindEcho}, 1)
		raw := dialRaw(t, e.base, token)
		raw.hello(t, 1)
		raw.expect(t, machine.TypeWelcome)
		run := e.echo(gina, "oublié", time.Minute, 0, 15*time.Second)
		raw.expect(t, machine.TypeDirective)
		raw.ws.CloseNow()
		// Back (its first token still pending: never confirmed), knowing
		// nothing of it.
		raw = dialRaw(t, e.base, token)
		raw.hello(t, 1)
		raw.expect(t, machine.TypeWelcome)
		raw.readInBackground()
		_, err := result(t, run, 20*time.Second)
		if err == nil || !strings.Contains(err.Error(), "lost the directive") {
			t.Fatalf("workflow: %v", err)
		}
		if _, s := e.directiveOf(run.GetID()); s != store.DirectiveLost {
			t.Errorf("directive %s", s)
		}
	})

	t.Run("sweep: orphaned and expired directives, the connected machine told to stop", func(t *testing.T) {
		hugo := e.user("hugo")
		id, token := e.enrollToken(hugo, "balayée", []string{machine.KindEcho}, 2)
		raw := dialRaw(t, e.base, token)
		raw.hello(t, 2)
		raw.expect(t, machine.TypeWelcome)
		run := e.echo(hugo, "trop long", time.Minute, 0, time.Minute)
		d := raw.expect(t, machine.TypeDirective)
		// A reservation whose RunOnMachine never came, and the running one
		// past its deadline.
		orphan, _, err := e.st.PickMachine(ctx, store.PickRequest{DirectiveID: "zz-orphan-" + smokeRandom(t), UserID: hugo, Capabilities: []string{machine.KindEcho},
			Kind: machine.KindEcho, Input: json.RawMessage(`{}`), WorkflowID: "nowhere", RunID: "nowhere-" + smokeRandom(t), CallKey: "c",
			HandoffBy: time.Now().Add(-time.Second), Deadline: time.Now().Add(time.Hour), SeenAfter: time.Now().Add(-time.Minute)})
		if err != nil || orphan.MachineID != id {
			t.Fatalf("reservation: %+v %v", orphan, err)
		}
		if _, err := e.db.Exec("UPDATE machine_directives SET deadline = NOW() - INTERVAL '1 second' WHERE id = $1", d.ID); err != nil {
			t.Fatal(err)
		}
		if c := raw.expect(t, machine.TypeCancel); c.ID != d.ID {
			t.Errorf("cancel %+v", c)
		}
		if _, s := e.directiveOf(run.GetID()); s != store.DirectiveExpired {
			t.Errorf("running past its deadline: %s", s)
		}
		waitFor(t, "reservation orphaned", 5*time.Second, func() bool {
			got, _ := e.st.GetDirective(ctx, orphan.ID)
			return got != nil && got.State == store.DirectiveOrphaned
		})
	})

	t.Run("analyze_repo on the user's machine, its progress on the turn's line", func(t *testing.T) {
		ivan := e.user("ivan")
		repo := smokeGitRepo(t)
		a, _ := e.startAnalyzer(t, ivan, "atelier", repo, `echo '{"type":"system","subtype":"init","session_id":"s","apiKeySource":"none"}'
echo '{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Grep","input":{}}]},"session_id":"s"}'
sleep 1.5
echo '{"type":"result","subtype":"success","is_error":false,"result":"The handler is in main.go.","session_id":"s","num_turns":2,"total_cost_usd":0.05}'
`)
		session := uuid.NewString()
		run := e.analyze(t, session, ivan, repo)
		n := e.waitNote(t, session, func(s string) bool { return strings.Contains(s, "1 outil (dernier : Grep)") })
		if n.participant != "jarvis" || n.agent != "Jarvis" || !strings.HasPrefix(n.text, "Analyse sur la machine « atelier »") {
			t.Errorf("note %+v", n)
		}
		var out workflow.ClaudeCodeOutput
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := run.Get(ctx, &out); err != nil {
			t.Fatal(err)
		}
		if out.Report != "The handler is in main.go." || out.Machine != "atelier" || len(out.Commit) != 40 || out.PaidBy != "subscription" ||
			!strings.Contains(out.Content, `machine: "atelier"`) {
			t.Errorf("output %+v", out)
		}
		e.waitNote(t, session, func(s string) bool { return s == "" })
		if entries, _ := os.ReadDir(a.WorkDir); len(entries) != 0 {
			t.Errorf("clone left: %v", entries)
		}
	})

	t.Run("analyze_repo cancelled: the machine's CLI ended, its clone deleted", func(t *testing.T) {
		kate := e.user("kate")
		repo := smokeGitRepo(t)
		a, _ := e.startAnalyzer(t, kate, "bureau", repo, `echo '{"type":"system","subtype":"init","session_id":"s"}'
echo '{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Read","input":{}}]},"session_id":"s"}'
sleep 60
`)
		session := uuid.NewString()
		run := e.analyze(t, session, kate, repo)
		e.waitNote(t, session, func(s string) bool { return strings.Contains(s, "1 outil") })
		asked := time.Now()
		if err := e.tc.CancelWorkflow(ctx, run.GetID(), ""); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		var canceled *temporal.CanceledError
		if err := run.Get(ctx, nil); !errors.As(err, &canceled) {
			t.Errorf("workflow: %v", err)
		}
		if took := time.Since(asked); took > 15*time.Second {
			t.Errorf("cancelled in %s", took)
		}
		waitFor(t, "clone deleted", 10*time.Second, func() bool { entries, _ := os.ReadDir(a.WorkDir); return len(entries) == 0 })
		e.waitNote(t, session, func(s string) bool { return s == "" })
	})

	t.Run("analyze_repo refused for the machine's login: Claude Code withdrawn, the next run elsewhere", func(t *testing.T) {
		leo := e.user("leo")
		repo := smokeGitRepo(t)
		e.startAnalyzer(t, leo, "expiree", repo, `cat <<'EOF'
{"type":"system","subtype":"init","session_id":"s"}
{"type":"result","subtype":"success","is_error":true,"result":"Invalid API key · Please run /login","session_id":"s"}
EOF
`)
		var out workflow.ClaudeCodeOutput
		ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		// Refused before any tool ran: the same run goes to the fallback.
		if err := e.analyze(t, uuid.NewString(), leo, repo).Get(ctx, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(out.Report, "from the fallback") || !strings.Contains(out.Note, "login was refused") || out.Interrupted {
			t.Errorf("output %+v", out)
		}
		waitFor(t, "Claude Code withdrawn", 10*time.Second, func() bool {
			ms, _ := e.st.ListMachines(ctx, leo)
			return len(ms) == 1 && ms[0].ClaudeCode == "logged_out" && !ms[0].Can(machine.CapClaudeCode)
		})
		if err := e.analyze(t, uuid.NewString(), leo, repo).Get(ctx, &out); err != nil || !strings.HasPrefix(out.Report, "from the fallback") {
			t.Errorf("the next run: %+v %v", out, err)
		}
	})

	t.Run("analyze_repo refused by the machine (a repository it does not allow): the same run goes to the fallback", func(t *testing.T) {
		mona := e.user("mona")
		allowed, other := smokeGitRepo(t), smokeGitRepo(t)
		e.startAnalyzer(t, mona, "prudente", allowed, analyzeOK)
		var out workflow.ClaudeCodeOutput
		ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		if err := e.analyze(t, uuid.NewString(), mona, other).Get(ctx, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(out.Report, "from the fallback") || out.Machine != "" || out.Interrupted ||
			!strings.Contains(out.Note, `your machine "prudente" turned it down`) || !strings.Contains(out.Note, "--repos") {
			t.Errorf("output %+v", out)
		}
	})

	t.Run("analyze_repo with no machine: the fallback queue runs it", func(t *testing.T) {
		judith := e.user("judith")
		session := uuid.NewString()
		run := e.analyze(t, session, judith, "https://example.com/app.git")
		var out workflow.ClaudeCodeOutput
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := run.Get(ctx, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(out.Report, "from the fallback, for "+judith+", as "+session+":") || out.Machine != "" {
			t.Errorf("output %+v", out)
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

// analyzeOK is a stand-in CLI's successful analysis.
const analyzeOK = `echo '{"type":"system","subtype":"init","session_id":"s","apiKeySource":"none"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"s"}'
`

// smokeNote is a note the gateway put on a turn's line.
type smokeNote struct{ session, participant, agent, text string }

// smokeGitRepo makes a repository with one commit.
func smokeGitRepo(t *testing.T) string {
	t.Helper()
	repo := t.TempDir()
	for _, args := range [][]string{
		{"init", "--quiet", "-b", "main"},
		{"-c", "user.email=a@b", "-c", "user.name=a", "commit", "--quiet", "--allow-empty", "-m", "first"},
	} {
		if out, err := exec.Command("git", append([]string{"-C", repo}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
	}
	return repo
}

// startAnalyzer starts a machine of userID's that runs analyses with a
// stand-in for the claude CLI (script), on repo.
func (e *smokeEnv) startAnalyzer(t *testing.T, userID, name, repo, script string) (*connect.Analyzer, chan error) {
	t.Helper()
	id, token := e.enrollToken(userID, name, []string{machine.CapClaudeCode}, 1)
	dir := t.TempDir()
	if err := (connect.State{Dir: dir}).Save(connect.Config{Server: e.base, MachineID: id, Name: name, Token: token}); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\ncat >/dev/null\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	a := &connect.Analyzer{Runner: claudecode.Runner{Binary: bin}, Auth: claudecode.AuthSubscription, Repos: []string{repo},
		WorkDir: t.TempDir(), Environ: []string{claudecode.OAuthTokenEnv + "=smoke"}, Home: t.TempDir(), ProgressEvery: 200 * time.Millisecond}
	c := &connect.Client{State: connect.State{Dir: dir}, Executors: map[string]connect.Executor{machine.KindAnalyzeRepo: a.Run},
		Status: func() connect.Status {
			s := connect.Status{ClaudeCode: string(a.Login())}
			if s.ClaudeCode == string(claudecode.LoginOK) {
				s.Capabilities = []string{machine.CapClaudeCode}
			}
			return s
		},
		MaxDirectives: 1, OS: "linux", Version: "smoke", MinBackoff: 100 * time.Millisecond, MaxBackoff: 500 * time.Millisecond, StopWait: time.Second}
	a.OnLoginRefused = c.Refresh
	c.OnConnect = a.Retry
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runErr:
		case <-time.After(10 * time.Second):
		}
	})
	waitFor(t, "analyzer online", 10*time.Second, func() bool { return e.gateway().Online(id) })
	return a, runErr
}

// analyze starts analyze_repo as a turn of session's participant jarvis
// would, for userID.
func (e *smokeEnv) analyze(t *testing.T, session, userID, repo string) client.WorkflowRun {
	t.Helper()
	raw, _ := json.Marshal(map[string]string{"repo": repo, "task": "Where is the handler?", "user_id": userID, "agent": "Jarvis"})
	id := session + ":p:jarvis:m1:tool:analyze_repo:call-" + smokeRandom(t)
	run, err := e.tc.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{ID: id, TaskQueue: e.queue}, workflow.CodingRunWorkflow, json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.tc.TerminateWorkflow(context.Background(), run.GetID(), "", "smoke test over") })
	return run
}

// waitNote waits for a note of session whose text satisfies match.
func (e *smokeEnv) waitNote(t *testing.T, session string, match func(string) bool) smokeNote {
	t.Helper()
	timeout := time.After(20 * time.Second)
	for {
		select {
		case n := <-e.notes:
			if n.session == session && match(n.text) {
				return n
			}
		case <-timeout:
			t.Fatalf("no such note on session %s", session)
		}
	}
}

// flakyTemporal is the gateway's Temporal client, whose next completions
// fail as Temporal out of reach would.
type flakyTemporal struct {
	Temporal
	failures atomic.Int32
}

func (f *flakyTemporal) CompleteActivity(ctx context.Context, token []byte, result any, err error) error {
	if f.failures.Add(-1) >= 0 {
		return serviceerror.NewUnavailable("smoke: Temporal out of reach")
	}
	return f.Temporal.CompleteActivity(ctx, token, result, err)
}

// directiveOf is the state of the directive of a workflow, and its ID.
func (e *smokeEnv) directiveOf(workflowID string) (id, state string) {
	e.db.QueryRow("SELECT id, state FROM machine_directives WHERE workflow_id = $1", workflowID).Scan(&id, &state)
	return id, state
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
