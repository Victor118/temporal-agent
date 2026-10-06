// Package gateway is the server's side of the machines
// (docs/design/machines.md): it enrolls them, holds their WebSocket, hands
// them the directives RunOnMachine gives it, and speaks to Temporal as a
// client for them: heartbeats while a machine answers its pings, the
// activity's completion with its result. A machine never sees a task token,
// the database or Temporal.
//
// One replica (phases 0 to 2): the gateway holding a machine's connection
// is the one every directive reaches.
package gateway

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/auth"
	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/store"
)

// Store is what the gateway reads and writes. *store.PostgresStore is one.
type Store interface {
	CreateMachineEnrollment(ctx context.Context, e store.MachineEnrollment) error
	CountPendingDeviceRequests(ctx context.Context, now time.Time) (int, error)
	FindDeviceRequest(ctx context.Context, userCode string, now time.Time) (*store.MachineEnrollment, error)
	ApproveDeviceRequest(ctx context.Context, id, userCode, userID string, now time.Time) error
	RedeemMachineEnrollment(ctx context.Context, kind, secretHash string, info store.MachineInfo, m store.Machine, tokenHash string, now time.Time) (store.Machine, error)
	DeleteExpiredEnrollments(ctx context.Context, now time.Time) error

	ListMachines(ctx context.Context, userID string) ([]store.Machine, error)
	GetMachine(ctx context.Context, id string) (*store.Machine, error)
	MachineByToken(ctx context.Context, tokenHash string) (*store.Machine, store.TokenUse, error)
	RotateMachineToken(ctx context.Context, id, presentedHash, nextHash string) error
	ConfirmMachineToken(ctx context.Context, id, currentHash string) error
	MachineConnected(ctx context.Context, id, gateway, addr string, hello store.MachineInfo) (string, error)
	MachineDisconnected(ctx context.Context, id, gateway string) error
	TouchMachines(ctx context.Context, ids []string) error
	ResetMachineConnections(ctx context.Context) error
	RevokeMachine(ctx context.Context, id, reason string) ([]store.Directive, error)

	GetDirective(ctx context.Context, id string) (*store.Directive, error)
	MarkDirectiveSent(ctx context.Context, id, conn string) (bool, error)
	OpenDirectives(ctx context.Context, machineID string) ([]store.Directive, error)
	SaveDirectiveResult(ctx context.Context, id string, result json.RawMessage) error
	CloseDirective(ctx context.Context, id, state, errText string) (bool, error)
	SweepDirectives(ctx context.Context, now time.Time) ([]store.Directive, error)
}

// Temporal is the gateway's side of Temporal: a client, never a worker.
// client.Client is one.
type Temporal interface {
	RecordActivityHeartbeat(ctx context.Context, taskToken []byte, details ...interface{}) error
	CompleteActivity(ctx context.Context, taskToken []byte, result interface{}, err error) error
}

// Defaults of the gateway's timings.
const (
	// DefaultHeartbeatEvery: the design's 30 s, well inside RunOnMachine's
	// heartbeat timeout (5 min), so a stop reaches a machine in 30 s.
	DefaultHeartbeatEvery = 30 * time.Second
	DefaultPingTimeout    = 10 * time.Second
	DefaultSweepEvery     = time.Minute
	helloTimeout          = 10 * time.Second
	writeTimeout          = 10 * time.Second
	// deviceTTL is how long a device request waits for its approval, and
	// enrollmentTokenTTL how long an enrollment token waits for its machine.
	deviceTTL          = 10 * time.Minute
	enrollmentTokenTTL = 15 * time.Minute
	// maxPendingDevices bounds the device requests waiting at once: their
	// route needs no login.
	maxPendingDevices = 500
)

// Revocation reasons, shown to the machine's owner.
const (
	ReasonByOwner     = "révoquée par son propriétaire"
	ReasonTokenReused = "jeton remplacé présenté à nouveau : une copie existe"
)

// Gateway holds the machines' connections. Its zero timings are the
// defaults; Store and Temporal are required.
type Gateway struct {
	Store    Store
	Temporal Temporal
	// Alert tells a machine's owner what they must know (a machine
	// enrolled, revoked, connected from a new address, a copied token);
	// nil = logged only. Every alert is logged.
	Alert func(ctx context.Context, userID, text string)
	// ClientAddr is the address a request comes from, "" when unknown
	// (auth.ClientAddrs.Of): shown to the owner, and alerted on when it
	// changes. AddrsKnown: it is the client's own (auth.ClientAddrs.Known),
	// so the device requests may be limited per address; otherwise it may
	// be a proxy's, every client's.
	ClientAddr func(r *http.Request) string
	AddrsKnown bool

	HeartbeatEvery time.Duration
	PingTimeout    time.Duration
	SweepEvery     time.Duration

	// id names this gateway's connections in the database.
	id string

	mu    sync.Mutex
	conns map[string]*conn // by machine ID
	ctx   context.Context  // the gateway's life: Run's
	stop  context.CancelFunc

	// Wrong user codes, by user; device requests, by client address.
	codeTries   *auth.Throttle
	deviceTries *auth.Throttle
}

// Errors of the gateway's operations.
var (
	ErrUnknownDirective = errors.New("unknown directive")
	ErrDirectiveClosed  = errors.New("directive closed")
	ErrMachineNotFound  = errors.New("machine not found")
	ErrTooManyTries     = errors.New("too many tries")
	ErrUnknownCode      = errors.New("unknown or expired code")
	ErrBusy             = errors.New("too many enrollments under way")
)

// Start resets what a previous gateway left in the database and starts the
// sweep, for the life of ctx. Call it before serving.
func (g *Gateway) Start(ctx context.Context) error {
	g.ctx, g.stop = context.WithCancel(ctx)
	g.id = "gw-" + uuid.NewString()
	g.conns = map[string]*conn{}
	g.codeTries = auth.NewThrottle(10, 15*time.Minute)
	g.deviceTries = auth.NewThrottle(20, 10*time.Minute)
	// One replica: no connection is held anywhere but here.
	if err := g.Store.ResetMachineConnections(ctx); err != nil {
		return fmt.Errorf("reset machine connections: %w", err)
	}
	go g.sweepLoop()
	return nil
}

// Close ends every connection, without a word to the machines: what a
// stopping server does. They reconnect to the next one.
func (g *Gateway) Close() {
	g.stop()
	g.mu.Lock()
	conns := make([]*conn, 0, len(g.conns))
	for _, c := range g.conns {
		conns = append(conns, c)
	}
	g.mu.Unlock()
	for _, c := range conns {
		c.ws.CloseNow()
	}
	for _, c := range conns {
		<-c.done
	}
}

func (g *Gateway) heartbeatEvery() time.Duration {
	if g.HeartbeatEvery > 0 {
		return g.HeartbeatEvery
	}
	return DefaultHeartbeatEvery
}

func (g *Gateway) pingTimeout() time.Duration {
	if g.PingTimeout > 0 {
		return g.PingTimeout
	}
	return DefaultPingTimeout
}

func (g *Gateway) alert(ctx context.Context, userID, text string) {
	log.Printf("machines: alert to user %s: %s", userID, text)
	if g.Alert != nil {
		g.Alert(ctx, userID, text)
	}
}

func (g *Gateway) conn(machineID string) *conn {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.conns[machineID]
}

// Online reports a machine this gateway holds.
func (g *Gateway) Online(machineID string) bool { return g.conn(machineID) != nil }

// Deliver hands a running directive to its machine (RunOnMachine, through
// /internal/machines/directives, or in process in dev). A machine not
// connected gets it at its next hello; one that does not come back lets the
// activity time out.
func (g *Gateway) Deliver(ctx context.Context, directiveID string) error {
	d, err := g.Store.GetDirective(ctx, directiveID)
	switch {
	case err != nil:
		return err
	case d == nil:
		return ErrUnknownDirective
	case d.State != store.DirectiveRunning:
		return ErrDirectiveClosed
	}
	if c := g.conn(d.MachineID); c != nil {
		c.send(*d)
	}
	return nil
}

// Revoke revokes a machine of userID's: its connection is cut, its
// directives cancelled, their activities ended at once.
func (g *Gateway) Revoke(ctx context.Context, userID, machineID string) error {
	m, err := g.Store.GetMachine(ctx, machineID)
	if err != nil {
		return err
	}
	if m == nil || m.UserID != userID {
		return ErrMachineNotFound
	}
	return g.revoke(ctx, *m, ReasonByOwner)
}

// revoke revokes m, closes its open directives, ends their activities with
// an error that says so (rather than let the turn wait out the heartbeat
// timeout), and cuts its connection.
func (g *Gateway) revoke(ctx context.Context, m store.Machine, reason string) error {
	closed, err := g.Store.RevokeMachine(ctx, m.ID, reason)
	if errors.Is(err, store.ErrMachineNotFound) {
		return ErrMachineNotFound
	}
	if err != nil {
		return err
	}
	c := g.conn(m.ID)
	for _, d := range closed {
		if c != nil {
			c.detach(d.ID)
			c.write(machine.Message{Type: machine.TypeCancel, ID: d.ID, Text: "machine revoked"})
		}
		if len(d.TaskToken) == 0 {
			continue
		}
		cerr := temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("machine %q was revoked (%s) during the directive", m.Name, reason), machine.ErrTypeRevoked, nil)
		if err := g.complete(ctx, d.TaskToken, nil, cerr); err != nil && !isNotFound(err) {
			log.Printf("machines: end directive %s of revoked machine %s: %v", d.ID, m.ID, err)
		}
	}
	if c != nil {
		c.closeWith(machine.CloseRevoked, "revoked")
	}
	g.alert(ctx, m.UserID, fmt.Sprintf("Machine « %s » révoquée : %s.", m.Name, reason))
	return nil
}

// complete ends a directive's activity, retrying what may pass (Temporal
// briefly away), for up to about half a minute. NotFound is final: the
// activity is gone.
func (g *Gateway) complete(ctx context.Context, token []byte, result any, err error) error {
	var last error
	for i, wait := 0, time.Second; i < 5; i, wait = i+1, wait*2 {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		last = g.Temporal.CompleteActivity(cctx, token, result, err)
		cancel()
		if last == nil || isNotFound(last) || isInvalidArgument(last) {
			return last
		}
		select {
		case <-ctx.Done():
			return last
		case <-time.After(wait):
		}
	}
	return last
}

func isNotFound(err error) bool {
	var nf *serviceerror.NotFound
	return errors.As(err, &nf)
}

func isInvalidArgument(err error) bool {
	var ia *serviceerror.InvalidArgument
	return errors.As(err, &ia)
}

// sweepLoop closes, every SweepEvery, the directives nobody will end, and
// forgets the expired enrollments.
func (g *Gateway) sweepLoop() {
	every := g.SweepEvery
	if every <= 0 {
		every = DefaultSweepEvery
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-t.C:
			g.sweep(g.ctx)
		}
	}
}

func (g *Gateway) sweep(ctx context.Context) {
	swept, err := g.Store.SweepDirectives(ctx, time.Now())
	if err != nil {
		log.Printf("machines: sweep directives: %v", err)
	}
	for _, d := range swept {
		log.Printf("machines: directive %s on machine %s closed by the sweep (%s)", d.ID, d.MachineID, d.State)
		if c := g.conn(d.MachineID); c != nil && c.detach(d.ID) {
			c.write(machine.Message{Type: machine.TypeCancel, ID: d.ID, Text: d.State})
		}
	}
	if err := g.Store.DeleteExpiredEnrollments(ctx, time.Now()); err != nil {
		log.Printf("machines: delete expired enrollments: %v", err)
	}
}

// ServeDirectives is the internal API's /internal/machines/directives: a
// worker's RunOnMachine hands a directive over (activity.HTTPDirectiveHandoff).
// It takes INTERNAL_API_KEY, like /internal/notify; empty = closed.
func (g *Gateway) ServeDirectives(apiKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || apiKey == "" || subtle.ConstantTimeCompare([]byte(got), []byte(apiKey)) != 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		var in struct {
			DirectiveID string `json:"directive_id"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&in); err != nil || in.DirectiveID == "" {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		switch err := g.Deliver(r.Context(), in.DirectiveID); {
		case errors.Is(err, ErrUnknownDirective):
			http.Error(w, "unknown directive", http.StatusNotFound)
		case errors.Is(err, ErrDirectiveClosed):
			http.Error(w, "directive closed", http.StatusConflict)
		case err != nil:
			log.Printf("machines: deliver %s: %v", in.DirectiveID, err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}
}
