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
	UpdateMachineStatus(ctx context.Context, id string, capabilities []string, claudeCode string) error
	SetMachinePaused(ctx context.Context, userID, id string, paused bool) error
	SetMachinePriority(ctx context.Context, userID, id string, priority int) error
	TouchMachines(ctx context.Context, ids []string) error
	ResetMachineConnections(ctx context.Context) error
	RevokeMachine(ctx context.Context, id, reason string) ([]store.Directive, error)

	GetDirective(ctx context.Context, id string) (*store.Directive, error)
	PendingDirectiveResults(ctx context.Context) ([]store.Directive, error)
	MarkDirectiveSent(ctx context.Context, id, conn string) (bool, error)
	OpenDirectives(ctx context.Context, machineID string) ([]store.Directive, error)
	SaveDirectiveResult(ctx context.Context, id string, result json.RawMessage) error
	CloseDirective(ctx context.Context, id, state, errText string) (bool, error)
	SweepDirectives(ctx context.Context, now time.Time) ([]store.Directive, error)

	SaveFile(ctx context.Context, f store.File, content []byte) (store.File, error)
	ListCallFiles(ctx context.Context, sessionID, turnKey, callID string) ([]store.File, error)
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
	// Notice shows a note on a participant's working line in a session
	// (the event activity.EventNotice, through session.Service.Observe):
	// what a directive of its turn does. Web only. Nil: none.
	Notice func(sessionID, participant, agent, text string)
	// FilesPublished tells a session's pages that a machine published files
	// for a turn (the event activity.EventFilePublished): its thread shows
	// them. Nil: none.
	FilesPublished func(sessionID, turnKey, agentID string, fileIDs []string)
	// MaxFileBytes is the largest file a machine publishes
	// (FILES_MAX_BYTES); zero = tool.DefaultMaxFileBytes.
	MaxFileBytes int64

	HeartbeatEvery time.Duration
	PingTimeout    time.Duration
	SweepEvery     time.Duration
	// NoteEvery is how often a directive's progress reaches its turn's line
	// at most (each note reloads the session's pages); zero = 5 s.
	NoteEvery time.Duration

	// id names this gateway's connections in the database.
	id string

	mu    sync.Mutex
	conns map[string]*conn // by machine ID
	// claimed are the directives whose result is being completed, by a
	// connection or by the sweep: one at a time.
	claimed map[string]bool
	// retrySlots bounds the sweep's completions under way.
	retrySlots chan struct{}
	// dupAlerts: when each machine's owner last heard of duplicate
	// connections.
	dupAlerts map[string]time.Time
	ctx       context.Context // the gateway's life: Run's
	stop      context.CancelFunc
	// completeWait is the first wait between two tries of a completion
	// (doubled each time); zero = 1 s. The tests shorten it.
	completeWait time.Duration

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
	g.claimed = map[string]bool{}
	g.retrySlots = make(chan struct{}, maxResultRetries)
	g.dupAlerts = map[string]time.Time{}
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
	if c := g.conn(d.MachineID); c != nil && g.isReady(c) {
		c.send(*d)
	}
	return nil
}

// isReady reports a connection welcomed: before, its machine would get a
// directive it cannot answer yet, and its reconcile sends what is unsent.
func (g *Gateway) isReady(c *conn) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return c.ready
}

// claim reserves the completion of a directive's result; false when it is
// under way already (a result sent twice, or the sweep at it).
func (g *Gateway) claim(id string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.claimed[id] {
		return false
	}
	g.claimed[id] = true
	return true
}

func (g *Gateway) release(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.claimed, id)
}

func (g *Gateway) completing(id string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.claimed[id]
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
		g.clearNote(d)
		if c != nil {
			c.detach(d.ID)
			c.write(machine.Message{Type: machine.TypeCancel, ID: d.ID, Text: "machine revoked"})
		}
		if len(d.TaskToken) == 0 {
			continue
		}
		cerr := temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("machine %q was revoked (%s) during the directive", m.Name, reason), machine.ErrTypeRevoked, nil)
		if err := g.complete(ctx, completeTries, d.TaskToken, nil, cerr); err != nil && !isNotFound(err) {
			log.Printf("machines: end directive %s of revoked machine %s: %v", d.ID, m.ID, err)
		}
	}
	if c != nil {
		c.closeWith(machine.CloseRevoked, "revoked")
	}
	g.alert(ctx, m.UserID, fmt.Sprintf("Machine « %s » révoquée : %s.", m.Name, reason))
	return nil
}

// completeTries is how many times a completion is tried before it is left
// to the sweep: 5 tries of up to 10 s, 1+2+4+8 s apart, about 80 s at worst
// (Temporal unreachable), 15 s when each try fails at once.
const completeTries = 5

// complete ends a directive's activity, tried up to tries times, retrying
// what may pass (Temporal briefly away). NotFound is final: the activity is
// gone.
func (g *Gateway) complete(ctx context.Context, tries int, token []byte, result any, err error) error {
	first := g.completeWait
	if first <= 0 {
		first = time.Second
	}
	var last error
	for i, wait := 0, first; i < tries; i, wait = i+1, wait*2 {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		last = g.Temporal.CompleteActivity(cctx, token, result, err)
		cancel()
		if last == nil || isNotFound(last) || isInvalidArgument(last) {
			return last
		}
		if i == tries-1 {
			break
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
		g.clearNote(d)
		if c := g.conn(d.MachineID); c != nil && c.detach(d.ID) {
			c.write(machine.Message{Type: machine.TypeCancel, ID: d.ID, Text: d.State})
		}
	}
	if err := g.Store.DeleteExpiredEnrollments(ctx, time.Now()); err != nil {
		log.Printf("machines: delete expired enrollments: %v", err)
	}
	g.retryResults(ctx)
}

// maxResultRetries bounds the completions the sweep has under way at once.
const maxResultRetries = 8

// retryResults completes again the directives whose result is in the
// database but whose completion failed (Temporal away): their machine does
// not send it again by itself. Each is tried once per sweep, in a goroutine
// of its own, at most maxResultRetries at a time: the sweep is the retry
// loop, and never waits on Temporal (one directive at a time, a Temporal out
// of reach would hold it up to a minute per directive). One the slots leave
// out waits for the next sweep.
func (g *Gateway) retryResults(ctx context.Context) {
	pending, err := g.Store.PendingDirectiveResults(ctx)
	if err != nil {
		log.Printf("machines: directives with a pending result: %v", err)
		return
	}
	for _, d := range pending {
		if !g.claim(d.ID) {
			continue // a connection, or a former sweep, completes it
		}
		var m machine.Message
		if err := json.Unmarshal(d.Result, &m); err != nil || m.ID != d.ID {
			log.Printf("machines: stored result of %s unreadable: %v", d.ID, err)
			g.release(d.ID)
			continue
		}
		select {
		case g.retrySlots <- struct{}{}:
		default:
			g.release(d.ID)
			continue
		}
		go func() {
			defer func() {
				g.release(d.ID)
				<-g.retrySlots
			}()
			if err := g.finish(ctx, 1, d, m); err != nil {
				log.Printf("machines: complete directive %s again: %v", d.ID, err)
			} else if c := g.conn(d.MachineID); c != nil && c.detach(d.ID) {
				c.write(machine.Message{Type: machine.TypeAck, ID: d.ID})
			}
		}()
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

// kindLabel names a directive's work on its turn's line.
func kindLabel(kind string) string {
	switch kind {
	case machine.KindAnalyzeRepo:
		return "Analyse"
	case machine.KindImplementFeature:
		return "Implémentation"
	}
	return "Tâche « " + kind + " »"
}

// note shows, on the line of the turn d works for, that it runs on machine
// name, and how far it is (progress, untrusted text from the machine, cut).
func (g *Gateway) note(d store.Directive, name, progress string) {
	if g.Notice == nil || d.SessionID == "" {
		return
	}
	text := fmt.Sprintf("%s sur la machine « %s »", kindLabel(d.Kind), name)
	if progress != "" {
		text += " — " + machine.Cut(progress, 200)
	}
	g.Notice(d.SessionID, d.Participant, d.Agent, text)
}

// clearNote takes a directive's note off its turn's line: it is over.
func (g *Gateway) clearNote(d store.Directive) {
	if g.Notice != nil && d.SessionID != "" {
		g.Notice(d.SessionID, d.Participant, d.Agent, "")
	}
}

// SetPaused pauses or resumes a machine of userID's.
func (g *Gateway) SetPaused(ctx context.Context, userID, machineID string, paused bool) error {
	err := g.Store.SetMachinePaused(ctx, userID, machineID, paused)
	if errors.Is(err, store.ErrMachineNotFound) {
		return ErrMachineNotFound
	}
	return err
}

// SetPriority sets a machine's priority, between MinPriority and
// MaxPriority: the highest is chosen first.
func (g *Gateway) SetPriority(ctx context.Context, userID, machineID string, priority int) error {
	if priority < MinPriority || priority > MaxPriority {
		return fmt.Errorf("priority %d out of [%d, %d]", priority, MinPriority, MaxPriority)
	}
	err := g.Store.SetMachinePriority(ctx, userID, machineID, priority)
	if errors.Is(err, store.ErrMachineNotFound) {
		return ErrMachineNotFound
	}
	return err
}

// The priorities a user may give a machine.
const (
	MinPriority = -10
	MaxPriority = 10
)
