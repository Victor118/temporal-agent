package connect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/victor/temporal-agent/machine"
)

// Executor runs one kind of directive: its input, a way to say how far it
// is, its output. It stops when ctx ends, with context.Cause(ctx).
type Executor func(ctx context.Context, input json.RawMessage, progress func(string)) (json.RawMessage, error)

// Client is a connected machine. Its directives outlive its connections:
// a run goes on while the gateway restarts, and its result waits on disk
// for the next connection, until the gateway's ack.
type Client struct {
	State     State
	Executors map[string]Executor // by kind: the machine's capabilities
	// MaxDirectives is how many directives it runs at once (≥ 1).
	MaxDirectives int
	OS            string
	Version       string
	Log           *log.Logger
	// MinBackoff and MaxBackoff bound the wait between reconnections; zero
	// = 1 s and 1 min.
	MinBackoff, MaxBackoff time.Duration
	// HTTPClient dials the gateway; nil = the default, which checks the
	// server's certificate.
	HTTPClient *http.Client
	// StopWait bounds how long a stopping machine waits for the acks of
	// its last results; zero = 5 s.
	StopWait time.Duration
	// OnConnect is called at the start of each connection, before its
	// hello (Analyzer.Retry: a refused login is tried again).
	OnConnect func()
	// Status says what the machine can do now; nil: the capability of each
	// executor, always. StatusEvery is how often it is checked (and
	// announced when it changed); zero = 30 s.
	Status      func() Status
	StatusEvery time.Duration
	refresh     chan struct{}
	// PingEvery is how often the machine pings the gateway: a network that
	// dropped without a word otherwise leaves it waiting for ever. Zero =
	// 30 s.
	PingEvery time.Duration
	// LogEvery is how often a directive's progress is logged at most, for
	// the machine's owner watching its terminal; zero = 1 min.
	LogEvery time.Duration

	mu   sync.Mutex
	jobs map[string]*job
	ws   *websocket.Conn // the current connection; nil between two
	// runCtx is the directives' life: it ends when the machine stops.
	runCtx context.Context
	kill   context.CancelCauseFunc
	jobsWG sync.WaitGroup
}

type job struct {
	cancel   context.CancelCauseFunc
	progress string
	// sentAt is when its last progress went out; flush sends the latest
	// one when the interval is over; done: no progress goes out any more.
	sentAt time.Time
	flush  *time.Timer
	done   bool
	// loggedAt is when its progress was last logged, logged what.
	loggedAt time.Time
	logged   string
}

// Refusal is a directive the machine turns down before doing anything (no
// clone, no run): its executor returns one, and the workflow takes the
// directive elsewhere.
type Refusal struct{ Reason string }

func (r *Refusal) Error() string { return r.Reason }

// Refuse is a Refusal.
func Refuse(format string, args ...any) error {
	return &Refusal{Reason: fmt.Sprintf(format, args...)}
}

// Errors that end the client for good.
var (
	// ErrRefused: the server refuses this machine (unknown or revoked
	// token, or a protocol too old). Reconnecting cannot help.
	ErrRefused = errors.New("the server refuses this machine")
	// errStopping and errCanceled are why a directive stops: the machine
	// stops, or the gateway asked.
	errStopping = errors.New("agent connect stops")
	errCanceled = errors.New("cancelled by the server")
)

func (c *Client) logf(format string, args ...any) {
	if c.Log != nil {
		c.Log.Printf(format, args...)
	} else {
		log.Printf(format, args...)
	}
}

// Status is what the machine can do now: the capabilities it announces, and
// the state of its claude CLI ("ok", "logged_out", "absent"; "" = not
// said).
type Status struct {
	Capabilities []string
	ClaudeCode   string
}

func (s Status) equal(o Status) bool {
	return s.ClaudeCode == o.ClaudeCode && slices.Equal(s.Capabilities, o.Capabilities)
}

// Capabilities are what the machine announces now: Status's when set,
// else the capability of each kind it has an executor for.
func (c *Client) Capabilities() []string { return c.status().Capabilities }

func (c *Client) status() Status {
	if c.Status != nil {
		s := c.Status()
		slices.Sort(s.Capabilities)
		return s
	}
	var s Status
	for k := range c.Executors {
		if cap := machine.CapabilityOf(k); !slices.Contains(s.Capabilities, cap) {
			s.Capabilities = append(s.Capabilities, cap)
		}
	}
	slices.Sort(s.Capabilities)
	return s
}

// Refresh asks the machine to check what it can do now, and to tell the
// gateway at once if that changed (a run refused for its login).
func (c *Client) Refresh() {
	select {
	case c.refresh <- struct{}{}:
	default:
	}
}

// statusLoop tells the gateway what the machine can do whenever it changes,
// checked every StatusEvery and on Refresh: a login lost or back is
// announced without a reconnection.
func (c *Client) statusLoop(ctx context.Context, ws *websocket.Conn, sent Status) {
	every := c.StatusEvery
	if every <= 0 {
		every = 30 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-c.refresh:
		}
		now := c.status()
		if now.equal(sent) {
			continue
		}
		if err := c.write(ws, machine.Message{Type: machine.TypeCapabilities, Capabilities: now.Capabilities, ClaudeCode: now.ClaudeCode}); err != nil {
			ws.CloseNow()
			return
		}
		c.logf("connect: now running %v (Claude Code: %s)", now.Capabilities, now.ClaudeCode)
		sent = now
	}
}

// Run connects, and reconnects with a growing backoff, until ctx ends (the
// machine stops: its directives end as machine_stopping, and their results
// go out if the connection is there) or the server refuses the machine.
func (c *Client) Run(ctx context.Context) error {
	if c.MaxDirectives < 1 {
		c.MaxDirectives = 1
	}
	minB, maxB := c.MinBackoff, c.MaxBackoff
	if minB <= 0 {
		minB = time.Second
	}
	if maxB <= 0 {
		maxB = time.Minute
	}
	if err := c.State.Init(); err != nil {
		return err
	}
	c.jobs = map[string]*job{}
	c.refresh = make(chan struct{}, 1)
	c.runCtx, c.kill = context.WithCancelCause(context.Background())
	if err := c.recover(); err != nil {
		return err
	}
	backoff := minB
	for {
		start := time.Now()
		err := c.session(ctx)
		if ctx.Err() != nil {
			// The session may have ended before its shutdown stopped them.
			c.stopJobs()
			c.jobsWG.Wait()
			return nil
		}
		if errors.Is(err, ErrRefused) {
			c.kill(errStopping)
			c.jobsWG.Wait()
			return err
		}
		if time.Since(start) > time.Minute {
			backoff = minB
		}
		wait := backoff/2 + rand.N(backoff/2+1)
		c.logf("connect: disconnected (%v); again in %s", err, wait.Round(time.Millisecond))
		select {
		case <-ctx.Done():
			c.stopJobs()
			c.jobsWG.Wait()
			return nil
		case <-time.After(wait):
		}
		backoff = min(2*backoff, maxB)
	}
}

// recover turns the directives a crash left running into results: they did
// not finish, and are not run again (at most once).
func (c *Client) recover() error {
	marked, err := c.State.Marked()
	if err != nil {
		return err
	}
	for _, id := range marked {
		if !c.State.HasResult(id) {
			c.logf("connect: directive %s was running when agent connect stopped: reported as failed", id)
			if err := c.State.SaveResult(machine.Message{Type: machine.TypeResult, ID: id, Status: machine.StatusError,
				Error: "agent connect stopped during the directive"}); err != nil {
				return err
			}
		}
		if err := c.State.Unmark(id); err != nil {
			return err
		}
	}
	return nil
}

// session is one connection: hello, then the gateway's messages until it
// ends. ctx ending stops the machine: its directives end, their results go
// out, and the connection closes.
func (c *Client) session(ctx context.Context) error {
	cfg, err := c.State.Load()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRefused, err)
	}
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	ws, resp, err := websocket.Dial(dctx, wsURL(cfg.Server), &websocket.DialOptions{
		HTTPClient: c.HTTPClient,
		HTTPHeader: http.Header{"Authorization": {"Bearer " + cfg.Token}},
	})
	cancel()
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusUnauthorized {
			return fmt.Errorf("%w: its token is unknown or revoked; enroll it again (agent connect --join)", ErrRefused)
		}
		return err
	}
	defer ws.CloseNow()
	ws.SetReadLimit(machine.MaxMessageBytes)

	results, err := c.State.Results()
	if err != nil {
		return err
	}
	if c.OnConnect != nil {
		c.OnConnect()
	}
	status := c.status()
	hello := machine.Message{Type: machine.TypeHello, Protocol: machine.Protocol, AgentVersion: c.Version, OS: c.OS,
		Capabilities: status.Capabilities, ClaudeCode: status.ClaudeCode, MaxDirectives: c.MaxDirectives}
	c.mu.Lock()
	for id := range c.jobs {
		hello.Running = append(hello.Running, id)
	}
	c.mu.Unlock()
	for _, r := range results {
		hello.Finished = append(hello.Finished, r.ID)
	}
	if err := c.write(ws, hello); err != nil {
		return err
	}
	// From the hello on: a directive may come before the welcome, and its
	// progress and result must go out.
	c.setConn(ws)

	sctx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() {
		select {
		case <-sctx.Done():
		case <-ctx.Done():
			c.shutdown(ws)
		}
	}()
	go c.pings(sctx, ws)
	go c.statusLoop(sctx, ws, status)

	for {
		var m machine.Message
		if err := wsjson.Read(sctx, ws, &m); err != nil {
			c.setConn(nil)
			switch websocket.CloseStatus(err) {
			case machine.CloseRevoked, machine.CloseProtocol:
				return fmt.Errorf("%w: %v", ErrRefused, err)
			}
			return err
		}
		switch m.Type {
		case machine.TypeWelcome:
			c.logf("connect: connected to %s as %q", cfg.Server, m.Name)
			// What finished while no gateway listened.
			results, err := c.State.Results()
			if err != nil {
				return err
			}
			for _, r := range results {
				c.send(r)
			}
		case machine.TypeRotate:
			if m.Token == "" {
				continue
			}
			cfg.Token = m.Token
			if err := c.State.Save(cfg); err != nil {
				// Not confirmed: the gateway keeps the old token valid.
				c.logf("connect: keep the new token: %v", err)
				continue
			}
			c.write(ws, machine.Message{Type: machine.TypeRotated})
		case machine.TypeDirective:
			c.start(m)
		case machine.TypeCancel:
			c.cancel(m.ID)
		case machine.TypeAck:
			if err := c.State.DropResult(m.ID); err != nil {
				c.logf("connect: drop result %s: %v", m.ID, err)
			}
		case machine.TypeError:
			c.logf("connect: the server says: %s (%s)", m.Text, m.Code)
			if m.Code == machine.CodeProtocol || m.Code == machine.CodeRevoked {
				return fmt.Errorf("%w: %s", ErrRefused, m.Text)
			}
		}
	}
}

// pings checks the gateway is still there; one that does not answer ends
// the connection, and the machine connects again.
func (c *Client) pings(ctx context.Context, ws *websocket.Conn) {
	every := c.PingEvery
	if every <= 0 {
		every = 30 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := ws.Ping(pctx)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				c.logf("connect: the server does not answer its ping")
				ws.CloseNow()
			}
			return
		}
	}
}

// shutdown stops the machine on its connection: every directive ends as
// machine_stopping, the results go out, and it waits a little for their
// acks before it closes.
func (c *Client) shutdown(ws *websocket.Conn) {
	c.stopJobs()
	c.jobsWG.Wait()
	wait := c.StopWait
	if wait <= 0 {
		wait = 5 * time.Second
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if rs, err := c.State.Results(); err != nil || len(rs) == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	// The close handshake: 5 s at most, the library's bound.
	ws.Close(websocket.StatusNormalClosure, "machine stopping")
}

func (c *Client) stopJobs() {
	c.kill(errStopping)
}

func (c *Client) setConn(ws *websocket.Conn) {
	c.mu.Lock()
	c.ws = ws
	c.mu.Unlock()
}

func (c *Client) write(ws *websocket.Conn, m machine.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return wsjson.Write(ctx, ws, m)
}

// send writes on the current connection, if there is one: what is lost
// meanwhile is a progress (the next replaces it) or a result (kept on disk,
// sent at the next welcome).
func (c *Client) send(m machine.Message) {
	c.mu.Lock()
	ws := c.ws
	c.mu.Unlock()
	if ws == nil {
		return
	}
	if err := c.write(ws, m); err != nil {
		ws.CloseNow()
	}
}

// start runs a directive, once: one it runs or holds the result of is not
// started again.
func (c *Client) start(m machine.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.jobs[m.ID] != nil || c.State.HasResult(m.ID) || !idPattern.MatchString(m.ID) {
		return
	}
	fail := func(why string) {
		r := machine.Message{Type: machine.TypeResult, ID: m.ID, Status: machine.StatusRefused, Error: why}
		if err := c.State.SaveResult(r); err != nil {
			c.logf("connect: keep result %s: %v", m.ID, err)
		}
		go c.send(r)
	}
	exec := c.Executors[m.Kind]
	switch {
	case exec == nil:
		fail(fmt.Sprintf("this machine does not run %q directives", m.Kind))
		return
	case !hasAll(c.status().Capabilities, machine.CapabilitiesOf(m.Kind)):
		fail(fmt.Sprintf("this machine cannot run %q directives now (it needs %s: see agent connect's log)", m.Kind,
			strings.Join(machine.CapabilitiesOf(m.Kind), ", ")))
		return
	case len(c.jobs) >= c.MaxDirectives:
		fail(fmt.Sprintf("this machine runs %d directives at most", c.MaxDirectives))
		return
	}
	if err := c.State.MarkRunning(m.ID); err != nil {
		fail(fmt.Sprintf("the machine cannot keep track of the directive: %v", err))
		return
	}
	ctx, cancel := context.WithCancelCause(c.runCtx)
	if m.Deadline != nil {
		var stop context.CancelFunc
		ctx, stop = context.WithDeadline(ctx, *m.Deadline)
		inner := cancel
		cancel = func(cause error) { inner(cause); stop() }
	}
	j := &job{cancel: cancel, loggedAt: time.Now()}
	c.jobs[m.ID] = j
	c.jobsWG.Add(1)
	c.logf("connect: directive %s (%s) started", m.ID, m.Kind)
	go c.run(withDirective(ctx, m.ID), m, exec, j)
}

func (c *Client) run(ctx context.Context, m machine.Message, exec Executor, j *job) {
	defer c.jobsWG.Done()
	out, err := exec(ctx, m.Input, func(p string) { c.progress(m.ID, j, p) })
	j.cancel(nil)
	c.mu.Lock()
	j.done = true
	if j.flush != nil {
		j.flush.Stop()
	}
	r := machine.Message{Type: machine.TypeResult, ID: m.ID, Status: machine.StatusOK, Output: out, Text: j.progress}
	c.mu.Unlock()
	if err != nil {
		// What the run produced, if anything, goes along: a partial report.
		var refusal *Refusal
		switch cause := context.Cause(ctx); {
		case errors.As(err, &refusal):
			r.Status = machine.StatusRefused
			r.Error = machine.Cut(refusal.Reason, machine.MaxErrorBytes)
		case errors.Is(cause, errStopping):
			r.Status = machine.StatusStopping
		case errors.Is(cause, errCanceled):
			r.Status = machine.StatusCanceled
		default:
			r.Status = machine.StatusError
			r.Error = machine.Cut(err.Error(), machine.MaxErrorBytes)
		}
	}
	// Kept before it is unmarked: a crash in between leaves the result.
	if err := c.State.SaveResult(r); err != nil {
		c.logf("connect: keep result %s: %v", m.ID, err)
	}
	if err := c.State.Unmark(m.ID); err != nil {
		c.logf("connect: unmark %s: %v", m.ID, err)
	}
	c.mu.Lock()
	delete(c.jobs, m.ID)
	c.mu.Unlock()
	c.logf("connect: directive %s ended: %s", m.ID, r.Status)
	c.send(r)
}

// progress records a directive's progress and sends it, one per
// machine.ProgressInterval at most: within the interval, the latest waits
// for its end and replaces those before it (the gateway keeps the last one
// only, and limits what a machine sends).
func (c *Client) progress(id string, j *job, p string) {
	p = machine.Cut(p, machine.MaxProgressBytes)
	c.mu.Lock()
	defer c.mu.Unlock()
	j.progress = p
	// Its owner sees how far it is, a line a minute at most.
	every := c.LogEvery
	if every <= 0 {
		every = time.Minute
	}
	if p != j.logged && time.Since(j.loggedAt) >= every {
		j.loggedAt, j.logged = time.Now(), p
		c.logf("connect: directive %s: %s", id, p)
	}
	if j.done || j.flush != nil {
		return
	}
	wait := machine.ProgressInterval - time.Since(j.sentAt)
	if wait <= 0 {
		j.sentAt = time.Now()
		go c.send(machine.Message{Type: machine.TypeProgress, ID: id, Text: p})
		return
	}
	j.flush = time.AfterFunc(wait, func() {
		c.mu.Lock()
		j.flush = nil
		latest, done := j.progress, j.done
		j.sentAt = time.Now()
		c.mu.Unlock()
		if !done {
			c.send(machine.Message{Type: machine.TypeProgress, ID: id, Text: latest})
		}
	})
}

// hasAll reports every one of want in have.
func hasAll(have, want []string) bool {
	for _, w := range want {
		if !slices.Contains(have, w) {
			return false
		}
	}
	return true
}

// cancel stops a directive the gateway closed: a running one ends as
// cancelled; the result of a finished one is dropped (the gateway no longer
// wants it).
func (c *Client) cancel(id string) {
	c.mu.Lock()
	j := c.jobs[id]
	c.mu.Unlock()
	if j != nil {
		c.logf("connect: directive %s cancelled by the server", id)
		j.cancel(errCanceled)
		return
	}
	if err := c.State.DropResult(id); err != nil {
		c.logf("connect: drop result %s: %v", id, err)
	}
}
