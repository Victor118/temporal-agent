package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"golang.org/x/time/rate"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/store"
)

// conn is one machine's connection.
type conn struct {
	g  *Gateway
	id string // names it in the database (machine_directives.sent_conn)
	m  store.Machine
	ws *websocket.Conn
	// ctx lives as long as the connection.
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // closed once it is unregistered
	// tokenHash is the token this connection handed the machine: its
	// `rotated` confirms it.
	tokenHash string
	// ready: welcomed, Deliver may send to it (under g.mu).
	ready bool

	mu sync.Mutex
	// directives are the ones this connection heartbeats.
	directives map[string]*attached
}

// attached is a directive a connection carries.
type attached struct {
	token    []byte
	progress string
	// cancelSent: the machine was told to stop it.
	cancelSent bool
}

// messageLimit is what a machine may send, per second and in a burst, past
// which the connection ends: four progresses a second per directive (agent
// connect sends one per 250 ms at most), its results, and room to spare.
func messageLimit(maxDirectives int) (rate.Limit, int) {
	perSecond := 4*maxDirectives + 10
	return rate.Limit(perSecond), 4 * perSecond
}

// ServeConnect is /machines/connect: a machine's WebSocket, authenticated by
// its token (Authorization: Bearer). A token replaced for good revokes its
// machine: a copy of it exists.
func (g *Gateway) ServeConnect(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		http.Error(w, "machine token required", http.StatusUnauthorized)
		return
	}
	presented := machine.HashToken(token)
	m, use, err := g.Store.MachineByToken(r.Context(), presented)
	if err != nil {
		log.Printf("machines: token lookup: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	switch use {
	case store.TokenUnknown:
		http.Error(w, "unknown machine token", http.StatusUnauthorized)
		return
	case store.TokenRetired:
		if m.RevokedAt == nil {
			log.Printf("machines: machine %s (%s) presented a token it replaced: revoked", m.ID, m.Name)
			if err := g.revoke(g.ctx, *m, ReasonTokenReused); err != nil {
				log.Printf("machines: revoke %s: %v", m.ID, err)
			}
		} else {
			log.Printf("machines: revoked machine %s (%s) tried to connect", m.ID, m.Name)
		}
		http.Error(w, "machine token refused: the machine is revoked", http.StatusUnauthorized)
		return
	}
	addr := ""
	if g.ClientAddr != nil {
		addr = g.ClientAddr(r)
	}
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return // Accept answered
	}
	ws.SetReadLimit(machine.MaxMessageBytes)
	ctx, cancel := context.WithCancel(g.ctx)
	c := &conn{g: g, id: "c-" + uuid.NewString(), m: *m, ws: ws, ctx: ctx, cancel: cancel, done: make(chan struct{}),
		directives: map[string]*attached{}}
	c.serve(presented, addr)
}

func (c *conn) serve(presented, addr string) {
	defer c.ws.CloseNow()
	defer close(c.done)
	defer c.cancel()

	hello, err := c.readHello()
	if err != nil {
		log.Printf("machines: machine %s: %v", c.m.ID, err)
		c.write(machine.Message{Type: machine.TypeError, Code: machine.CodeBadMessage, Text: err.Error()})
		c.closeWith(machine.ClosePolicy, "bad hello")
		return
	}
	if hello.Protocol < machine.MinProtocol {
		c.write(machine.Message{Type: machine.TypeError, Code: machine.CodeProtocol,
			Text: fmt.Sprintf("protocole %d trop ancien (au moins %d) : mets à jour agent", hello.Protocol, machine.MinProtocol)})
		c.closeWith(machine.CloseProtocol, "protocol too old")
		return
	}
	if !c.g.register(c) {
		return
	}
	defer c.g.unregister(c)

	// Rotation, first step: the machine gets its next token; the one it
	// presented stays valid until it confirms (`rotated`).
	next := machine.NewMachineToken()
	if err := c.g.Store.RotateMachineToken(c.ctx, c.m.ID, presented, machine.HashToken(next)); err != nil {
		c.refuseOrRetry("rotate the token", err)
		return
	}
	c.tokenHash = machine.HashToken(next)
	info := store.MachineInfo{OS: hello.OS, Capabilities: hello.Capabilities, MaxDirectives: hello.MaxDirectives, AgentVersion: hello.AgentVersion}
	prevAddr, err := c.g.Store.MachineConnected(c.ctx, c.m.ID, c.g.id, addr, info)
	if err != nil {
		c.refuseOrRetry("record the connection", err)
		return
	}
	log.Printf("machines: machine %s (%s) connected from %q, protocol %d, %v, up to %d at a time",
		c.m.ID, c.m.Name, addr, hello.Protocol, hello.Capabilities, hello.MaxDirectives)
	if prevAddr != "" && addr != "" && prevAddr != addr {
		c.g.alert(c.ctx, c.m.UserID, fmt.Sprintf("Machine « %s » connectée depuis une nouvelle adresse : %s (avant : %s). Si ce n'est pas toi, révoque-la.",
			c.m.Name, addr, prevAddr))
	}
	c.write(machine.Message{Type: machine.TypeWelcome, Protocol: machine.Protocol, MachineID: c.m.ID, Name: c.m.Name})
	c.write(machine.Message{Type: machine.TypeRotate, Token: next})
	// From here on Deliver may send to it: never before its welcome. What
	// was handed over until now is unsent, and reconcile sends it.
	c.g.mu.Lock()
	c.ready = true
	c.g.mu.Unlock()
	c.reconcile(hello)

	go c.heartbeats()
	c.readLoop(hello.MaxDirectives)
}

// refuseOrRetry ends a connection the database could not admit: for good
// (4001: the machine stops) only when its token is refused or it is
// revoked; on any other error (the database away), with 1011, and the
// machine comes back after its backoff.
func (c *conn) refuseOrRetry(what string, err error) {
	log.Printf("machines: %s of %s: %v", what, c.m.ID, err)
	if errors.Is(err, store.ErrTokenRefused) || errors.Is(err, store.ErrMachineNotFound) {
		c.write(machine.Message{Type: machine.TypeError, Code: machine.CodeRevoked, Text: "machine token refused"})
		c.closeWith(machine.CloseRevoked, "token refused")
		return
	}
	c.closeWith(int(websocket.StatusInternalError), "try again later")
}

func (c *conn) readHello() (machine.Message, error) {
	ctx, cancel := context.WithTimeout(c.ctx, helloTimeout)
	defer cancel()
	var hello machine.Message
	if err := wsjson.Read(ctx, c.ws, &hello); err != nil {
		return hello, fmt.Errorf("read hello: %w", err)
	}
	if hello.Type != machine.TypeHello {
		return hello, fmt.Errorf("%w: %q before hello", machine.ErrBadMessage, hello.Type)
	}
	return hello, machine.CheckFromMachine(&hello)
}

// register makes c its machine's connection. One machine, one connection:
// an older one that still answers its ping means two copies of the machine's
// token are in use, and both are cut. One that does not answer is stale (a
// network that dropped without a word) and gives way.
func (g *Gateway) register(c *conn) bool {
	g.mu.Lock()
	old := g.conns[c.m.ID]
	if old == nil {
		g.conns[c.m.ID] = c
		g.mu.Unlock()
		return true
	}
	g.mu.Unlock()
	pctx, cancel := context.WithTimeout(c.ctx, g.pingTimeout())
	err := old.ws.Ping(pctx)
	cancel()
	if err == nil {
		log.Printf("machines: two connections of machine %s (%s) at once: both cut", c.m.ID, c.m.Name)
		c.write(machine.Message{Type: machine.TypeError, Code: machine.CodeDuplicate, Text: "another connection uses this machine's token"})
		old.closeWith(machine.CloseDuplicate, "duplicate connection")
		c.closeWith(machine.CloseDuplicate, "duplicate connection")
		if g.firstDuplicate(c.m.ID, time.Now()) {
			g.alert(c.ctx, c.m.UserID, fmt.Sprintf("Deux connexions simultanées de la machine « %s » : coupées toutes les deux. "+
				"Si une copie de son jeton circule, révoque-la dans « Mes machines ».", c.m.Name))
		}
		return false
	}
	old.ws.CloseNow()
	select {
	case <-old.done:
	case <-c.ctx.Done():
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if cur := g.conns[c.m.ID]; cur != nil {
		return false
	}
	g.conns[c.m.ID] = c
	return true
}

// duplicateAlertEvery: two copies of a token keep cutting each other; their
// owner hears of it once an hour, not at every reconnection.
const duplicateAlertEvery = time.Hour

// firstDuplicate reports whether the duplicate connections of a machine
// are the first in duplicateAlertEvery, and records them.
func (g *Gateway) firstDuplicate(machineID string, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if last, ok := g.dupAlerts[machineID]; ok && now.Sub(last) < duplicateAlertEvery {
		return false
	}
	g.dupAlerts[machineID] = now
	for id, t := range g.dupAlerts {
		if now.Sub(t) >= duplicateAlertEvery {
			delete(g.dupAlerts, id)
		}
	}
	return true
}

func (g *Gateway) unregister(c *conn) {
	g.mu.Lock()
	if g.conns[c.m.ID] == c {
		delete(g.conns, c.m.ID)
	}
	g.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.Store.MachineDisconnected(ctx, c.m.ID, g.id); err != nil {
		log.Printf("machines: record the disconnection of %s: %v", c.m.ID, err)
	}
	log.Printf("machines: machine %s (%s) disconnected", c.m.ID, c.m.Name)
}

// reconcilePlan is what a hello makes of a machine's directives.
type reconcilePlan struct {
	attach []store.Directive // known to the machine and open here: carried on
	send   []store.Directive // never sent: sent now
	lost   []store.Directive // sent by an earlier connection, unknown to the machine
	cancel []string          // running on the machine, over here (or never its own)
	ack    []string          // finished on the machine, over here: its result is dropped
}

// reconcile matches what a machine says it holds with what the database
// holds open for it. Only a directive the database gives this machine and
// holds open is carried on: a hello brings nothing back to life.
func reconcile(open []store.Directive, running, finished []string, connID string) reconcilePlan {
	var p reconcilePlan
	listed := map[string]bool{}
	for _, id := range running {
		listed[id] = true
	}
	for _, id := range finished {
		listed[id] = true
	}
	isOpen := map[string]bool{}
	for _, d := range open {
		if d.State != store.DirectiveRunning {
			continue // reserved: RunOnMachine hands it over
		}
		isOpen[d.ID] = true
		switch {
		case listed[d.ID]:
			p.attach = append(p.attach, d)
		case d.SentConn == "":
			p.send = append(p.send, d)
		case d.SentConn != connID:
			p.lost = append(p.lost, d)
		}
	}
	for _, id := range running {
		if !isOpen[id] {
			p.cancel = append(p.cancel, id)
		}
	}
	for _, id := range finished {
		if !isOpen[id] {
			p.ack = append(p.ack, id)
		}
	}
	return p
}

func (c *conn) reconcile(hello machine.Message) {
	open, err := c.g.Store.OpenDirectives(c.ctx, c.m.ID)
	if err != nil {
		log.Printf("machines: directives of %s: %v", c.m.ID, err)
		c.ws.CloseNow()
		return
	}
	p := reconcile(open, hello.Running, hello.Finished, c.id)
	for _, d := range p.attach {
		c.attach(d)
	}
	for _, id := range p.cancel {
		c.write(machine.Message{Type: machine.TypeCancel, ID: id, Text: "closed"})
	}
	for _, id := range p.ack {
		c.write(machine.Message{Type: machine.TypeAck, ID: id})
	}
	for _, d := range p.send {
		c.send(d)
	}
	for _, d := range p.lost {
		c.lose(d)
	}
	if len(p.attach)+len(p.send)+len(p.lost)+len(p.cancel)+len(p.ack) > 0 {
		log.Printf("machines: machine %s back: %d carried on, %d sent, %d lost, %d cancelled, %d dropped",
			c.m.ID, len(p.attach), len(p.send), len(p.lost), len(p.cancel), len(p.ack))
	}
}

// send hands a running directive to the machine, once: whoever marks it
// sent first sends it.
func (c *conn) send(d store.Directive) {
	sent, err := c.g.Store.MarkDirectiveSent(c.ctx, d.ID, c.id)
	if err != nil {
		log.Printf("machines: mark directive %s sent: %v", d.ID, err)
		return
	}
	if !sent {
		return
	}
	c.attach(d)
	deadline := d.Deadline
	c.write(machine.Message{Type: machine.TypeDirective, ID: d.ID, Kind: d.Kind, Input: d.Input, Deadline: &deadline})
}

// lose ends a directive the machine no longer knows: it restarted between
// receiving it and saying so. At most once: never sent again.
func (c *conn) lose(d store.Directive) {
	closed, err := c.g.Store.CloseDirective(c.ctx, d.ID, store.DirectiveLost, "unknown to its machine")
	if err != nil || !closed {
		return
	}
	cerr := temporal.NewNonRetryableApplicationError(
		fmt.Sprintf("machine %q lost the directive (it restarted during it); nothing was done twice", c.m.Name), machine.ErrTypeLost, nil)
	if err := c.g.complete(c.ctx, completeTries, d.TaskToken, nil, cerr); err != nil && !isNotFound(err) {
		log.Printf("machines: end lost directive %s: %v", d.ID, err)
	}
}

func (c *conn) attach(d store.Directive) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.directives[d.ID] == nil {
		c.directives[d.ID] = &attached{token: d.TaskToken}
	}
}

// detach stops carrying a directive; false when it was not carried.
func (c *conn) detach(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.directives[id]
	delete(c.directives, id)
	return ok
}

// write sends a message; a failure ends the connection.
func (c *conn) write(m machine.Message) {
	ctx, cancel := context.WithTimeout(c.ctx, writeTimeout)
	defer cancel()
	if err := wsjson.Write(ctx, c.ws, m); err != nil {
		if c.ctx.Err() == nil {
			log.Printf("machines: write to %s: %v", c.m.ID, err)
		}
		c.ws.CloseNow()
	}
}

func (c *conn) closeWith(code int, reason string) {
	c.ws.Close(websocket.StatusCode(code), reason)
}

// readLoop reads the machine's messages until the connection ends. Results
// are completed apart: the reads go on meanwhile, pongs included.
func (c *conn) readLoop(maxDirectives int) {
	limit := rate.NewLimiter(messageLimit(maxDirectives))
	for {
		var m machine.Message
		if err := wsjson.Read(c.ctx, c.ws, &m); err != nil {
			if s := websocket.CloseStatus(err); s != websocket.StatusNormalClosure && s != websocket.StatusGoingAway && c.ctx.Err() == nil {
				log.Printf("machines: read from %s: %v", c.m.ID, err)
			}
			return
		}
		if !limit.Allow() {
			log.Printf("machines: machine %s sends too much: disconnected", c.m.ID)
			c.closeWith(machine.ClosePolicy, "too many messages")
			return
		}
		if err := machine.CheckFromMachine(&m); err != nil {
			log.Printf("machines: machine %s: %v", c.m.ID, err)
			c.write(machine.Message{Type: machine.TypeError, Code: machine.CodeBadMessage, Text: err.Error()})
			c.closeWith(machine.ClosePolicy, "bad message")
			return
		}
		switch m.Type {
		case machine.TypeRotated:
			if err := c.g.Store.ConfirmMachineToken(c.ctx, c.m.ID, c.tokenHash); err != nil {
				log.Printf("machines: confirm the token of %s: %v", c.m.ID, err)
			}
		case machine.TypeProgress:
			c.mu.Lock()
			if a := c.directives[m.ID]; a != nil {
				a.progress = m.Text
			}
			c.mu.Unlock()
		case machine.TypeResult:
			if c.g.claim(m.ID) {
				go c.result(m)
			}
		default:
			c.write(machine.Message{Type: machine.TypeError, Code: machine.CodeBadMessage, Text: "unexpected " + m.Type})
			c.closeWith(machine.ClosePolicy, "bad message")
			return
		}
	}
}

// heartbeats pings the machine every HeartbeatEvery and, while it answers,
// records a heartbeat for each directive it carries, with the machine's
// last progress. A machine that does not answer is disconnected: its
// directives get no heartbeat, and time out unless it comes back.
func (c *conn) heartbeats() {
	t := time.NewTicker(c.g.heartbeatEvery())
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		}
		pctx, cancel := context.WithTimeout(c.ctx, c.g.pingTimeout())
		err := c.ws.Ping(pctx)
		cancel()
		if err != nil {
			if c.ctx.Err() == nil {
				log.Printf("machines: machine %s does not answer its ping: disconnected", c.m.ID)
			}
			c.ws.CloseNow()
			return
		}
		if err := c.g.Store.TouchMachines(c.ctx, []string{c.m.ID}); err != nil {
			log.Printf("machines: touch %s: %v", c.m.ID, err)
		}
		type beat struct {
			id       string
			token    []byte
			progress string
		}
		var beats []beat
		c.mu.Lock()
		for id, a := range c.directives {
			beats = append(beats, beat{id, a.token, a.progress})
		}
		c.mu.Unlock()
		for _, b := range beats {
			if c.g.completing(b.id) {
				continue // its result is being completed: no heartbeat may close it as gone
			}
			c.heartbeat(b.id, b.token, b.progress)
		}
	}
}

// heartbeat records one directive's heartbeat. Its answer says whether the
// workflow cancelled the activity (the machine is told to stop; heartbeats
// go on until it answers) or whether the activity is gone (the machine is
// told to stop, the directive closed).
func (c *conn) heartbeat(id string, token []byte, progress string) {
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	err := c.g.Temporal.RecordActivityHeartbeat(ctx, token, machine.Heartbeat{Progress: progress})
	cancel()
	switch {
	case err == nil:
	case temporal.IsCanceledError(err):
		c.mu.Lock()
		a := c.directives[id]
		first := a != nil && !a.cancelSent
		if first {
			a.cancelSent = true
		}
		c.mu.Unlock()
		if first {
			log.Printf("machines: directive %s cancelled by its workflow", id)
			c.write(machine.Message{Type: machine.TypeCancel, ID: id, Text: "cancel requested"})
		}
	case isNotFound(err):
		if !c.detach(id) {
			return
		}
		log.Printf("machines: directive %s: its activity is gone", id)
		c.write(machine.Message{Type: machine.TypeCancel, ID: id, Text: "gone"})
		if _, err := c.g.Store.CloseDirective(c.ctx, id, store.DirectiveGone, "its activity no longer exists"); err != nil {
			log.Printf("machines: close directive %s: %v", id, err)
		}
	default:
		if c.ctx.Err() == nil {
			log.Printf("machines: heartbeat of directive %s: %v", id, err)
		}
	}
}

// completion is what a machine's result makes of its activity: a result, or
// an error a workflow can read.
func completion(m machine.Message) (machine.Result, error) {
	res := machine.Result{Output: m.Output, Progress: m.Text}
	switch m.Status {
	case machine.StatusOK:
		return res, nil
	case machine.StatusCanceled:
		return res, temporal.NewCanceledError(res)
	case machine.StatusStopping:
		return res, temporal.NewNonRetryableApplicationError("the machine stopped (agent connect ended) during the directive",
			machine.ErrTypeStopping, nil, res)
	default:
		msg := m.Error
		if msg == "" {
			msg = "the directive failed on the machine"
		}
		return res, temporal.NewNonRetryableApplicationError(msg, machine.ErrTypeFailed, nil, res)
	}
}

// closedState is the state a result closes its directive in.
func closedState(status string) string {
	switch status {
	case machine.StatusOK:
		return store.DirectiveCompleted
	case machine.StatusCanceled:
		return store.DirectiveCanceled
	case machine.StatusStopping:
		return store.DirectiveStopping
	}
	return store.DirectiveFailed
}

// result completes a directive's activity with the machine's result, closes
// the directive, then acks: the machine keeps the result until then. A
// result for a directive that is over is dropped (and acked). A completion
// that fails (Temporal away) leaves the result in the database, unacked, the
// directive carried on: the sweep completes it, then acks. The connection
// stays: closing it would only rotate the token at every reconnection while
// Temporal is away. A database error closes it (1011): the machine sends the
// result again when it is back.
func (c *conn) result(m machine.Message) {
	defer c.g.release(m.ID)
	// The gateway's context, not the connection's: a result that came in is
	// handled even if its machine leaves meanwhile (sent, then gone).
	ctx := c.g.ctx
	ack := func() { c.write(machine.Message{Type: machine.TypeAck, ID: m.ID}) }
	d, err := c.g.Store.GetDirective(ctx, m.ID)
	if err != nil {
		log.Printf("machines: result of %s: %v", m.ID, err)
		c.closeWith(int(websocket.StatusInternalError), "try again later")
		return // kept by the machine, sent again on its next connection
	}
	if d == nil || d.MachineID != c.m.ID || d.State != store.DirectiveRunning {
		log.Printf("machines: result of directive %s from %s dropped: not an open directive of this machine", m.ID, c.m.ID)
		c.detach(m.ID)
		ack()
		return
	}
	raw, _ := json.Marshal(m)
	if err := c.g.Store.SaveDirectiveResult(ctx, m.ID, raw); err != nil {
		if errors.Is(err, store.ErrDirectiveClosed) {
			c.detach(m.ID)
			ack()
		} else {
			log.Printf("machines: keep the result of %s: %v", m.ID, err)
			c.closeWith(int(websocket.StatusInternalError), "try again later")
		}
		return
	}
	if err := c.g.finish(ctx, completeTries, *d, m); err != nil {
		log.Printf("machines: complete directive %s: %v; the sweep tries again", m.ID, err)
		return
	}
	c.detach(m.ID)
	ack()
}

// finish completes a running directive's activity with its machine's result
// m, and closes the directive. An activity already gone (NotFound) closes it
// as gone: the result is dropped. Any other failure leaves it open, its
// result kept, for the sweep to try again.
func (g *Gateway) finish(ctx context.Context, tries int, d store.Directive, m machine.Message) error {
	res, cerr := completion(m)
	var result any
	if cerr == nil {
		result = res
	}
	err := g.complete(ctx, tries, d.TaskToken, result, cerr)
	if err != nil && isInvalidArgument(err) && m.Status == machine.StatusCanceled {
		// Cancelled on the machine without the workflow asking: a failure.
		err = g.complete(ctx, tries, d.TaskToken, nil, temporal.NewNonRetryableApplicationError(
			"the directive was cancelled on the machine", machine.ErrTypeFailed, nil, res))
	}
	state := closedState(m.Status)
	switch {
	case err == nil:
	case isNotFound(err):
		state = store.DirectiveGone
	default:
		return err
	}
	if _, err := g.Store.CloseDirective(ctx, d.ID, state, m.Error); err != nil {
		// Completed: a close that failed leaves the row open until its
		// deadline; the sweep's completion of it then finds it gone.
		log.Printf("machines: close directive %s: %v", d.ID, err)
	}
	log.Printf("machines: directive %s done on %s: %s", d.ID, d.MachineID, state)
	return nil
}
