package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/coder/websocket"
	"go.temporal.io/sdk/temporal"

	"github.com/victor/temporal-agent/machine"
	"github.com/victor/temporal-agent/store"
)

// The model on a machine (docs/design/machine-llm.md): a worker's
// CallLLMOnMachine hands the gateway an llm directive with its request,
// which goes to the machine at once on its WebSocket, or nowhere. The
// request is never stored: a directive the gateway cannot send now is
// closed, and the workflow tries again (§5).

// ErrMachineUnreachable is an llm directive that could not be handed to its
// machine now: not connected, or the write failed. The directive is closed;
// nothing ran.
var ErrMachineUnreachable = errors.New("machine unreachable")

// llmChunk is how much of a request one write gives the WebSocket: a ping
// goes out between two frames, and a large request on a slow link does not
// hold the connection's pings up (§5).
const llmChunk = 64 << 10

// DeliverLLM hands an llm directive to its machine with its request, at once
// (CallLLMOnMachine, through /internal/machines/directives, or in process in
// dev). A machine that is not connected, or a write that fails, closes the
// directive: ErrMachineUnreachable. The request is kept only while it is
// written.
func (g *Gateway) DeliverLLM(ctx context.Context, directiveID string, request json.RawMessage) error {
	d, err := g.Store.GetDirective(ctx, directiveID)
	switch {
	case err != nil:
		return err
	case d == nil:
		return ErrUnknownDirective
	case d.State != store.DirectiveRunning || d.Kind != machine.KindLLM:
		return ErrDirectiveClosed
	}
	c := g.conn(d.MachineID)
	if c == nil || !g.isReady(c) {
		g.unreachable(*d, "not connected")
		return ErrMachineUnreachable
	}
	if err := c.sendLLM(*d, request); err != nil {
		g.unreachable(*d, err.Error())
		return ErrMachineUnreachable
	}
	return nil
}

// unreachable closes an llm directive that could not be sent: its activity
// is still at its handoff, and says so to its workflow itself.
func (g *Gateway) unreachable(d store.Directive, why string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := g.Store.CloseDirective(ctx, d.ID, store.DirectiveFailed, "machine unreachable: "+why); err != nil {
		log.Printf("machines: close directive %s: %v", d.ID, err)
	}
	log.Printf("machines: call to the model %s not handed to machine %s: %s", d.ID, d.MachineID, why)
}

// sendLLM writes an llm directive and its request to the machine, once
// (sent_conn), in chunks of llmChunk within machine.LLMWriteTimeout. A write
// cut halfway leaves the stream unreadable: the connection is closed, and
// the machine comes back.
func (c *conn) sendLLM(d store.Directive, request json.RawMessage) error {
	sent, err := c.g.Store.MarkDirectiveSent(c.ctx, d.ID, c.id)
	switch {
	case err != nil:
		return fmt.Errorf("mark it sent: %w", err)
	case !sent:
		return errors.New("sent already, or over")
	}
	deadline := d.Deadline
	msg, err := json.Marshal(machine.Message{Type: machine.TypeDirective, ID: d.ID, Kind: d.Kind, Input: request, Deadline: &deadline})
	if err != nil {
		return err
	}
	c.attach(d)
	ctx, cancel := context.WithTimeout(c.ctx, machine.LLMWriteTimeout)
	defer cancel()
	if err := writeChunked(ctx, c.ws, msg); err != nil {
		c.detach(d.ID)
		c.ws.CloseNow()
		return fmt.Errorf("write: %w", err)
	}
	// Its first heartbeat now: its activity started before the request was
	// built and written.
	c.mu.Lock()
	a := c.directives[d.ID]
	c.mu.Unlock()
	if a != nil {
		go c.heartbeat(d.ID, a.token, "")
	}
	return nil
}

// writeChunked writes msg as one text message, llmChunk bytes at a time.
func writeChunked(ctx context.Context, ws *websocket.Conn, msg []byte) error {
	w, err := ws.Writer(ctx, websocket.MessageText)
	if err != nil {
		return err
	}
	for len(msg) > 0 {
		n := min(len(msg), llmChunk)
		if _, err := w.Write(msg[:n]); err != nil {
			w.Close()
			return err
		}
		msg = msg[n:]
	}
	return w.Close()
}

// checkLLMResult is what the gateway keeps of a machine's result for an llm
// directive (docs/design/machine-llm.md §9): an answer that breaks the rules,
// or a failure it cannot read, becomes a failure the gateway writes in its
// place, completed like any other. The machine has the agent's authority
// over the turn's tools, not over what a turn can carry.
func checkLLMResult(m machine.Message) machine.Message {
	refuse := func(why string) machine.Message {
		out, _ := json.Marshal(machine.LLMFailure{Type: machine.LLMFailBadResult})
		return machine.Message{Type: machine.TypeResult, ID: m.ID, Status: machine.StatusError, Output: out,
			Error: machine.Cut("the machine's answer was refused: "+why, machine.MaxErrorBytes)}
	}
	switch m.Status {
	case machine.StatusOK:
		if _, err := machine.CheckLLMOutput(m.Output); err != nil {
			return refuse(err.Error())
		}
	case machine.StatusError:
		if len(m.Output) > machine.MaxMessageBytes {
			return refuse("failure too large")
		}
		var f machine.LLMFailure
		if len(m.Output) > 0 && json.Unmarshal(m.Output, &f) != nil {
			return refuse("failure unreadable")
		}
	default:
		if len(m.Output) > machine.MaxMessageBytes {
			m.Output = nil
		}
	}
	return m
}

// llmCompletion is what a machine's result makes of an llm directive's
// activity: the answer, with what the prompt held of the memory (kept in the
// directive's input: the sweep completes from the database alone), or the
// error the workflow reads to retry, fall back or stop.
func llmCompletion(d store.Directive, m machine.Message) (any, error) {
	switch m.Status {
	case machine.StatusOK:
		var res machine.LLMResult
		if err := json.Unmarshal(m.Output, &res.ChatResponse); err != nil {
			return nil, temporal.NewNonRetryableApplicationError("the machine's answer is unreadable", machine.ErrTypeBadResult, nil)
		}
		var in machine.LLMInput
		if err := json.Unmarshal(d.Input, &in); err != nil {
			log.Printf("machines: input of directive %s unreadable: %v", d.ID, err)
			in.MemoryUnread = true
		}
		res.PromptMemory = in.PromptMemory
		return res, nil
	case machine.StatusCanceled:
		return nil, temporal.NewCanceledError()
	case machine.StatusStopping:
		return nil, temporal.NewNonRetryableApplicationError("the machine stopped (agent connect ended) during the call", machine.ErrTypeStopping, nil)
	case machine.StatusRefused:
		msg := m.Error
		if msg == "" {
			msg = "the machine refused the call"
		}
		return nil, temporal.NewNonRetryableApplicationError(msg, machine.ErrTypeRefused, nil)
	}
	msg := m.Error
	if msg == "" {
		msg = "the call to the model failed on the machine"
	}
	var f machine.LLMFailure
	json.Unmarshal(m.Output, &f)
	switch f.Type {
	case machine.LLMFailRetryAfter:
		return nil, temporal.NewApplicationErrorWithOptions(msg, machine.ErrTypeRetryAfter, temporal.ApplicationErrorOptions{
			NonRetryable: true, NextRetryDelay: f.RetryAfter(), Details: []any{f}})
	case machine.LLMFailContextTooLong:
		return nil, temporal.NewNonRetryableApplicationError(msg, machine.ErrTypeContextTooLong, nil, f)
	case machine.LLMFailPermanent, machine.LLMFailCredentials:
		return nil, temporal.NewNonRetryableApplicationError(msg, machine.ErrTypePermanentAPI, nil, f)
	case machine.LLMFailBadResult:
		return nil, temporal.NewNonRetryableApplicationError(msg, machine.ErrTypeBadResult, nil, f)
	}
	return nil, temporal.NewNonRetryableApplicationError(msg, machine.ErrTypeFailed, nil, f)
}

// readLimitFor is what a message of a connection may weigh once read: an
// llm directive's result, up to machine.MaxReadBytes (CheckLLMOutput bounds
// its answer); anything else, machine.MaxMessageBytes.
func (c *conn) readLimitFor(m machine.Message) int {
	if m.Type != machine.TypeResult {
		return machine.MaxMessageBytes
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if a := c.directives[m.ID]; a != nil && a.d.Kind == machine.KindLLM {
		return machine.MaxReadBytes
	}
	return machine.MaxMessageBytes
}
