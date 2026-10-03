package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

// mcpMaxResumes bounds how many times a request's event stream is resumed
// after the server closed it before the response.
const mcpMaxResumes = 5

// streamableConn is a session over the Streamable HTTP transport (MCP
// 2025-03-26 and later): every message is a POST to the server's one URL,
// answered with JSON or with an event stream that carries the response.
type streamableConn struct {
	http   *http.Client
	url    string
	apiKey string
	ids    *atomic.Int64

	// Set during the handshake, before the connection is shared; read-only
	// after.
	session string // Mcp-Session-Id the server gave at initialize, echoed after
	version string // negotiated protocol version, sent after initialize

	expired atomic.Bool // the server answered 404: nothing left to end
}

func (c *streamableConn) alive() bool { return true } // an expired session answers 404

func (c *streamableConn) setVersion(v string) { c.version = v }

func (c *streamableConn) call(ctx context.Context, method string, params any, limit int) (json.RawMessage, error) {
	id := c.ids.Add(1)
	resp, err := c.post(ctx, jsonRPCRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params})
	if err != nil {
		return nil, err
	}
	// Closed, not drained: a server may hold an event stream open after
	// the response.
	defer resp.Body.Close()
	if err := c.check(resp); err != nil {
		return nil, err
	}
	if method == "initialize" {
		if err := c.takeSession(resp); err != nil {
			return nil, err
		}
	}

	switch mt := mediaType(resp); mt {
	case "application/json":
		body, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
		if err != nil {
			return nil, fmt.Errorf("mcp read response: %w", err)
		}
		if len(body) > limit {
			return nil, errTooLarge
		}
		msg, ok := parseMessage(body)
		if !ok || !(msg.answers(id) || msg.unattributedError()) {
			return nil, fmt.Errorf("mcp: not a response to request %d: %.200s", id, body)
		}
		return msg.outcome()
	case "text/event-stream":
		return c.readStream(ctx, resp.Body, id, limit)
	default:
		if resp.StatusCode == http.StatusAccepted {
			return nil, errors.New("mcp: the server accepted the request without answering it")
		}
		return nil, fmt.Errorf("mcp: unexpected response type %q", mt)
	}
}

// readStream reads the event stream a POST was answered with until the
// response to request id. The server may close it early, having given each
// event an ID: the stream is then resumed with a GET from the last one.
func (c *streamableConn) readStream(ctx context.Context, body io.Reader, id int64, limit int) (json.RawMessage, error) {
	sse := newSSEReader(body, limit)
	res, done, err := c.untilResponse(ctx, sse, id)
	for resumes := 0; !done; resumes++ {
		if sse.lastID == "" || resumes == mcpMaxResumes {
			return nil, fmt.Errorf("mcp: the event stream ended before the response: %w", err)
		}
		if err := sleepCtx(ctx, min(sse.retry, 5*time.Second)); err != nil {
			return nil, err
		}
		resp, rerr := c.resume(ctx, sse.lastID)
		if rerr != nil {
			return nil, rerr
		}
		next := newSSEReader(resp.Body, limit)
		next.lastID, next.retry = sse.lastID, sse.retry
		res, done, err = c.untilResponse(ctx, next, id)
		resp.Body.Close()
		sse = next
	}
	return res, err
}

// untilResponse reads events until the response to request id (done), or
// the end of the stream (not done, with the reason). A request from the
// server is answered; a notification is skipped.
func (c *streamableConn) untilResponse(ctx context.Context, sse *sseReader, id int64) (json.RawMessage, bool, error) {
	for {
		ev, err := sse.next()
		if err != nil {
			if ctx.Err() != nil {
				return nil, true, ctx.Err()
			}
			if errors.Is(err, errTooLarge) {
				return nil, true, err
			}
			return nil, false, err
		}
		if ev.event != "" && ev.event != "message" {
			continue
		}
		msg, ok := parseMessage([]byte(ev.data))
		switch {
		case !ok:
		case msg.answers(id):
			res, err := msg.outcome()
			return res, true, err
		case msg.isRequest():
			if err := c.send(ctx, replyTo(msg)); err != nil {
				log.Printf("Warning: mcp: reply to the server's %q request: %v", msg.Method, err)
			}
		}
	}
}

// resume reopens the stream of a request after its last event.
func (c *streamableConn) resume(ctx context.Context, lastID string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Last-Event-ID", lastID)
	c.headers(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mcp resume stream: %w", err)
	}
	if err := c.check(resp); err != nil {
		resp.Body.Close()
		// The request was accepted: sending it again in a new session
		// could run it twice.
		if errors.Is(err, errSessionExpired) {
			err = errors.New("mcp resume stream: session expired")
		}
		return nil, err
	}
	if mediaType(resp) != "text/event-stream" {
		resp.Body.Close()
		return nil, errors.New("mcp resume stream: not an event stream")
	}
	return resp, nil
}

func (c *streamableConn) notify(ctx context.Context, method string, params any) error {
	return c.send(ctx, jsonRPCNotification{JSONRPC: "2.0", Method: method, Params: params})
}

// send posts a message that has no response: a notification, or the reply
// to a server's request. The server accepts it with 202.
func (c *streamableConn) send(ctx context.Context, msg any) error {
	resp, err := c.post(ctx, msg)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return c.check(resp)
}

func (c *streamableConn) post(ctx context.Context, msg any) (*http.Response, error) {
	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	c.headers(req)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mcp http call: %w", err)
	}
	return resp, nil
}

func (c *streamableConn) headers(req *http.Request) {
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	if c.session != "" {
		req.Header.Set("Mcp-Session-Id", c.session)
	}
	if c.version != "" {
		req.Header.Set("MCP-Protocol-Version", c.version)
	}
}

// check turns an error status into an error. 404 on a session means the
// server dropped it (expired, or restarted) without processing the request.
func (c *streamableConn) check(resp *http.Response) error {
	if resp.StatusCode/100 == 2 {
		return nil
	}
	if resp.StatusCode == http.StatusNotFound && c.session != "" {
		c.expired.Store(true)
		return errSessionExpired
	}
	return statusError(resp)
}

// takeSession keeps the session ID the server assigned at initialize, if
// any. The spec allows visible ASCII only: anything else is refused rather
// than echoed in a header.
func (c *streamableConn) takeSession(resp *http.Response) error {
	id := resp.Header.Get("Mcp-Session-Id")
	for i := 0; i < len(id); i++ {
		if id[i] < 0x21 || id[i] > 0x7e {
			return errors.New("mcp: invalid session ID")
		}
	}
	c.session = id
	return nil
}

// close ends the session on the server, as the spec asks of a client that
// no longer needs it. A server that does not allow it answers 405: no harm.
func (c *streamableConn) close() {
	if c.session == "" || c.expired.Load() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.url, nil)
	if err != nil {
		return
	}
	c.headers(req)
	if resp, err := c.http.Do(req); err == nil {
		resp.Body.Close()
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
