package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// sseConn is a session over the HTTP+SSE transport of MCP 2024-11-05,
// deprecated since but still served: the client keeps a GET event stream
// open, the server names on it the endpoint to POST messages to, and sends
// its responses on the stream.
type sseConn struct {
	http     *http.Client
	apiKey   string
	endpoint string
	ids      *atomic.Int64
	cancel   context.CancelFunc // ends the stream

	mu      sync.Mutex
	pending map[int64]chan jsonRPCMessage
	done    chan struct{} // closed when the stream ends
	err     error         // why, set before done is closed
}

// dialSSE opens the event stream and waits, within ctx, for the endpoint.
// The stream itself outlives ctx: it is the session.
func dialSSE(ctx context.Context, httpc *http.Client, rawURL, apiKey string, ids *atomic.Int64) (*sseConn, error) {
	streamCtx, cancel := context.WithCancel(context.Background())
	stop := context.AfterFunc(ctx, cancel)
	fail := func(err error) (*sseConn, error) {
		stop()
		cancel()
		return nil, err
	}

	req, err := http.NewRequestWithContext(streamCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return fail(err)
	}
	req.Header.Set("Accept", "text/event-stream")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := httpc.Do(req)
	if err != nil {
		return fail(fmt.Errorf("mcp sse connect: %w", err))
	}
	if resp.StatusCode != http.StatusOK {
		err := statusError(resp)
		resp.Body.Close()
		return fail(fmt.Errorf("%w (an HTTP+SSE server streams events on GET; a Streamable HTTP server takes transport: http)", err))
	}
	if mt := mediaType(resp); mt != "text/event-stream" {
		resp.Body.Close()
		return fail(fmt.Errorf("mcp sse connect: %q is not an event stream (a Streamable HTTP server takes transport: http)", mt))
	}

	// The server names the endpoint in its first event; anything before is
	// skipped.
	sse := newSSEReader(resp.Body, mcpMaxMessage) // one stream for every response
	var endpoint string
	for endpoint == "" {
		ev, err := sse.next()
		if err != nil {
			resp.Body.Close()
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			return fail(fmt.Errorf("mcp sse: no endpoint event: %w", err))
		}
		if ev.event == "endpoint" {
			endpoint = strings.TrimSpace(ev.data)
		}
	}
	if !stop() { // ctx ended meanwhile, and with it the stream
		resp.Body.Close()
		cancel()
		return nil, ctx.Err()
	}
	target, err := sameOriginEndpoint(rawURL, endpoint)
	if err != nil {
		resp.Body.Close()
		cancel()
		return nil, err
	}

	c := &sseConn{
		http:     httpc,
		apiKey:   apiKey,
		endpoint: target,
		ids:      ids,
		cancel:   cancel,
		pending:  make(map[int64]chan jsonRPCMessage),
		done:     make(chan struct{}),
	}
	go c.read(streamCtx, sse, resp.Body)
	return c, nil
}

// sameOriginEndpoint resolves the endpoint the server named against the
// stream's URL. It must stay on the same origin: the API key goes with every
// message, and a server must not send it elsewhere.
func sameOriginEndpoint(base, endpoint string) (string, error) {
	b, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	e, err := b.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("mcp sse: invalid endpoint %q: %w", endpoint, err)
	}
	if e.Scheme != b.Scheme || e.Host != b.Host {
		return "", fmt.Errorf("mcp sse: endpoint %q is not on the server's origin", endpoint)
	}
	return e.String(), nil
}

// read hands each response to the request waiting for it and answers the
// server's requests, until the stream ends.
func (c *sseConn) read(ctx context.Context, sse *sseReader, body io.ReadCloser) {
	defer body.Close()
	var err error
	for {
		var ev sseEvent
		if ev, err = sse.next(); err != nil {
			break
		}
		if ev.event != "" && ev.event != "message" {
			continue
		}
		msg, ok := parseMessage([]byte(ev.data))
		switch {
		case !ok:
		case msg.isRequest():
			go func() {
				rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				if err := c.send(rctx, replyTo(msg)); err != nil {
					log.Printf("Warning: mcp: reply to the server's %q request: %v", msg.Method, err)
				}
			}()
		case msg.Method == "" && msg.hasID():
			c.deliver(msg)
		}
	}
	c.mu.Lock()
	c.err = fmt.Errorf("mcp sse: event stream closed: %w", err)
	close(c.done)
	c.mu.Unlock()
}

func (c *sseConn) deliver(msg jsonRPCMessage) {
	var id int64
	if json.Unmarshal(msg.ID, &id) != nil {
		return
	}
	c.mu.Lock()
	ch, ok := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	if ok {
		ch <- msg // buffered: never blocks the stream
	}
}

func (c *sseConn) alive() bool {
	select {
	case <-c.done:
		return false
	default:
		return true
	}
}

func (c *sseConn) setVersion(string) {} // this transport has no version header

func (c *sseConn) call(ctx context.Context, method string, params any, limit int) (json.RawMessage, error) {
	id := c.ids.Add(1)
	ch := make(chan jsonRPCMessage, 1)
	c.mu.Lock()
	if !c.alive() {
		c.mu.Unlock()
		return nil, c.err
	}
	c.pending[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	if err := c.send(ctx, jsonRPCRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params}); err != nil {
		return nil, err
	}
	select {
	case msg := <-ch:
		return outcomeWithin(msg, limit)
	case <-c.done:
		select {
		case msg := <-ch: // came in just before the end
			return outcomeWithin(msg, limit)
		default:
			return nil, c.err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// outcomeWithin is msg's outcome, if its result fits the request's limit.
// The stream carries every response, so it can only bound them all as one
// (mcpMaxMessage); a request that asked for less is held to it here.
func outcomeWithin(msg jsonRPCMessage, limit int) (json.RawMessage, error) {
	if len(msg.Result) > limit {
		return nil, errTooLarge
	}
	return msg.outcome()
}

func (c *sseConn) notify(ctx context.Context, method string, params any) error {
	return c.send(ctx, jsonRPCNotification{JSONRPC: "2.0", Method: method, Params: params})
}

// send posts a message to the endpoint; what it answers comes on the stream.
func (c *sseConn) send(ctx context.Context, msg any) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("mcp sse post: %w", err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode/100 == 2:
		return nil
	case resp.StatusCode == http.StatusNotFound:
		// The endpoint carries the session: the server forgot it.
		c.cancel()
		return errSessionExpired
	default:
		return statusError(resp)
	}
}

func (c *sseConn) close() { c.cancel() }
