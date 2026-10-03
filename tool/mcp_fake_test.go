package tool

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// fakeMCP is an MCP server for tests, as strict as the spec lets one be:
// Streamable HTTP by default (or HTTP+SSE with legacy), initialize before
// anything else, a session ID to echo, the negotiated version in a header.
type fakeMCP struct {
	srv *httptest.Server

	// Set before the first request.
	apiKey  string
	legacy  bool     // HTTP+SSE: GET /sse for the stream, POST /messages
	version string   // answered to initialize; "" = the version asked
	sse     bool     // answer requests with an event stream
	preface bool     // ... starting with a notification and a ping
	early   bool     // ... closed after a priming event, the response on resume
	linger  bool     // ... held open after the response
	pages   int      // > 0: tools/list answers this many tools a page
	gate    *barrier // non-nil: tools/list waits for every server sharing it
	idless  bool     // JSON errors carry no ID, as for a request not read
	// legacy only
	pings     int           // pings sent before each response
	stringIDs bool          // responses echo the request's ID as a string
	replyGate chan struct{} // non-nil: the client's replies wait for it

	down atomic.Bool // the connection is cut before any answer

	mu        sync.Mutex
	tools     []mcpToolInfo
	sessions  map[string]*fakeSession
	nextID    int
	requests  []fakeRequest
	replies   []string // the client's replies to the server's requests
	parked    map[string]string
	block     chan struct{} // non-nil: tools/call waits for it
	callError *jsonRPCError
}

type fakeSession struct {
	version     string
	initialized bool
	out         chan string // legacy: messages for the session's stream
}

type fakeRequest struct {
	Method, Session, Version, Auth string
}

func newFakeMCP(t *testing.T, configure func(*fakeMCP)) *fakeMCP {
	t.Helper()
	f := &fakeMCP{sessions: map[string]*fakeSession{}, parked: map[string]string{}}
	f.setTools("echo")
	if configure != nil {
		configure(f)
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeMCP) config(name string) MCPServerConfig {
	c := MCPServerConfig{Name: name, URL: f.srv.URL, APIKey: f.apiKey}
	if f.legacy {
		c.URL += "/sse"
		c.Transport = "sse"
	}
	return c
}

func (f *fakeMCP) setTools(names ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tools = nil
	for _, n := range names {
		f.tools = append(f.tools, mcpToolInfo{Name: n, Description: "tool " + n, InputSchema: json.RawMessage(`{"type":"object"}`)})
	}
}

// setToolInfos makes the server give these tools, as they are.
func (f *fakeMCP) setToolInfos(tools ...mcpToolInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tools = tools
}

// expire forgets every session, as a server does when they time out or
// when it restarts.
func (f *fakeMCP) expire() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for id, s := range f.sessions {
		if s.out != nil {
			close(s.out)
		}
		delete(f.sessions, id)
	}
}

func (f *fakeMCP) methods() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var m []string
	for _, r := range f.requests {
		m = append(m, r.Method)
	}
	return m
}

func (f *fakeMCP) count(method string) int {
	n := 0
	for _, m := range f.methods() {
		if m == method {
			n++
		}
	}
	return n
}

type fakeMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

func (f *fakeMCP) serve(w http.ResponseWriter, r *http.Request) {
	if f.down.Load() {
		panic(http.ErrAbortHandler)
	}
	if f.apiKey != "" && r.Header.Get("Authorization") != "Bearer "+f.apiKey {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch {
	case f.legacy && r.Method == http.MethodGet:
		f.serveLegacyStream(w, r)
	case f.legacy && r.Method == http.MethodPost:
		f.serveLegacyPost(w, r)
	case r.Method == http.MethodPost:
		f.servePost(w, r)
	case r.Method == http.MethodGet:
		f.serveResume(w, r)
	case r.Method == http.MethodDelete:
		f.mu.Lock()
		delete(f.sessions, r.Header.Get("Mcp-Session-Id"))
		f.mu.Unlock()
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (f *fakeMCP) read(r *http.Request, session string) (fakeMessage, bool) {
	var m fakeMessage
	body, _ := io.ReadAll(r.Body)
	if json.Unmarshal(body, &m) != nil {
		return m, false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	method := m.Method
	if method == "" {
		method = "(reply)"
		f.replies = append(f.replies, string(body))
	}
	f.requests = append(f.requests, fakeRequest{
		Method:  method,
		Session: session,
		Version: r.Header.Get("MCP-Protocol-Version"),
		Auth:    r.Header.Get("Authorization"),
	})
	return m, true
}

func (f *fakeMCP) servePost(w http.ResponseWriter, r *http.Request) {
	sid := r.Header.Get("Mcp-Session-Id")
	m, ok := f.read(r, sid)
	if !ok {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if m.Method == "initialize" {
		if sid != "" {
			http.Error(w, "initialize carries no session", http.StatusBadRequest)
			return
		}
		result, id := f.initialize(m, nil)
		w.Header().Set("Mcp-Session-Id", id)
		f.respond(w, r, m.ID, result, nil, false)
		return
	}

	f.mu.Lock()
	s := f.sessions[sid]
	f.mu.Unlock()
	switch {
	case sid == "":
		http.Error(w, "missing session", http.StatusBadRequest)
		return
	case s == nil:
		http.Error(w, "unknown session", http.StatusNotFound)
		return
	case r.Header.Get("MCP-Protocol-Version") != s.version:
		http.Error(w, "wrong protocol version header", http.StatusBadRequest)
		return
	}
	if m.Method == "" || len(m.ID) == 0 { // a notification, or a reply
		f.notified(s, m)
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if !f.isInitialized(s) {
		http.Error(w, "not initialized", http.StatusBadRequest)
		return
	}
	result, rpcErr := f.answer(m)
	f.respond(w, r, m.ID, result, rpcErr, f.preface)
}

func (f *fakeMCP) initialize(m fakeMessage, out chan string) (any, string) {
	var p struct {
		ProtocolVersion string `json:"protocolVersion"`
	}
	json.Unmarshal(m.Params, &p)
	version := f.version
	if version == "" {
		version = p.ProtocolVersion
	}
	f.mu.Lock()
	f.nextID++
	id := "session-" + strconv.Itoa(f.nextID)
	f.sessions[id] = &fakeSession{version: version, out: out}
	f.mu.Unlock()
	return map[string]any{
		"protocolVersion": version,
		"capabilities":    map[string]any{"tools": map[string]any{}},
		"serverInfo":      map[string]any{"name": "fake", "version": "0"},
	}, id
}

func (f *fakeMCP) notified(s *fakeSession, m fakeMessage) {
	if m.Method == "notifications/initialized" {
		f.mu.Lock()
		s.initialized = true
		f.mu.Unlock()
	}
}

func (f *fakeMCP) isInitialized(s *fakeSession) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return s.initialized
}

func (f *fakeMCP) answer(m fakeMessage) (any, *jsonRPCError) {
	switch m.Method {
	case "tools/list":
		if f.gate != nil && !f.gate.arrive() {
			return nil, &jsonRPCError{Code: -32000, Message: "the other servers were never asked"}
		}
		var p struct {
			Cursor string `json:"cursor"`
		}
		json.Unmarshal(m.Params, &p)
		f.mu.Lock()
		tools := slices.Clone(f.tools)
		f.mu.Unlock()
		if f.pages == 0 {
			return mcpToolListResult{Tools: tools}, nil
		}
		start, _ := strconv.Atoi(p.Cursor)
		end := min(start+f.pages, len(tools))
		res := mcpToolListResult{Tools: tools[start:end]}
		if end < len(tools) {
			res.NextCursor = strconv.Itoa(end)
		}
		return res, nil
	case "tools/call":
		var p mcpCallToolParams
		json.Unmarshal(m.Params, &p)
		f.mu.Lock()
		block, callErr := f.block, f.callError
		f.mu.Unlock()
		if block != nil {
			<-block
		}
		if callErr != nil {
			return nil, callErr
		}
		if p.Name == "fail" {
			return mcpCallToolResult{IsError: true, Content: []mcpContentBlock{{Type: "text", Text: "it failed"}}}, nil
		}
		return mcpCallToolResult{Content: []mcpContentBlock{{Type: "text", Text: p.Name + " " + string(p.Arguments)}}}, nil
	}
	return nil, &jsonRPCError{Code: jsonRPCMethodNotFound, Message: "no such method"}
}

func encodeResponse(id json.RawMessage, result any, rpcErr *jsonRPCError) string {
	msg := map[string]any{"jsonrpc": "2.0", "id": id}
	if rpcErr != nil {
		msg["error"] = rpcErr
	} else {
		msg["result"] = result
	}
	b, _ := json.Marshal(msg)
	return string(b)
}

func (f *fakeMCP) respond(w http.ResponseWriter, r *http.Request, id json.RawMessage, result any, rpcErr *jsonRPCError, preface bool) {
	if rpcErr != nil && f.idless {
		id = json.RawMessage("null")
	}
	resp := encodeResponse(id, result, rpcErr)
	if !f.sse {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, resp)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	if preface {
		io.WriteString(w, ": a comment\n\n")
		io.WriteString(w, `data: {"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info","data":"hi"}}`+"\n\n")
		io.WriteString(w, `data: {"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error about another message"}}`+"\n\n")
		io.WriteString(w, `event: message`+"\n"+`data: {"jsonrpc":"2.0","id":"srv-1","method":"ping"}`+"\n\n")
	}
	if f.early {
		f.mu.Lock()
		f.parked["ev-1"] = resp
		f.mu.Unlock()
		io.WriteString(w, "id: ev-1\nretry: 1\ndata:\n\n")
		return // closed before the response
	}
	// The response, split over two data lines as the format allows.
	io.WriteString(w, "id: ev-9\ndata: "+resp[:1]+"\ndata: "+resp[1:]+"\n\n")
	if f.linger {
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}
}

// serveResume answers a GET resuming a stream after its last event.
func (f *fakeMCP) serveResume(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	s := f.sessions[r.Header.Get("Mcp-Session-Id")]
	resp, ok := f.parked[r.Header.Get("Last-Event-ID")]
	f.mu.Unlock()
	if s == nil || !ok {
		http.Error(w, "nothing to resume", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	io.WriteString(w, "id: ev-2\ndata: "+resp+"\n\n")
}

func (f *fakeMCP) serveLegacyStream(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/sse" {
		http.NotFound(w, r)
		return
	}
	out := make(chan string, 16)
	f.mu.Lock()
	f.nextID++
	sid := "legacy-" + strconv.Itoa(f.nextID)
	f.sessions[sid] = &fakeSession{out: out}
	f.mu.Unlock()

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "event: endpoint\ndata: /messages?sid=%s\n\n", sid)
	w.(http.Flusher).Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-out:
			if !ok {
				return // the session is gone: so is its stream
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", msg)
			w.(http.Flusher).Flush()
		}
	}
}

// push sends msg on a legacy session's stream, if the session still exists.
func (f *fakeMCP) push(sid, msg string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s := f.sessions[sid]; s != nil {
		s.out <- msg
	}
}

func (f *fakeMCP) serveLegacyPost(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("sid")
	m, ok := f.read(r, sid)
	f.mu.Lock()
	s := f.sessions[sid]
	f.mu.Unlock()
	if !ok || s == nil {
		http.Error(w, "unknown session", http.StatusNotFound)
		return
	}
	if m.Method == "" && f.replyGate != nil {
		<-f.replyGate
	}
	w.WriteHeader(http.StatusAccepted)
	id := m.ID
	if f.stringIDs {
		id = json.RawMessage(strconv.Quote(string(m.ID)))
	}
	switch {
	case m.Method == "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		json.Unmarshal(m.Params, &p)
		f.mu.Lock()
		s.version = p.ProtocolVersion
		f.mu.Unlock()
		f.push(sid, encodeResponse(id, map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{"tools": map[string]any{}},
			"serverInfo":      map[string]any{"name": "fake", "version": "0"},
		}, nil))
	case m.Method == "" || len(m.ID) == 0:
		f.notified(s, m)
	case !f.isInitialized(s):
		f.push(sid, encodeResponse(id, nil, &jsonRPCError{Code: -32600, Message: "not initialized"}))
	default:
		go func() {
			result, rpcErr := f.answer(m)
			if f.preface {
				f.push(sid, `{"jsonrpc":"2.0","method":"notifications/progress","params":{}}`)
				f.push(sid, `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}`)
				f.push(sid, `{"jsonrpc":"2.0","id":7,"method":"ping"}`)
			}
			for i := range f.pings {
				f.push(sid, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"ping"}`, 1000+i))
			}
			f.push(sid, encodeResponse(id, result, rpcErr))
		}()
	}
}
