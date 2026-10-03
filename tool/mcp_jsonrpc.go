package tool

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
)

// JSON-RPC 2.0, as MCP uses it.

type jsonRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type jsonRPCNotification struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

// jsonRPCReply is the client's answer to a request from the server.
type jsonRPCReply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

// jsonRPCMessage is any message a server sends: a response, a request or a
// notification, told apart by which fields it has.
type jsonRPCMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

// jsonRPCError is an error the server answered a request with.
type jsonRPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *jsonRPCError) Error() string {
	return fmt.Sprintf("mcp rpc error %d: %s", e.Code, e.Message)
}

const jsonRPCMethodNotFound = -32601

func parseMessage(data []byte) (jsonRPCMessage, bool) {
	var m jsonRPCMessage
	if err := json.Unmarshal(data, &m); err != nil || m.JSONRPC != "2.0" {
		return jsonRPCMessage{}, false
	}
	return m, true
}

func (m jsonRPCMessage) hasID() bool {
	return len(m.ID) > 0 && !bytes.Equal(bytes.TrimSpace(m.ID), []byte("null"))
}

// isRequest: the server asks something and waits for the answer.
func (m jsonRPCMessage) isRequest() bool { return m.Method != "" && m.hasID() }

// answers reports whether m is the response to the request numbered id. A
// server echoes the ID as sent; one written as a string is accepted too.
func (m jsonRPCMessage) answers(id int64) bool {
	if m.Method != "" || !m.hasID() {
		return false
	}
	var n int64
	if json.Unmarshal(m.ID, &n) == nil {
		return n == id
	}
	var s string
	if json.Unmarshal(m.ID, &s) == nil {
		n, err := strconv.ParseInt(s, 10, 64)
		return err == nil && n == id
	}
	return false
}

// unattributedError: an error response without an ID, which a server sends
// when it could not read the request at all.
func (m jsonRPCMessage) unattributedError() bool {
	return m.Method == "" && !m.hasID() && m.Error != nil
}

func (m jsonRPCMessage) outcome() (json.RawMessage, error) {
	if m.Error != nil {
		return nil, m.Error
	}
	return m.Result, nil
}

// replyTo is this client's answer to a server's request. It offers no
// capability (sampling, roots, elicitation), so it answers ping alone; any
// other request gets an error at once rather than a server left waiting.
func replyTo(m jsonRPCMessage) jsonRPCReply {
	if m.Method == "ping" {
		return jsonRPCReply{JSONRPC: "2.0", ID: m.ID, Result: struct{}{}}
	}
	return jsonRPCReply{JSONRPC: "2.0", ID: m.ID, Error: &jsonRPCError{
		Code:    jsonRPCMethodNotFound,
		Message: "method not supported by this client: " + m.Method,
	}}
}

// errSessionExpired: the server no longer knows the session, and did not
// process the request. A new session may send it again.
var errSessionExpired = errors.New("mcp: session expired")

// mcpHTTPError is an HTTP status a server answered with instead of a message.
type mcpHTTPError struct {
	Status int
	Body   string
}

func (e *mcpHTTPError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("mcp server answered HTTP %d", e.Status)
	}
	return fmt.Sprintf("mcp server answered HTTP %d: %s", e.Status, e.Body)
}

// statusError reads the start of an error answer, enough to say why.
func statusError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return &mcpHTTPError{Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
}

func mediaType(resp *http.Response) string {
	mt, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	return mt
}
