// Package machine is what the gateway, the workers and `agent connect`
// share about machines: the messages of their WebSocket, the tokens and
// codes of their enrollment, and the directives they run. It holds no I/O
// of its own: see docs/design/machines.md.
package machine

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"
)

// Protocol is the version of the messages this binary speaks; a machine
// sends it in its hello. MinProtocol is the oldest the gateway accepts: the
// machines run the binary their owner installed, the one place where two
// versions of the project really meet.
const (
	Protocol    = 2
	MinProtocol = 2
)

// Message types. A machine sends hello, rotated, progress, result and
// capabilities; the gateway welcome, rotate, directive, cancel, ack and
// error.
const (
	TypeHello     = "hello"
	TypeWelcome   = "welcome"
	TypeRotate    = "rotate"
	TypeRotated   = "rotated"
	TypeDirective = "directive"
	TypeProgress  = "progress"
	TypeResult    = "result"
	TypeCancel    = "cancel"
	TypeAck       = "ack"
	TypeError     = "error"
	// TypeCapabilities: a machine's capabilities changed while connected (a
	// login lost, or back).
	TypeCapabilities = "capabilities"
)

// Result statuses: how a directive ended on the machine.
const (
	StatusOK       = "ok"
	StatusError    = "error"
	StatusCanceled = "canceled"
	// StatusStopping is a directive ended because `agent connect` stops:
	// the run is lost, like a machine that went away.
	StatusStopping = "machine_stopping"
)

// Error codes the gateway sends before it closes a connection.
const (
	CodeProtocol   = "protocol_too_old"
	CodeBadMessage = "bad_message"
	CodeDuplicate  = "duplicate_connection"
	CodeRevoked    = "revoked"
)

// WebSocket close codes (4000-4999 are the application's). A machine stops
// reconnecting on CloseRevoked and CloseProtocol: retrying cannot help.
const (
	CloseRevoked   = 4001
	CloseDuplicate = 4002
	CloseProtocol  = 4003
	ClosePolicy    = 4008
)

// Limits on what a machine sends. What it says is not trusted (§11 of the
// design): a message past MaxMessageBytes ends the connection, a progress
// is cut to MaxProgressBytes (it ends up in the workflow's history, through
// the heartbeats).
const (
	MaxMessageBytes  = 256 << 10
	MaxProgressBytes = 1024
	MaxListedIDs     = 64
	MaxCapabilities  = 16
	MaxDirectives    = 16
	MaxErrorBytes    = 4096
)

// ProgressInterval is how often a machine sends a directive's progress at
// most; the gateway's limit on messages counts on it.
const ProgressInterval = 250 * time.Millisecond

// Message is one message of the WebSocket, in JSON. Which fields it carries
// depends on its type; the others are empty.
type Message struct {
	Type string `json:"type"`

	// hello: what the machine is and holds. Running are the directives it
	// runs, Finished those whose result it keeps until the gateway's ack.
	Protocol      int      `json:"protocol,omitempty"`
	AgentVersion  string   `json:"agent_version,omitempty"`
	OS            string   `json:"os,omitempty"`
	Capabilities  []string `json:"capabilities,omitempty"`
	MaxDirectives int      `json:"max_directives,omitempty"`
	Running       []string `json:"running,omitempty"`
	Finished      []string `json:"finished,omitempty"`
	// ClaudeCode is the state of the machine's claude CLI (hello and
	// capabilities): "ok", "logged_out", "absent" (claudecode.LoginStatus).
	ClaudeCode string `json:"claude_code,omitempty"`

	// welcome
	MachineID string `json:"machine_id,omitempty"`
	Name      string `json:"name,omitempty"`

	// rotate: the machine's next token
	Token string `json:"token,omitempty"`

	// directive, progress, result, cancel, ack: the directive
	ID       string          `json:"id,omitempty"`
	Kind     string          `json:"kind,omitempty"`
	Input    json.RawMessage `json:"input,omitempty"`
	Deadline *time.Time      `json:"deadline,omitempty"`
	// Text is a progress's text, a cancel's reason, an error's message.
	Text   string          `json:"text,omitempty"`
	Status string          `json:"status,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
	// Code is an error's code (Code*).
	Code string `json:"code,omitempty"`
}

var (
	capabilityPattern = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
	idPattern         = regexp.MustCompile(`^[A-Za-z0-9-]{1,64}$`)
)

// ValidCapability reports a capability's shape: what a machine may announce.
func ValidCapability(c string) bool { return capabilityPattern.MatchString(c) }

// ErrBadMessage is a message from a machine that breaks the protocol.
var ErrBadMessage = errors.New("bad message")

func bad(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrBadMessage, fmt.Sprintf(format, args...))
}

// CheckFromMachine validates a message a machine sent, and cuts what may be
// cut (a progress's text). What it refuses ends the connection.
func CheckFromMachine(m *Message) error {
	switch m.Type {
	case TypeHello:
		if m.Protocol <= 0 {
			return bad("hello without a protocol version")
		}
		if m.MaxDirectives < 1 || m.MaxDirectives > MaxDirectives {
			return bad("max_directives %d", m.MaxDirectives)
		}
		if len(m.Running)+len(m.Finished) > MaxListedIDs {
			return bad("%d directives listed", len(m.Running)+len(m.Finished))
		}
		for _, id := range append(append([]string(nil), m.Running...), m.Finished...) {
			if !idPattern.MatchString(id) {
				return bad("directive id %q", id)
			}
		}
		m.OS = Cut(m.OS, 64)
		m.AgentVersion = Cut(m.AgentVersion, 64)
		if err := checkStatus(m); err != nil {
			return err
		}
	case TypeCapabilities:
		if err := checkStatus(m); err != nil {
			return err
		}
	case TypeRotated:
	case TypeProgress:
		if !idPattern.MatchString(m.ID) {
			return bad("directive id %q", m.ID)
		}
		m.Text = Cut(m.Text, MaxProgressBytes)
	case TypeResult:
		if !idPattern.MatchString(m.ID) {
			return bad("directive id %q", m.ID)
		}
		switch m.Status {
		case StatusOK, StatusError, StatusCanceled, StatusStopping:
		default:
			return bad("result status %q", m.Status)
		}
		if len(m.Output) > 0 && !json.Valid(m.Output) {
			return bad("result output is not JSON")
		}
		m.Error = Cut(m.Error, MaxErrorBytes)
		m.Text = Cut(m.Text, MaxProgressBytes)
	default:
		return bad("type %q from a machine", m.Type)
	}
	return nil
}

// checkStatus checks what a hello or capabilities message says the machine
// can do.
func checkStatus(m *Message) error {
	if len(m.Capabilities) > MaxCapabilities {
		return bad("%d capabilities", len(m.Capabilities))
	}
	for _, c := range m.Capabilities {
		if !capabilityPattern.MatchString(c) {
			return bad("capability %q", c)
		}
	}
	switch m.ClaudeCode {
	case "", "ok", "logged_out", "absent":
	default:
		return bad("claude_code %q", m.ClaudeCode)
	}
	return nil
}

// Cut shortens s to at most n bytes, on a rune boundary, and marks the cut.
func Cut(s string, n int) string {
	if len(s) <= n {
		return s
	}
	const mark = "…"
	end := n - len(mark)
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end] + mark
}
