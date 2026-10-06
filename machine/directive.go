package machine

import (
	"encoding/json"
	"fmt"
	"time"
)

// Directive kinds. A kind needs the capability of the same name: a machine
// announces what it can run.
const (
	// KindEcho answers its text after a while, with progress on the way:
	// phase 0's directive, which proves the mechanism and nothing else.
	KindEcho = "echo"
)

// Temporal error types of RunOnMachine, for the workflow to say why a
// directive ended without a result. A machine lost past the heartbeat
// timeout is Temporal's own timeout.
const (
	ErrTypeRevoked  = "MachineRevoked"
	ErrTypeStopping = "MachineStopping"
	ErrTypeFailed   = "DirectiveFailed"
	// ErrTypeLost is a directive its machine no longer knows (it restarted
	// between receiving it and saying so).
	ErrTypeLost = "DirectiveLost"
	// ErrTypeClosed is a directive closed before it could start (swept,
	// its machine revoked).
	ErrTypeClosed = "DirectiveClosed"
	// ErrTypeRefused is a directive that never started: its machine turned
	// it down (a repository it does not allow, Claude Code not logged in),
	// or it could not be handed over. Nothing ran: it may go elsewhere.
	ErrTypeRefused = "DirectiveRefused"
)

// Result is what RunOnMachine returns: the machine's output, its last
// progress, and the files it published for the directive's turn, as the
// gateway lists them from its database (never the machine's word).
type Result struct {
	Output   json.RawMessage `json:"output,omitempty"`
	Progress string          `json:"progress,omitempty"`
	Files    []FileRef       `json:"files,omitempty"`
}

// FileRef is a file a machine published, as the server stored it: never its
// content (tool.FileRef's fields).
type FileRef struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	ContentType string `json:"content_type"`
	Size        int64  `json:"size"`
	SHA256      string `json:"sha256"`
}

// FilesPath is the server's route where a machine publishes a file of a
// directive it runs: PUT, its machine token as Bearer, the directive and the
// file's name as the query's "directive" and "name", the content as the
// body. The answer is the FileRef, or an UploadError.
const FilesPath = "/machines/files"

// UploadError is the server's answer to a file it refused, in words for the
// model (the machine passes them on in its output).
type UploadError struct {
	Error string `json:"error"`
}

// Heartbeat is what the gateway records on a directive's activity: its
// machine's last progress. A timed out activity's last heartbeat says how
// far it went.
type Heartbeat struct {
	Progress string `json:"progress,omitempty"`
}

// EchoInput is an echo directive: Text comes back after Duration, with a
// progress every ProgressEvery (0 = none).
type EchoInput struct {
	Text            string `json:"text"`
	DurationMS      int64  `json:"duration_ms"`
	ProgressEveryMS int64  `json:"progress_every_ms,omitempty"`
}

// MaxEchoDuration bounds an echo, and MinEchoProgress its progress rate.
const (
	MaxEchoDuration = time.Hour
	MinEchoProgress = 100 * time.Millisecond
	maxEchoText     = 4096
)

func (in EchoInput) Duration() time.Duration { return time.Duration(in.DurationMS) * time.Millisecond }
func (in EchoInput) ProgressEvery() time.Duration {
	return time.Duration(in.ProgressEveryMS) * time.Millisecond
}

// Check refuses an echo the machine should not run.
func (in EchoInput) Check() error {
	switch {
	case len(in.Text) > maxEchoText:
		return fmt.Errorf("echo text over %d bytes", maxEchoText)
	case in.DurationMS < 0 || in.Duration() > MaxEchoDuration:
		return fmt.Errorf("echo duration %s out of [0, %s]", in.Duration(), MaxEchoDuration)
	case in.ProgressEveryMS < 0 || (in.ProgressEveryMS > 0 && in.ProgressEvery() < MinEchoProgress):
		return fmt.Errorf("echo progress every %s, under %s", in.ProgressEvery(), MinEchoProgress)
	}
	return nil
}

// EchoOutput is an echo's result: its text, and how many progresses it sent.
type EchoOutput struct {
	Text       string `json:"text"`
	Progresses int    `json:"progresses"`
}
