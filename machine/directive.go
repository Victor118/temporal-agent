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
)

// Result is what RunOnMachine returns: the machine's output, and its last
// progress.
type Result struct {
	Output   json.RawMessage `json:"output,omitempty"`
	Progress string          `json:"progress,omitempty"`
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
