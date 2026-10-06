package claudecode

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// streamLine is the envelope every line of --output-format stream-json shares.
// Only the fields this package acts on are modeled; the rest of each line
// reaches the caller as Event.Raw.
type streamLine struct {
	Type      string          `json:"type"`
	Subtype   string          `json:"subtype"`
	SessionID string          `json:"session_id"`
	Model     string          `json:"model"`
	Message   json.RawMessage `json:"message"`
	// init line: which API key the CLI uses, "none" when it has none (a
	// subscription's login or token).
	APIKeySource string `json:"apiKeySource"`

	// result lines
	Result            string          `json:"result"`
	IsError           bool            `json:"is_error"`
	NumTurns          int             `json:"num_turns"`
	DurationMS        int64           `json:"duration_ms"`
	TotalCostUSD      float64         `json:"total_cost_usd"`
	TerminalReason    string          `json:"terminal_reason"`
	PermissionDenials []denial        `json:"permission_denials"`
	Error             json.RawMessage `json:"error"`
}

type denial struct {
	ToolName string `json:"tool_name"`
	Message  string `json:"message"`
}

// message is the Anthropic message carried by assistant and user lines.
type message struct {
	Model   string         `json:"model"`
	Content []contentBlock `json:"content"`
}

type contentBlock struct {
	Type      string          `json:"type"` // text, thinking, tool_use, tool_result
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	Name      string          `json:"name"`  // tool_use
	ID        string          `json:"id"`    // tool_use
	Input     json.RawMessage `json:"input"` // tool_use
	ToolUseID string          `json:"tool_use_id"`
	IsError   bool            `json:"is_error"` // tool_result
}

// consume reads the CLI's stream to EOF, building the Result as it goes and
// emitting events. A line it cannot parse is skipped rather than fatal: the
// CLI may add line types, and losing one is no reason to lose a 20-minute run.
//
// What it reads also goes to w: how far the run got, and when the CLI last
// wrote, which the heartbeat and the stall check read (Runner.monitor).
func (r *Runner) consume(stdout io.Reader, w *watch) (Result, error) {
	res := Result{ToolUses: map[string]int{}}
	var report strings.Builder
	toolNames := map[string]string{} // tool_use_id → name, to name tool results

	// A bufio.Reader rather than a Scanner: one line carries a whole tool
	// result, which routinely passes Scanner's 64 KiB token limit, and a
	// Scanner that trips on it abandons the rest of the stream.
	br := bufio.NewReaderSize(stdout, 64*1024)
	for {
		line, err := readLine(br)
		if len(line) > 0 {
			w.wrote()
			for _, ev := range r.parseLine(line, &res, &report, toolNames) {
				if r.OnEvent != nil {
					r.OnEvent(ev)
				}
				w.observe(ev, res.SessionID)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return res, fmt.Errorf("claudecode: reading CLI output: %w", err)
		}
	}

	if res.Report == "" {
		// No result line carried the final text (an interrupted run): fall
		// back to what the assistant had said so far, so the caller still
		// sees the work instead of an empty string.
		res.Report = truncate(strings.TrimSpace(report.String()), r.maxReportBytes())
	}
	if len(res.ToolUses) == 0 {
		res.ToolUses = nil
	}
	return res, nil
}

// parseLine folds one stream line into res and returns the events it produced.
func (r *Runner) parseLine(line []byte, res *Result, report *strings.Builder, toolNames map[string]string) []Event {
	var sl streamLine
	if err := json.Unmarshal(line, &sl); err != nil {
		return nil // not JSON: CLI noise on stdout, ignore
	}
	raw := json.RawMessage(append([]byte(nil), line...))

	if sl.SessionID != "" {
		res.SessionID = sl.SessionID
	}

	switch sl.Type {
	case "system":
		if sl.Subtype == "init" {
			if sl.Model != "" {
				res.Model = sl.Model
			}
			res.APIKeySource = sl.APIKeySource
			return []Event{{Kind: EventInit, Raw: raw}}
		}

	case "assistant", "user":
		var m message
		if err := json.Unmarshal(sl.Message, &m); err != nil {
			return []Event{{Kind: EventOther, Raw: raw}}
		}
		if m.Model != "" {
			res.Model = m.Model
		}
		var events []Event
		for _, b := range m.Content {
			switch b.Type {
			case "text":
				if b.Text == "" {
					continue
				}
				report.WriteString(b.Text)
				report.WriteString("\n")
				events = append(events, Event{Kind: EventText, Text: b.Text, Raw: raw})
			case "thinking":
				events = append(events, Event{Kind: EventThinking, Text: b.Thinking, Raw: raw})
			case "tool_use":
				res.ToolUses[b.Name]++
				toolNames[b.ID] = b.Name
				events = append(events, Event{Kind: EventToolUse, ToolName: b.Name, ToolInput: b.Input, Raw: raw})
			case "tool_result":
				events = append(events, Event{Kind: EventToolResult, ToolName: toolNames[b.ToolUseID], IsError: b.IsError, Raw: raw})
			}
		}
		return events

	case "result":
		res.Subtype = sl.Subtype
		res.IsError = sl.IsError
		res.NumTurns = sl.NumTurns
		res.DurationMS = sl.DurationMS
		res.CostUSD = sl.TotalCostUSD
		res.TerminalReason = sl.TerminalReason
		for _, d := range sl.PermissionDenials {
			res.PermissionDenials = append(res.PermissionDenials, strings.TrimSpace(d.ToolName+" "+d.Message))
		}
		text := sl.Result
		if text == "" && len(sl.Error) > 0 {
			text = string(sl.Error)
		}
		res.Report = truncate(strings.TrimSpace(text), r.maxReportBytes())
		return []Event{{Kind: EventResult, Text: res.Report, IsError: res.IsError, Raw: raw}}
	}

	return []Event{{Kind: EventOther, Raw: raw}}
}

// watch is what the CLI's stream says of a run while it lasts: how far it
// got, and when the CLI last wrote a line. consume writes it; the heartbeat
// and the stall check (Runner.monitor) read it from another goroutine.
type watch struct {
	mu       sync.Mutex
	prog     Progress
	lastLine time.Time
}

func newWatch(start time.Time) *watch { return &watch{lastLine: start} }

// wrote records that the CLI just wrote a line, whatever it was.
func (w *watch) wrote() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastLine = time.Now()
}

func (w *watch) observe(ev Event, sessionID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.prog.observe(ev, sessionID)
}

func (w *watch) progress() Progress {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.prog
}

// silentFor is how long the CLI has written nothing.
func (w *watch) silentFor() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return time.Since(w.lastLine)
}

// monitor heartbeats while the CLI lives, and ends the run (stall) once the
// CLI has written nothing for stallTimeout, until done is closed.
//
// The heartbeat goes on a timer, not on the CLI's lines: the CLI is silent
// for minutes on end and fine — a Bash command writes nothing until it ends
// (up to 10 min by default), an assistant message only comes out whole, nor
// do an MCP call, a WebFetch or the CLI's own backoff on an overloaded API
// write anything while they last. Beating on its
// lines alone, such a run was taken for a lost worker. The heartbeat now
// only says the worker is alive, and carries the progress; a CLI that is
// alive but stuck is this function's to notice, at stallTimeout.
//
// It beats only inside an activity (beat), which is what lets the same
// runner serve the CLI subcommand; the stall check runs either way, unless
// StallTimeout is negative.
func (r *Runner) monitor(ctx context.Context, w *watch, stall context.CancelCauseFunc, done <-chan struct{}) {
	beats := time.NewTicker(r.heartbeatEvery())
	defer beats.Stop()
	limit := r.stallTimeout()
	var checks <-chan time.Time // nil, never ready: no stall check
	var check *time.Timer
	if limit > 0 {
		check = time.NewTimer(limit)
		defer check.Stop()
		checks = check.C
	}
	for {
		select {
		case <-done:
			return
		case <-beats.C:
			r.beat(ctx, w.progress())
		case <-checks:
			silent := w.silentFor()
			if silent >= limit {
				stall(&StallError{Silence: limit, Progress: w.progress()})
				return
			}
			check.Reset(limit - silent)
		}
	}
}

// beat hands the run's progress to OnBeat, the caller's: a Temporal
// heartbeat inside an activity (which is also what lets a run be cancelled
// at all: the SDK learns of a cancellation, or of a timeout the server saw,
// in a heartbeat's answer), a machine's progress in agent connect. This
// package knows neither.
func (r *Runner) beat(ctx context.Context, prog Progress) {
	if r.OnBeat != nil {
		r.OnBeat(ctx, prog)
	}
}

func (r *Runner) heartbeatEvery() time.Duration {
	if r.HeartbeatEvery > 0 {
		return r.HeartbeatEvery
	}
	return DefaultHeartbeatEvery
}

// stallTimeout is StallTimeout, or its default; negative means none.
func (r *Runner) stallTimeout() time.Duration {
	if r.StallTimeout != 0 {
		return r.StallTimeout
	}
	return DefaultStallTimeout
}

func (p *Progress) observe(ev Event, sessionID string) {
	p.SessionID = sessionID
	p.Events++
	switch ev.Kind {
	case EventToolUse:
		p.ToolCalls++
		p.LastTool = ev.ToolName
	case EventText:
		p.LastText = truncate(ev.Text, 500)
	}
}

func (r *Runner) maxReportBytes() int {
	if r.MaxReportBytes > 0 {
		return r.MaxReportBytes
	}
	return defaultMaxReportBytes
}

// readLine returns one line without its terminator, growing as needed. It
// returns any bytes read before an error alongside it.
func readLine(br *bufio.Reader) ([]byte, error) {
	var buf []byte
	for {
		chunk, isPrefix, err := br.ReadLine()
		buf = append(buf, chunk...)
		if err != nil {
			return buf, err
		}
		if !isPrefix {
			return buf, nil
		}
	}
}

// truncate keeps the head and the tail of an oversized string: the head says
// what the run did, the tail usually carries the conclusion. Cuts land on rune
// boundaries so the result stays valid UTF-8 for the JSON payloads downstream.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	head := runeStart(s, max*2/3)
	tail := runeStart(s, len(s)-(max-head))
	if tail <= head {
		tail = len(s)
	}
	return fmt.Sprintf("%s\n\n[... %d bytes omitted ...]\n\n%s", s[:head], tail-head, s[tail:])
}

func runeStart(s string, i int) int {
	if i >= len(s) {
		return len(s)
	}
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}
	return i
}

// stderrTail keeps the last maxStderrTailBytes of the CLI's stderr. The head
// is startup noise; the tail is where an auth or flag failure shows up.
type stderrTail struct {
	buf []byte
}

func (t *stderrTail) Write(p []byte) (int, error) {
	t.buf = append(t.buf, p...)
	if len(t.buf) > maxStderrTailBytes {
		t.buf = t.buf[len(t.buf)-maxStderrTailBytes:]
	}
	return len(p), nil
}

func (t *stderrTail) String() string { return strings.TrimSpace(string(t.buf)) }
