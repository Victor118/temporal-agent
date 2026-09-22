// Package claudecode runs the Claude Code CLI as a subprocess and turns its
// stream-json output into a structured result.
//
// This is the reusable core behind the future ClaudeCodeWorkflow: the same
// Run method is meant to be registered as a Temporal activity (it heartbeats
// on its own when it runs inside one) and is exercised standalone by the
// `agent claude-code-run` subcommand. It is deliberately policy-free — which
// repo, which branch, which permissions, whether anything gets pushed are
// decisions for the caller, not for this package.
package claudecode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultBinary         = "claude"
	defaultMaxReportBytes = 64 * 1024
	maxStderrTailBytes    = 8 * 1024
	// killGrace is how long the CLI gets to exit after SIGTERM before the
	// process group is killed outright.
	killGrace = 10 * time.Second
)

// Params describes one Claude Code run. Only Cwd and Task are required; every
// other field maps to a CLI flag that is omitted when left empty.
type Params struct {
	Cwd  string `json:"cwd"`
	Task string `json:"task"`

	Model              string   `json:"model,omitempty"`
	PermissionMode     string   `json:"permission_mode,omitempty"`    // --permission-mode: plan, acceptEdits, bypassPermissions…
	PermissionPrompts  string   `json:"permission_prompts,omitempty"` // --permission-prompts: defaults to "none" (never wait on a prompt)
	AllowedTools       []string `json:"allowed_tools,omitempty"`
	DisallowedTools    []string `json:"disallowed_tools,omitempty"`
	Tools              []string `json:"tools,omitempty"` // --tools: restrict the built-in set
	AppendSystemPrompt string   `json:"append_system_prompt,omitempty"`
	AddDirs            []string `json:"add_dirs,omitempty"`
	MCPConfig          []string `json:"mcp_config,omitempty"` // JSON strings or file paths
	StrictMCPConfig    bool     `json:"strict_mcp_config,omitempty"`
	MaxBudgetUSD       float64  `json:"max_budget_usd,omitempty"`

	// SessionID pins the CLI session id. A retried activity that reuses the
	// same id keeps one session in the CLI's own logs instead of scattering
	// the attempts. Must be a UUID.
	SessionID string `json:"session_id,omitempty"`
	// NoSessionPersistence stops the CLI from writing the transcript to disk.
	// Right for a workspace that gets deleted at the end of the run.
	NoSessionPersistence bool `json:"no_session_persistence,omitempty"`

	// Env adds "KEY=value" entries on top of the process environment. This is
	// how context reaches an MCP server the CLI spawns.
	Env []string `json:"env,omitempty"`
}

// Result is what one run produced. A run that the CLI itself reports as failed
// (IsError) is still a Result, not a Go error: the caller decides what to make
// of it. A Go error means the CLI could not be run or did not report at all.
type Result struct {
	Report    string `json:"report"`   // final assistant text, truncated to MaxReportBytes
	IsError   bool   `json:"is_error"` // the CLI's own verdict on the run
	Subtype   string `json:"subtype"`  // success, error_max_turns, error_during_execution…
	SessionID string `json:"session_id"`
	Model     string `json:"model,omitempty"`

	NumTurns       int     `json:"num_turns"`
	DurationMS     int64   `json:"duration_ms"`
	CostUSD        float64 `json:"cost_usd"`
	ExitCode       int     `json:"exit_code"`
	TerminalReason string  `json:"terminal_reason,omitempty"`

	ToolUses          map[string]int `json:"tool_uses,omitempty"` // tool name → call count
	PermissionDenials []string       `json:"permission_denials,omitempty"`
	Stderr            string         `json:"stderr,omitempty"` // tail, for diagnosing a CLI that never reported
}

// Progress is the heartbeat payload. It stays small on purpose: Temporal
// stores it, and a retry reads it back to know how far the last attempt got.
type Progress struct {
	SessionID string `json:"session_id,omitempty"`
	Events    int    `json:"events"`
	ToolCalls int    `json:"tool_calls"`
	LastTool  string `json:"last_tool,omitempty"`
	LastText  string `json:"last_text,omitempty"`
}

// Event is one normalized item off the CLI's stream. Assistant messages are
// split so a caller gets one event per text block and one per tool call.
type Event struct {
	Kind      EventKind
	Text      string          // Kind text: the assistant's words
	ToolName  string          // Kind tool_use / tool_result
	ToolInput json.RawMessage // Kind tool_use
	IsError   bool            // Kind tool_result
	Raw       json.RawMessage // the original line, for anything not modeled here
}

type EventKind string

const (
	EventInit       EventKind = "init"
	EventText       EventKind = "text"
	EventThinking   EventKind = "thinking"
	EventToolUse    EventKind = "tool_use"
	EventToolResult EventKind = "tool_result"
	EventResult     EventKind = "result"
	EventOther      EventKind = "other"
)

// Runner runs the CLI. The zero value works: it calls "claude" from PATH.
type Runner struct {
	// Binary is the CLI to run. Empty means "claude" from PATH, or the value
	// of CLAUDE_CODE_BIN.
	Binary string
	// OnEvent, when set, is called for every event as it arrives. It runs on
	// the goroutine reading the stream, so it must not block for long.
	OnEvent func(Event)
	// MaxReportBytes caps Result.Report. Zero means 64 KiB.
	MaxReportBytes int
	// HeartbeatEvery throttles heartbeats when running inside an activity.
	// Zero means every event.
	HeartbeatEvery time.Duration
}

// Run executes one Claude Code run and blocks until the CLI exits.
//
// The CLI gets no stdin beyond the task, so it can never sit waiting for
// input: it either finishes or hits its own limits. When ctx is cancelled —
// an activity timeout, a cancelled workflow — the whole process group is
// signalled, not just the CLI, because it spawns shells of its own that would
// otherwise outlive it.
func (r *Runner) Run(ctx context.Context, p Params) (Result, error) {
	if p.Cwd == "" {
		return Result{}, errors.New("claudecode: cwd is required")
	}
	if strings.TrimSpace(p.Task) == "" {
		return Result{}, errors.New("claudecode: task is required")
	}
	if fi, err := os.Stat(p.Cwd); err != nil {
		return Result{}, fmt.Errorf("claudecode: cwd %q: %w", p.Cwd, err)
	} else if !fi.IsDir() {
		return Result{}, fmt.Errorf("claudecode: cwd %q is not a directory", p.Cwd)
	}

	// CommandContext, not Command: os/exec only honours Cmd.Cancel for a
	// command that carries a context.
	cmd := exec.CommandContext(ctx, r.binary(), buildArgs(p)...)
	cmd.Dir = p.Cwd
	cmd.Env = append(os.Environ(), p.Env...)
	// The task goes in on stdin rather than argv: it is arbitrary user text of
	// arbitrary length, and argv has a limit.
	cmd.Stdin = strings.NewReader(p.Task)

	// Cancellation reaches the CLI's children too. Without the process group,
	// a build or test it launched keeps running in the container after the
	// activity is gone.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) }
	cmd.WaitDelay = killGrace

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return Result{}, fmt.Errorf("claudecode: stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return Result{}, fmt.Errorf("claudecode: stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return Result{}, fmt.Errorf("claudecode: cannot start %q: %w", r.binary(), err)
	}

	var tail stderrTail
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		io.Copy(&tail, stderr)
	}()

	res, parseErr := r.consume(ctx, stdout)
	wg.Wait()

	waitErr := cmd.Wait()
	res.ExitCode = cmd.ProcessState.ExitCode()
	res.Stderr = tail.String()

	if ctxErr := ctx.Err(); ctxErr != nil {
		return res, fmt.Errorf("claudecode: run interrupted after %s: %w", time.Duration(res.DurationMS)*time.Millisecond, ctxErr)
	}
	if parseErr != nil {
		return res, parseErr
	}
	// No result event means the CLI died before reporting — a bad flag, a
	// missing credential, an OOM. The exit code alone says little, so the
	// stderr tail is the diagnosis and belongs in the error.
	if res.Subtype == "" {
		if waitErr != nil {
			return res, fmt.Errorf("claudecode: CLI exited with %v and reported nothing: %s", waitErr, res.Stderr)
		}
		return res, fmt.Errorf("claudecode: CLI exited without reporting a result: %s", res.Stderr)
	}
	return res, nil
}

func (r *Runner) binary() string {
	if r.Binary != "" {
		return r.Binary
	}
	if bin := os.Getenv("CLAUDE_CODE_BIN"); bin != "" {
		return bin
	}
	return defaultBinary
}

// buildArgs assembles the argv. -p with stream-json is the only mode that
// reports tool calls as they happen, which is what heartbeats and the
// execution view are built on; --verbose is what makes it emit them.
func buildArgs(p Params) []string {
	args := []string{"-p", "--output-format", "stream-json", "--verbose"}

	prompts := p.PermissionPrompts
	if prompts == "" {
		// Nobody is there to answer. Deny instead of waiting forever.
		prompts = "none"
	}
	args = append(args, "--permission-prompts", prompts)

	if p.Model != "" {
		args = append(args, "--model", p.Model)
	}
	if p.PermissionMode != "" {
		args = append(args, "--permission-mode", p.PermissionMode)
	}
	if len(p.AllowedTools) > 0 {
		args = append(args, "--allowedTools", strings.Join(p.AllowedTools, ","))
	}
	if len(p.DisallowedTools) > 0 {
		args = append(args, "--disallowedTools", strings.Join(p.DisallowedTools, ","))
	}
	if len(p.Tools) > 0 {
		args = append(args, "--tools", strings.Join(p.Tools, ","))
	}
	if p.AppendSystemPrompt != "" {
		args = append(args, "--append-system-prompt", p.AppendSystemPrompt)
	}
	for _, d := range p.AddDirs {
		args = append(args, "--add-dir", d)
	}
	for _, c := range p.MCPConfig {
		args = append(args, "--mcp-config", c)
	}
	if p.StrictMCPConfig {
		args = append(args, "--strict-mcp-config")
	}
	if p.MaxBudgetUSD > 0 {
		args = append(args, "--max-budget-usd", fmt.Sprintf("%g", p.MaxBudgetUSD))
	}
	if p.SessionID != "" {
		args = append(args, "--session-id", p.SessionID)
	}
	if p.NoSessionPersistence {
		args = append(args, "--no-session-persistence")
	}
	return args
}
