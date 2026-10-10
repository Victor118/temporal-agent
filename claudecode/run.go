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
	"syscall"
	"time"

	"github.com/victor/temporal-agent/subproc"
)

const (
	defaultBinary         = "claude"
	defaultMaxReportBytes = 64 * 1024
	maxStderrTailBytes    = 8 * 1024
)

// DefaultHeartbeatEvery is how often a run heartbeats while its CLI lives,
// when Runner.HeartbeatEvery is zero: well inside the activity's
// HeartbeatTimeout (a minute), so that only a lost worker misses it.
const DefaultHeartbeatEvery = 20 * time.Second

// DefaultStallTimeout is how long the CLI may write nothing before its run
// is taken for stuck and ended, when Runner.StallTimeout is zero. Longer than
// what the CLI is silent for while it works: a Bash command runs 10 min at
// most (unless BASH_MAX_TIMEOUT_MS raises it), and the CLI writes nothing
// until it ends. A worker that allows longer sets CLAUDE_CODE_STALL_TIMEOUT.
const DefaultStallTimeout = 12 * time.Minute

// killGrace is how long the CLI gets to exit after SIGTERM before the process
// group is killed outright, and how long its output is waited on once it has
// exited (exec.Cmd.WaitDelay). A variable: the tests shorten it.
var killGrace = 10 * time.Second

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
	// SettingSources are the settings files the CLI loads (--setting-sources:
	// user, project, local); empty = all. A clone's own .claude/settings.json
	// (hooks it would run, even in plan mode) is "project": a run on a
	// repository someone else writes loads "user" only.
	SettingSources []string `json:"setting_sources,omitempty"`
	MaxBudgetUSD   float64  `json:"max_budget_usd,omitempty"`
	// PluginDirs are plugins the CLI loads for this run alone
	// (--plugin-dir, PluginDirFlag): a run's skills (machine.WritePlugin).
	PluginDirs []string `json:"plugin_dirs,omitempty"`

	// SessionID pins the CLI session id. A retried activity that reuses the
	// same id keeps one session in the CLI's own logs instead of scattering
	// the attempts. Must be a UUID.
	SessionID string `json:"session_id,omitempty"`
	// NoSessionPersistence stops the CLI from writing the transcript to disk.
	// Right for a workspace that gets deleted at the end of the run.
	NoSessionPersistence bool `json:"no_session_persistence,omitempty"`

	// Env adds "KEY=value" entries to the CLI's environment, which otherwise
	// holds only what cliEnv keeps from the worker's. This is how context
	// reaches an MCP server the CLI spawns.
	Env []string `json:"env,omitempty"`

	// ConfigDir is the CLI's configuration for this run (CLAUDE_CONFIG_DIR),
	// one of its own (SeedConfigDir); empty keeps the worker's.
	ConfigDir string `json:"config_dir,omitempty"`
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
	// APIKeySource is the CLI's own word on where its API key came from:
	// "none" when it used none. Empty if it never said.
	APIKeySource string `json:"api_key_source,omitempty"`
	// SlashCommands are the commands the CLI said it has, in its init: a
	// plugin's skill it loaded is one of them ("<plugin>:<skill>").
	SlashCommands []string `json:"slash_commands,omitempty"`
	// Auth is the worker's way of authenticating the run, and PaidBy what
	// paid it (Payer), both set by the caller that knows the worker's choice:
	// the CLI's word alone does not say who pays.
	Auth   Auth `json:"auth,omitempty"`
	PaidBy Auth `json:"paid_by,omitempty"`

	NumTurns       int     `json:"num_turns"`
	DurationMS     int64   `json:"duration_ms"`
	CostUSD        float64 `json:"cost_usd"`
	ExitCode       int     `json:"exit_code"`
	TerminalReason string  `json:"terminal_reason,omitempty"`

	ToolUses          map[string]int `json:"tool_uses,omitempty"` // tool name → call count
	PermissionDenials []string       `json:"permission_denials,omitempty"`
	Stderr            string         `json:"stderr,omitempty"` // tail, for diagnosing a CLI that never reported

	// Progress is how far the run got, as its last heartbeat would say it:
	// all there is to tell of a run that failed before its result line.
	Progress Progress `json:"progress"`
}

// StallError is a run ended because its CLI wrote nothing for Silence
// (Runner.StallTimeout): stuck, or waiting on something that never comes.
type StallError struct {
	Silence  time.Duration
	Progress Progress
	// Stderr is the tail of the CLI's stderr, where it may have said why.
	Stderr string
}

func (e *StallError) Error() string {
	msg := fmt.Sprintf("claudecode: the CLI wrote nothing for %s (stuck?), so the run was ended", e.Silence)
	if e.Stderr != "" {
		msg += "; its stderr: " + e.Stderr
	}
	return msg
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
	// HeartbeatEvery is how often the run heartbeats (OnBeat) while the CLI
	// lives, whether it writes or not. Zero means DefaultHeartbeatEvery.
	HeartbeatEvery time.Duration
	// OnBeat receives the run's progress every HeartbeatEvery: an
	// activity's heartbeat, a machine's progress. Nil: none.
	OnBeat func(ctx context.Context, p Progress)
	// StallTimeout is how long the CLI may write nothing before the run is
	// ended as stuck (StallError). Zero means DefaultStallTimeout; negative,
	// never.
	StallTimeout time.Duration
	// RunAs is the user the CLI runs as; nil = this process's. Whether that
	// is acceptable is the caller's decision (subproc.CheckRunAs).
	RunAs *subproc.Identity
	// Runs counts the run while the CLI works, and ends every process of
	// RunAs's once no command of the worker runs as it (subproc.Runs, shared
	// with the rest of the worker). Required with RunAs: what a run left
	// running would otherwise never be ended.
	Runs Holder
	// Auth keeps the CLI to the worker's way of authenticating (ResolveAuth):
	// the other mode's credential never reaches it. Zero: as found.
	Auth Auth
	// NewSession starts the CLI in a session of its own, with no controlling
	// terminal: nothing it starts (ssh, git) can stop to prompt on one. For a
	// run started from a user's terminal (agent connect).
	NewSession bool
}

// Holder counts a command while it runs, and once none runs, ends every
// process of the user it ran as (subproc.Runs).
type Holder interface {
	Hold() (release func())
}

// Run executes one Claude Code run and blocks until the CLI exits.
//
// The CLI gets no stdin beyond the task, so it can never sit waiting for
// input: it either finishes or hits its own limits. When ctx is cancelled —
// an activity timeout, a cancelled workflow — the whole process group is
// signalled, not just the CLI, because it spawns shells of its own that would
// otherwise outlive it; when the CLI exits, what is left of the group is
// killed, and with RunAs, every process of that user once no other command of
// this worker runs as it (Runs). Its output is waited on for
// killGrace at most once it has exited, whatever still holds it open.
//
// A CLI that writes nothing for StallTimeout is ended the same way, and the
// run returns a StallError. While the CLI lives, OnBeat gets its Progress
// every HeartbeatEvery.
func (r *Runner) Run(ctx context.Context, p Params) (Result, error) {
	if p.Cwd == "" {
		return Result{}, errors.New("claudecode: cwd is required")
	}
	if strings.TrimSpace(p.Task) == "" {
		return Result{}, errors.New("claudecode: task is required")
	}
	if r.RunAs != nil && r.Runs == nil {
		return Result{}, errors.New("claudecode: a CLI run as another user needs a count of the runs (Runner.Runs)")
	}
	if fi, err := os.Stat(p.Cwd); err != nil {
		return Result{}, fmt.Errorf("claudecode: cwd %q: %w", p.Cwd, err)
	} else if !fi.IsDir() {
		return Result{}, fmt.Errorf("claudecode: cwd %q is not a directory", p.Cwd)
	}

	// CommandContext, not Command: os/exec only honours Cmd.Cancel for a
	// command that carries a context. Its own, which a stall cancels too.
	runCtx, stall := context.WithCancelCause(ctx)
	defer stall(nil)
	cmd := exec.CommandContext(runCtx, r.binary(), buildArgs(p)...)
	cmd.Dir = p.Cwd
	cmd.Env = r.RunAs.Env(r.Auth.Filter(append(cliEnv(os.Environ()), p.Env...)))
	// The CLI's version is the image's: it does not update itself, wherever
	// the user it runs as could write.
	cmd.Env = append(cmd.Env, "DISABLE_AUTOUPDATER=1")
	if p.ConfigDir != "" {
		cmd.Env = append(cmd.Env, "CLAUDE_CONFIG_DIR="+p.ConfigDir)
	}
	r.RunAs.Apply(cmd)
	// The task goes in on stdin rather than argv: it is arbitrary user text of
	// arbitrary length, and argv has a limit.
	cmd.Stdin = strings.NewReader(p.Task)

	// Cancellation reaches the CLI's children too. Without the process group,
	// a build or test it launched keeps running in the container after the
	// activity is gone.
	subproc.KillGroupOnCancel(cmd, syscall.SIGTERM, killGrace)
	if r.NewSession {
		subproc.NewSession(cmd)
	}

	// Writers, not StdoutPipe/StderrPipe: os/exec then makes the pipes and
	// copies from them itself, and WaitDelay bounds that copy too. A process
	// the CLI started with its stdout or stderr, and that outlives it, keeps
	// the pipe open: read to EOF, the stream would only end with the
	// activity's timeout; this way it ends killGrace after the CLI exits.
	stdout, stdoutW := io.Pipe()
	var tail stderrTail
	cmd.Stdout = stdoutW
	cmd.Stderr = &tail

	start := time.Now()
	if r.Runs != nil {
		defer r.Runs.Hold()()
	}
	if err := cmd.Start(); err != nil {
		return Result{}, fmt.Errorf("claudecode: cannot start %q: %w", r.binary(), err)
	}

	w := newWatch(start)
	watching, monitored := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(monitored)
		r.monitor(ctx, w, stall, watching)
	}()

	var res Result
	var parseErr error
	parsed := make(chan struct{})
	go func() {
		defer close(parsed)
		res, parseErr = r.consume(stdout, w)
		// Whatever follows a read error is drained: the copy writing into
		// the pipe must not block, nor Wait with it.
		io.Copy(io.Discard, stdout)
	}()

	// ErrWaitDelay is the output held open past the CLI's exit by something
	// it left running: the run itself is over, what it reported is in res.
	waitErr := cmd.Wait()
	stdoutW.Close()
	<-parsed
	// No heartbeat once Run has returned: the activity may be over by then.
	close(watching)
	<-monitored
	// What the CLI's shells left running dies with the run, not only with a
	// cancelled one: a dev server, a watcher.
	subproc.KillGroup(cmd)
	res.ExitCode = cmd.ProcessState.ExitCode()
	res.Stderr = tail.String()
	res.Progress = w.progress()
	if res.DurationMS == 0 {
		// The CLI reports its own duration on the result line. An interrupted
		// run never gets there, and a run that says it took no time at all is
		// the least useful thing to hand someone diagnosing a timeout.
		res.DurationMS = time.Since(start).Milliseconds()
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return res, fmt.Errorf("claudecode: run interrupted after %s: %w",
			time.Since(start).Round(time.Second), ctxErr)
	}
	// Ended as stuck, unless the CLI had reported its result by then: the
	// stall check fired as it exited, and the run is whole.
	var stalled *StallError
	if errors.As(context.Cause(runCtx), &stalled) && res.Subtype == "" {
		stalled.Stderr = res.Stderr
		return res, stalled
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

// Available reports whether the CLI this runner would start is installed. A
// worker without it has no business offering coding tools.
func (r *Runner) Available() bool {
	_, err := exec.LookPath(r.binary())
	return err == nil
}

// PluginDirFlag is the CLI's option that loads a plugin for one session
// (since 2.1.x; documented, unlike --plugin-dir-no-mcp).
const PluginDirFlag = "--plugin-dir"

// SupportsFlag tells whether the CLI this runner starts lists flag in its
// --help: an option an older CLI would refuse, run and all. Read without a
// call to the model; false when the CLI does not answer.
func (r *Runner) SupportsFlag(ctx context.Context, flag string) bool {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, r.binary(), "--help")
	cmd.Env = append(r.Auth.Filter(cliEnv(os.Environ())), "DISABLE_AUTOUPDATER=1")
	cmd.WaitDelay = killGrace
	out, err := cmd.Output()
	if err != nil {
		return false
	}
	for _, field := range strings.Fields(string(out)) {
		if strings.TrimRight(field, ",=") == flag {
			return true
		}
	}
	return false
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
	for _, d := range p.PluginDirs {
		args = append(args, PluginDirFlag, d)
	}
	for _, c := range p.MCPConfig {
		args = append(args, "--mcp-config", c)
	}
	if p.StrictMCPConfig {
		args = append(args, "--strict-mcp-config")
	}
	if len(p.SettingSources) > 0 {
		args = append(args, "--setting-sources", strings.Join(p.SettingSources, ","))
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

// cliEnvNames and cliEnvPrefixes are what the CLI keeps from the worker's
// environment beyond what any subprocess keeps (subproc.Env): the CLI's own
// settings, and the Go toolchain's, named as exec names them.
var (
	cliEnvNames    = append([]string{"CLAUDE_CONFIG_DIR", "NODE_EXTRA_CA_CERTS", OAuthTokenEnv}, subproc.GoToolchainNames...)
	cliEnvPrefixes = []string{"ANTHROPIC_"}
)

// cliEnv filters environ down to what the CLI may see. The worker holds
// credentials the run must not: DATABASE_URL first, with full rights on the
// platform's database. And the CLI hands its environment down to every shell
// it opens, so whatever it receives, the run can read.
func cliEnv(environ []string) []string {
	return subproc.Env(environ, cliEnvNames, cliEnvPrefixes)
}
