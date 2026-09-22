package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/victor/temporal-agent/claudecode"
)

// claudeCodeRunCmd exercises the Claude Code runner outside Temporal. It is a
// debugging tool, not a product surface: it is how you check that the CLI is
// installed, authenticated, and that its stream parses, without going through
// a workflow or spending a single LLM token on the agent side.
var claudeCodeRunCmd = &cobra.Command{
	Use:   "claude-code-run",
	Short: "Run the Claude Code CLI once and print the result (debugging)",
	Long: "Run the Claude Code CLI once in a directory and print what it did.\n\n" +
		"The task is read from --task, from --task-file, or from stdin.\n" +
		"Progress goes to stderr, the final report to stdout.",
	// A run that fails is not a usage error: don't bury the CLI's own
	// diagnosis under a wall of flag help. main already reports the error,
	// so cobra doesn't print it a second time either.
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE:          runClaudeCodeRun,
}

var claudeCodeFlags struct {
	cwd                string
	task               string
	taskFile           string
	model              string
	permissionMode     string
	permissionPrompts  string
	allowedTools       []string
	disallowedTools    []string
	tools              []string
	appendSystemPrompt string
	addDirs            []string
	mcpConfig          []string
	strictMCPConfig    bool
	maxBudgetUSD       float64
	sessionID          string
	persistSession     bool
	env                []string
	binary             string
	timeout            time.Duration
	jsonOut            bool
	quiet              bool
}

func init() {
	f := claudeCodeRunCmd.Flags()
	f.StringVar(&claudeCodeFlags.cwd, "cwd", ".", "Working directory for the run")
	f.StringVar(&claudeCodeFlags.task, "task", "", "Task to give Claude Code")
	f.StringVar(&claudeCodeFlags.taskFile, "task-file", "", "Read the task from this file")
	f.StringVar(&claudeCodeFlags.model, "model", "", "Model (alias or full name)")
	f.StringVar(&claudeCodeFlags.permissionMode, "permission-mode", "", "plan, acceptEdits, bypassPermissions…")
	f.StringVar(&claudeCodeFlags.permissionPrompts, "permission-prompts", "", `Who answers permission prompts (default "none")`)
	f.StringSliceVar(&claudeCodeFlags.allowedTools, "allowed-tools", nil, "Tools to allow")
	f.StringSliceVar(&claudeCodeFlags.disallowedTools, "disallowed-tools", nil, "Tools to deny")
	f.StringSliceVar(&claudeCodeFlags.tools, "tools", nil, "Restrict the built-in tool set")
	f.StringVar(&claudeCodeFlags.appendSystemPrompt, "append-system-prompt", "", "Text appended to the CLI's system prompt")
	f.StringSliceVar(&claudeCodeFlags.addDirs, "add-dir", nil, "Extra directories the CLI may touch")
	f.StringSliceVar(&claudeCodeFlags.mcpConfig, "mcp-config", nil, "MCP server config (JSON string or file path)")
	f.BoolVar(&claudeCodeFlags.strictMCPConfig, "strict-mcp-config", false, "Ignore MCP servers other than --mcp-config")
	f.Float64Var(&claudeCodeFlags.maxBudgetUSD, "max-budget-usd", 0, "Stop the run past this API spend")
	f.StringVar(&claudeCodeFlags.sessionID, "session-id", "", "Pin the CLI session id (UUID)")
	f.BoolVar(&claudeCodeFlags.persistSession, "persist-session", false, "Let the CLI save the transcript to disk")
	f.StringArrayVar(&claudeCodeFlags.env, "env", nil, "Extra environment entry KEY=value (repeatable)")
	f.StringVar(&claudeCodeFlags.binary, "binary", "", "Path to the claude binary (default: CLAUDE_CODE_BIN or claude)")
	f.DurationVar(&claudeCodeFlags.timeout, "timeout", 30*time.Minute, "Abort the run after this long")
	f.BoolVar(&claudeCodeFlags.jsonOut, "json", false, "Print the full result as JSON instead of the report")
	f.BoolVar(&claudeCodeFlags.quiet, "quiet", false, "Don't stream progress to stderr")
}

func runClaudeCodeRun(cmd *cobra.Command, args []string) error {
	task, err := resolveTask(cmd.InOrStdin())
	if err != nil {
		return err
	}

	// Ctrl-C must reach the CLI and everything it spawned — the same path an
	// activity cancellation takes.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if claudeCodeFlags.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, claudeCodeFlags.timeout)
		defer cancel()
	}

	runner := &claudecode.Runner{Binary: claudeCodeFlags.binary}
	if !claudeCodeFlags.quiet {
		start := time.Now()
		runner.OnEvent = func(ev claudecode.Event) {
			if line := formatEvent(ev); line != "" {
				fmt.Fprintf(os.Stderr, "%7.1fs  %s\n", time.Since(start).Seconds(), line)
			}
		}
	}

	res, err := runner.Run(ctx, claudecode.Params{
		Cwd:                  claudeCodeFlags.cwd,
		Task:                 task,
		Model:                claudeCodeFlags.model,
		PermissionMode:       claudeCodeFlags.permissionMode,
		PermissionPrompts:    claudeCodeFlags.permissionPrompts,
		AllowedTools:         claudeCodeFlags.allowedTools,
		DisallowedTools:      claudeCodeFlags.disallowedTools,
		Tools:                claudeCodeFlags.tools,
		AppendSystemPrompt:   claudeCodeFlags.appendSystemPrompt,
		AddDirs:              claudeCodeFlags.addDirs,
		MCPConfig:            claudeCodeFlags.mcpConfig,
		StrictMCPConfig:      claudeCodeFlags.strictMCPConfig,
		MaxBudgetUSD:         claudeCodeFlags.maxBudgetUSD,
		SessionID:            claudeCodeFlags.sessionID,
		NoSessionPersistence: !claudeCodeFlags.persistSession,
		Env:                  claudeCodeFlags.env,
	})
	if err != nil {
		return err
	}

	if claudeCodeFlags.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(res); err != nil {
			return err
		}
	} else {
		fmt.Println(res.Report)
		if !claudeCodeFlags.quiet {
			fmt.Fprintf(os.Stderr, "\n%s\n", summarize(res))
		}
	}

	if res.IsError {
		// The CLI ran but reported failure. Say so in the exit code rather
		// than as an error: the report above already says why, and cobra
		// would only add a redundant line.
		os.Exit(1)
	}
	return nil
}

func resolveTask(stdin io.Reader) (string, error) {
	switch {
	case claudeCodeFlags.task != "" && claudeCodeFlags.taskFile != "":
		return "", fmt.Errorf("use --task or --task-file, not both")
	case claudeCodeFlags.task != "":
		return claudeCodeFlags.task, nil
	case claudeCodeFlags.taskFile != "":
		b, err := os.ReadFile(claudeCodeFlags.taskFile)
		if err != nil {
			return "", fmt.Errorf("reading --task-file: %w", err)
		}
		return string(b), nil
	}

	// A terminal on stdin means nobody piped anything in: say what's missing
	// instead of sitting there looking hung.
	if f, ok := stdin.(*os.File); ok {
		if fi, err := f.Stat(); err == nil && fi.Mode()&os.ModeCharDevice != 0 {
			return "", fmt.Errorf("no task: pass --task, --task-file, or pipe it on stdin")
		}
	}

	b, err := io.ReadAll(stdin)
	if err != nil {
		return "", fmt.Errorf("reading task from stdin: %w", err)
	}
	if strings.TrimSpace(string(b)) == "" {
		return "", fmt.Errorf("no task: pass --task, --task-file, or pipe it on stdin")
	}
	return string(b), nil
}

func formatEvent(ev claudecode.Event) string {
	switch ev.Kind {
	case claudecode.EventInit:
		return "session started"
	case claudecode.EventText:
		return firstLine(ev.Text, 140)
	case claudecode.EventToolUse:
		return fmt.Sprintf("→ %s %s", ev.ToolName, firstLine(string(ev.ToolInput), 100))
	case claudecode.EventToolResult:
		if ev.IsError {
			return fmt.Sprintf("← %s failed", orUnknown(ev.ToolName))
		}
		return fmt.Sprintf("← %s ok", orUnknown(ev.ToolName))
	case claudecode.EventResult:
		return "done"
	}
	return ""
}

func summarize(res claudecode.Result) string {
	var b strings.Builder
	status := "ok"
	if res.IsError {
		status = "FAILED"
	}
	fmt.Fprintf(&b, "%s (%s) — %d turns, %s, $%.4f, exit %d",
		status, res.Subtype, res.NumTurns,
		(time.Duration(res.DurationMS) * time.Millisecond).Round(time.Millisecond),
		res.CostUSD, res.ExitCode)
	if len(res.ToolUses) > 0 {
		fmt.Fprintf(&b, "\ntools: %s", formatToolUses(res.ToolUses))
	}
	if len(res.PermissionDenials) > 0 {
		fmt.Fprintf(&b, "\ndenied: %s", strings.Join(res.PermissionDenials, "; "))
	}
	if res.SessionID != "" {
		fmt.Fprintf(&b, "\nsession: %s", res.SessionID)
	}
	return b.String()
}

func formatToolUses(uses map[string]int) string {
	parts := make([]string, 0, len(uses))
	for name, n := range uses {
		parts = append(parts, fmt.Sprintf("%s×%d", name, n))
	}
	slices.Sort(parts)
	return strings.Join(parts, ", ")
}

func firstLine(s string, max int) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i] + " …"
	}
	if len(s) > max {
		s = s[:max] + "…"
	}
	return s
}

func orUnknown(s string) string {
	if s == "" {
		return "tool"
	}
	return s
}
