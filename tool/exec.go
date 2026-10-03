package tool

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/victor/temporal-agent/subproc"
)

// execKillGrace is how long a cancelled command's pipes are waited on once its
// process group is killed.
const execKillGrace = 2 * time.Second

// The time a command may take: what the model asks for, within the bounds.
// The tool's Timeout derives from the maximum: a command that runs to it
// returns its own timeout error, not Temporal's.
const (
	execDefaultTimeout = 30 * time.Second
	execMaxTimeout     = 300 * time.Second
)

// Holder counts a command while it runs, and once none runs, ends every
// process of the user it ran as (subproc.Runs).
type Holder interface {
	Hold() (release func())
}

// RegisterExecTool registers exec, which runs a shell command chosen by the
// model in the workspace, as runAs.
//
// The command sees a filtered environment and its whole process group dies
// when it returns or at the timeout, whichever comes first; once no command
// runs as runAs, every process of runAs's does (runs, shared with whatever
// else of the worker runs commands as runAs), one that left the group
// included. A runAs without runs refuses every command: what one left
// running would never be ended. Run as runAs, a user of its own, it cannot read the worker's
// /proc/<pid>/environ nor its 0600 files, which hold what the filtered
// environment leaves out. It is still not a sandbox: it can leave the
// workspace, and read whatever that user can. A worker running as root with
// no runAs refuses every command (subproc.CheckRunAs): they would run as root.
func RegisterExecTool(r *Registry, workspacePath string, runAs *subproc.Identity, runs Holder) {
	r.Register(&Tool{
		Name:        "exec",
		Description: "Execute a shell command in the workspace. The command runs with the workspace as the working directory. Processes it starts in the background are stopped when it returns.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"command": {"type": "string", "description": "Shell command to execute"},
				"timeout_seconds": {"type": "integer", "description": "Timeout in seconds (default: ` + fmt.Sprint(execDefaultTimeout.Seconds()) + `, max: ` + fmt.Sprint(execMaxTimeout.Seconds()) + `)"}
			},
			"required": ["command"]
		}`),
		Kind:      ToolKindActivity,
		Sensitive: true,
		Timeout:   execMaxTimeout + TimeoutMargin,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Command        string `json:"command"`
				TimeoutSeconds int    `json:"timeout_seconds"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}

			if err := subproc.CheckRunAs(runAs); err != nil {
				return "", fmt.Errorf("exec: %w", err)
			}
			if runAs != nil && runs == nil {
				return "", errors.New("exec: commands run as RUN_AS_UID need a count of them (subproc.Runs)")
			}

			timeout := time.Duration(params.TimeoutSeconds) * time.Second
			if timeout <= 0 || timeout > execMaxTimeout {
				timeout = execDefaultTimeout
			}

			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			cmd := exec.CommandContext(ctx, "sh", "-c", params.Command)
			cmd.Dir = workspacePath
			cmd.Env = runAs.Env(subproc.Env(os.Environ(), subproc.GoToolchainNames, nil))
			runAs.Apply(cmd)
			subproc.KillGroupOnCancel(cmd, syscall.SIGKILL, execKillGrace)

			release := hold(runs)
			output, err := cmd.CombinedOutput()
			subproc.KillGroup(cmd)
			release()
			result := string(output)

			// A process left in the background held the output open: the
			// command itself succeeded, and what it left is gone.
			if errors.Is(err, exec.ErrWaitDelay) {
				err = nil
				result += "\n(background processes do not outlive the command: they were stopped)"
			}

			if err != nil {
				if ctx.Err() == context.DeadlineExceeded {
					return "", fmt.Errorf("exec: command timed out after %s", timeout)
				}
				// Return error output to the LLM so it can reason about it
				return fmt.Sprintf("Command failed: %s\n%s", err.Error(), result), nil
			}

			if len(result) > 50000 {
				result = result[:50000] + "\n... (output truncated)"
			}

			return strings.TrimSpace(result), nil
		},
	})
}

// hold counts a command with runs, if there are any to count it with.
func hold(runs Holder) (release func()) {
	if runs == nil {
		return func() {}
	}
	return runs.Hold()
}
