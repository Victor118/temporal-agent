package tool

import (
	"context"
	"encoding/json"
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

// RegisterExecTool registers exec, which runs a shell command chosen by the
// model in the workspace.
//
// The command sees a filtered environment and its whole process group dies at
// the timeout. That is not a sandbox: the command runs as the worker's user,
// can leave the workspace and can read what that user can — the worker's own
// /proc/<pid>/environ included. Serve exec from a worker whose user and
// filesystem hold nothing the agent must not reach.
func RegisterExecTool(r *Registry, workspacePath string) {
	r.Register(&Tool{
		Name:        "exec",
		Description: "Execute a shell command in the workspace. The command runs with the workspace as the working directory.",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"command": {"type": "string", "description": "Shell command to execute"},
				"timeout_seconds": {"type": "integer", "description": "Timeout in seconds (default: 30, max: 300)"}
			},
			"required": ["command"]
		}`),
		Kind:      ToolKindActivity,
		Sensitive: true,
		Execute: func(ctx context.Context, input json.RawMessage) (string, error) {
			var params struct {
				Command        string `json:"command"`
				TimeoutSeconds int    `json:"timeout_seconds"`
			}
			if err := json.Unmarshal(input, &params); err != nil {
				return "", err
			}

			timeout := time.Duration(params.TimeoutSeconds) * time.Second
			if timeout <= 0 || timeout > 300*time.Second {
				timeout = 30 * time.Second
			}

			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			cmd := exec.CommandContext(ctx, "sh", "-c", params.Command)
			cmd.Dir = workspacePath
			cmd.Env = subproc.Env(os.Environ(), subproc.GoToolchainNames, nil)
			subproc.KillGroupOnCancel(cmd, syscall.SIGKILL, execKillGrace)

			output, err := cmd.CombinedOutput()
			result := string(output)

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
