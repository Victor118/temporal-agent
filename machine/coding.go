package machine

import (
	"fmt"
	"strings"
)

// Coding runs on a machine (phase 1: analyze_repo).
const (
	// CapClaudeCode: the machine has the claude CLI and a login it can use.
	CapClaudeCode = "claude-code"
	// KindAnalyzeRepo is a read-only analysis of a repository, run by the
	// machine's own CLI, with its owner's subscription or key.
	KindAnalyzeRepo = "analyze_repo"
)

// CapabilityOf is the capability a directive's kind needs.
func CapabilityOf(kind string) string {
	if kind == KindAnalyzeRepo {
		return CapClaudeCode
	}
	return kind
}

// What keeps an analysis read-only, on a worker (AnalyzeRepoWorkflow) as on
// a machine. Never taken from a directive: a machine applies them itself,
// whatever the server says, and so does the workflow, whatever the model
// asks.
const (
	AnalyzePermissionMode = "plan"
	// AnalyzeSystemPrompt tells the run what it is: the CLI otherwise
	// behaves as in an interactive session, and ends a report offering to
	// make the changes or asking what to do next — which no one will
	// answer, and which the agent reading the report repeats to its user as
	// a promise.
	AnalyzeSystemPrompt = `This is a one-shot, read-only analysis. Nothing you change is kept: the clone is deleted when you finish, and nothing can be committed or pushed.
Your final message is a report read by another agent, not by a person, and no one will reply to it. End with the report: do not offer to make changes, to start on fixes, or to continue, and do not ask questions.
If a command you need is refused, say that it was refused, not that a tool is missing.`
)

// maxAnalyzeTask bounds an analysis's task.
const maxAnalyzeTask = 64 << 10

// AnalyzeInput is an analyze_repo directive: what the calling model chose.
type AnalyzeInput struct {
	Repo string `json:"repo"`
	Ref  string `json:"ref,omitempty"`
	Task string `json:"task"`
}

// Check refuses what git or the CLI would read as something else: a
// repository or ref starting with a dash, control characters, no task.
func (in AnalyzeInput) Check() error {
	switch {
	case strings.TrimSpace(in.Repo) == "" || strings.TrimSpace(in.Task) == "":
		return fmt.Errorf("analyze_repo needs a repo and a task")
	case strings.HasPrefix(in.Repo, "-") || strings.ContainsAny(in.Repo, "\x00\r\n"):
		return fmt.Errorf("invalid repository %q", in.Repo)
	case strings.HasPrefix(in.Ref, "-") || strings.ContainsAny(in.Ref, "\x00\r\n"):
		return fmt.Errorf("invalid ref %q", in.Ref)
	case len(in.Task) > maxAnalyzeTask:
		return fmt.Errorf("task over %d bytes", maxAnalyzeTask)
	}
	return nil
}

// CodingOutput is what a machine's coding run returns: what the workflow
// needs to say what happened (workflow.ClaudeCodeOutput), nothing it would
// have to trust more than text. Error is a failure the machine reports
// (a repository it refuses, a clone that failed, a run that ended without
// the CLI's result); the report may still be there.
type CodingOutput struct {
	Report     string         `json:"report,omitempty"`
	Error      string         `json:"error,omitempty"`
	Commit     string         `json:"commit,omitempty"`
	IsError    bool           `json:"is_error,omitempty"`
	Subtype    string         `json:"subtype,omitempty"`
	NumTurns   int            `json:"num_turns,omitempty"`
	DurationMS int64          `json:"duration_ms,omitempty"`
	CostUSD    float64        `json:"cost_usd,omitempty"`
	PaidBy     string         `json:"paid_by,omitempty"`
	ToolUses   map[string]int `json:"tool_uses,omitempty"`
	// Interrupted: the run ended without the CLI's result (stopped, stuck,
	// cancelled); ToolCalls, LastTool and Events say how far it got.
	Interrupted bool   `json:"interrupted,omitempty"`
	ToolCalls   int    `json:"tool_calls,omitempty"`
	LastTool    string `json:"last_tool,omitempty"`
	Events      int    `json:"events,omitempty"`
}

// CloneProgress is a coding run's progress while it clones: the CLI has not
// started (nothing paid yet).
const CloneProgress = "clone"

// CodingProgress is a coding run's progress, as its user reads it on the
// turn's line: "34 outils (dernier : Grep)".
func CodingProgress(toolCalls int, lastTool string) string {
	s := fmt.Sprintf("%d outil", toolCalls)
	if toolCalls > 1 {
		s += "s"
	}
	if lastTool != "" {
		s += " (dernier : " + lastTool + ")"
	}
	return s
}
