package claudecode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeCLI writes an executable stand-in for the claude binary and returns its
// path. The tests exercise the parsing and the process handling, which is
// everything this package owns; what the real CLI decides to do is its own
// business and not something a unit test should need tokens for.
func fakeCLI(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-claude")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func run(t *testing.T, script string, p Params) (Result, error) {
	t.Helper()
	if p.Cwd == "" {
		p.Cwd = t.TempDir()
	}
	if p.Task == "" {
		p.Task = "do the thing"
	}
	r := &Runner{Binary: fakeCLI(t, script)}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return r.Run(ctx, p)
}

const successStream = `
cat <<'EOF'
{"type":"system","subtype":"init","session_id":"sess-1","model":"claude-opus-5","cwd":"/w"}
{"type":"assistant","message":{"model":"claude-opus-5","content":[{"type":"text","text":"Looking at the repo."}]},"session_id":"sess-1"}
{"type":"assistant","message":{"content":[{"type":"tool_use","id":"tu_1","name":"Bash","input":{"command":"go test ./..."}}]},"session_id":"sess-1"}
{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"tu_1","is_error":false}]},"session_id":"sess-1"}
{"type":"assistant","message":{"content":[{"type":"tool_use","id":"tu_2","name":"Bash","input":{"command":"git status"}}]},"session_id":"sess-1"}
{"type":"rate_limit_event","session_id":"sess-1"}
{"type":"result","subtype":"success","is_error":false,"result":"Tests pass.","session_id":"sess-1","num_turns":3,"duration_ms":4200,"total_cost_usd":0.0425,"terminal_reason":"completed","permission_denials":[]}
EOF
`

func TestRunParsesSuccessfulStream(t *testing.T) {
	var kinds []EventKind
	r := &Runner{Binary: fakeCLI(t, successStream)}
	r.OnEvent = func(ev Event) { kinds = append(kinds, ev.Kind) }

	res, err := r.Run(context.Background(), Params{Cwd: t.TempDir(), Task: "check the tests"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Report != "Tests pass." {
		t.Errorf("Report = %q, want %q", res.Report, "Tests pass.")
	}
	if res.IsError || res.Subtype != "success" {
		t.Errorf("IsError = %v, Subtype = %q", res.IsError, res.Subtype)
	}
	if res.SessionID != "sess-1" || res.Model != "claude-opus-5" {
		t.Errorf("SessionID = %q, Model = %q", res.SessionID, res.Model)
	}
	if res.NumTurns != 3 || res.DurationMS != 4200 || res.CostUSD != 0.0425 {
		t.Errorf("turns/duration/cost = %d/%d/%v", res.NumTurns, res.DurationMS, res.CostUSD)
	}
	if res.TerminalReason != "completed" || res.ExitCode != 0 {
		t.Errorf("TerminalReason = %q, ExitCode = %d", res.TerminalReason, res.ExitCode)
	}
	if got := res.ToolUses["Bash"]; got != 2 {
		t.Errorf("ToolUses[Bash] = %d, want 2", got)
	}

	want := []EventKind{EventInit, EventText, EventToolUse, EventToolResult, EventToolUse, EventOther, EventResult}
	if len(kinds) != len(want) {
		t.Fatalf("events = %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("events = %v, want %v", kinds, want)
		}
	}
}

func TestRunNamesToolResults(t *testing.T) {
	var named []string
	r := &Runner{Binary: fakeCLI(t, successStream)}
	r.OnEvent = func(ev Event) {
		if ev.Kind == EventToolResult {
			named = append(named, ev.ToolName)
		}
	}
	if _, err := r.Run(context.Background(), Params{Cwd: t.TempDir(), Task: "x"}); err != nil {
		t.Fatal(err)
	}
	// The result line only carries a tool_use_id; the runner maps it back.
	if len(named) != 1 || named[0] != "Bash" {
		t.Errorf("tool result names = %v, want [Bash]", named)
	}
}

// A run the CLI itself reports as failed is a Result, not a Go error: the
// caller is the one who decides whether a failed run is fatal.
func TestRunReportedFailureIsNotAnError(t *testing.T) {
	script := `
cat <<'EOF'
{"type":"result","subtype":"error_max_turns","is_error":true,"result":"Ran out of turns.","session_id":"s","num_turns":40,"permission_denials":[{"tool_name":"Bash","message":"denied by policy"}]}
EOF
exit 1
`
	res, err := run(t, script, Params{})
	if err != nil {
		t.Fatalf("Run returned an error for a reported failure: %v", err)
	}
	if !res.IsError || res.Subtype != "error_max_turns" {
		t.Errorf("IsError = %v, Subtype = %q", res.IsError, res.Subtype)
	}
	if res.Report != "Ran out of turns." {
		t.Errorf("Report = %q", res.Report)
	}
	if res.ExitCode != 1 {
		t.Errorf("ExitCode = %d, want 1", res.ExitCode)
	}
	if len(res.PermissionDenials) != 1 || !strings.Contains(res.PermissionDenials[0], "denied by policy") {
		t.Errorf("PermissionDenials = %v", res.PermissionDenials)
	}
}

// A CLI that dies before reporting is a genuine failure, and the stderr tail
// is the only diagnosis available — so it has to reach the caller.
func TestRunWithoutResultLineIsAnError(t *testing.T) {
	script := `
echo "Invalid API key · Please run /login" >&2
exit 1
`
	res, err := run(t, script, Params{})
	if err == nil {
		t.Fatal("expected an error when the CLI reports nothing")
	}
	if !strings.Contains(err.Error(), "Invalid API key") {
		t.Errorf("error should carry the stderr tail, got: %v", err)
	}
	if res.ExitCode != 1 {
		t.Errorf("ExitCode = %d, want 1", res.ExitCode)
	}
}

func TestRunMissingBinary(t *testing.T) {
	r := &Runner{Binary: filepath.Join(t.TempDir(), "nope")}
	if _, err := r.Run(context.Background(), Params{Cwd: t.TempDir(), Task: "x"}); err == nil {
		t.Fatal("expected an error for a missing binary")
	}
}

// The task goes in on stdin, not argv: it is arbitrary text of arbitrary
// length written by an LLM.
func TestRunPassesTaskOnStdin(t *testing.T) {
	script := `
task=$(cat)
printf '{"type":"result","subtype":"success","is_error":false,"result":"%s","session_id":"s"}\n' "$task"
`
	res, err := run(t, script, Params{Task: "implement the thing"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Report != "implement the thing" {
		t.Errorf("Report = %q, want the task echoed back", res.Report)
	}
}

// One tool result routinely passes bufio.Scanner's 64 KiB token limit. A
// reader that trips on it would abandon the rest of a long run.
func TestRunHandlesOversizedLine(t *testing.T) {
	script := `
python3 -c '
import json
big = {"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"tu_1","is_error":False,"content":"x"*300000}]},"session_id":"s"}
print(json.dumps(big))
print(json.dumps({"type":"result","subtype":"success","is_error":False,"result":"done","session_id":"s"}))
'
`
	res, err := run(t, script, Params{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Report != "done" {
		t.Errorf("Report = %q, want the line after the oversized one to be parsed", res.Report)
	}
}

// A line the runner can't make sense of must not sink the run.
func TestRunSkipsUnparseableLines(t *testing.T) {
	script := `
echo "Warning: something printed on stdout"
echo '{"type":"assistant","message":"not an object"}'
echo '{"type":"result","subtype":"success","is_error":false,"result":"fine","session_id":"s"}'
`
	res, err := run(t, script, Params{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Report != "fine" {
		t.Errorf("Report = %q", res.Report)
	}
}

// An interrupted run still carries what the assistant managed to say.
func TestReportFallsBackToAssistantTextWhenNoResultLine(t *testing.T) {
	script := `
echo '{"type":"assistant","message":{"content":[{"type":"text","text":"Partial finding."}]},"session_id":"s"}'
exit 3
`
	res, _ := run(t, script, Params{})
	if res.Report != "Partial finding." {
		t.Errorf("Report = %q, want the partial assistant text", res.Report)
	}
}

// Cancellation has to reach the CLI's children. Without the process group, a
// build or a test suite it launched keeps running in the container long after
// the activity that started it is gone.
func TestRunKillsTheProcessGroupOnCancel(t *testing.T) {
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	script := fmt.Sprintf(`
sleep 60 &
echo $! > %q
echo '{"type":"system","subtype":"init","session_id":"s"}'
wait
`, pidFile)

	r := &Runner{Binary: fakeCLI(t, script)}
	ctx, cancel := context.WithCancel(context.Background())

	started := make(chan struct{})
	r.OnEvent = func(ev Event) {
		if ev.Kind == EventInit {
			close(started)
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := r.Run(ctx, Params{Cwd: dir, Task: "x"})
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("fake CLI never started")
	}
	cancel()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "interrupted") {
			t.Errorf("Run error = %v, want an interruption", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	pid := readPID(t, pidFile)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !processRunning(pid) {
			return // the child is gone, which is the point
		}
		time.Sleep(50 * time.Millisecond)
	}
	syscall.Kill(pid, syscall.SIGKILL)
	t.Fatalf("child process %d outlived the cancelled run", pid)
}

// processRunning reports whether pid is a live process. A signal-0 probe is
// not enough: a killed orphan stays visible as a zombie until something reaps
// it, and in a container whose PID 1 is not an init, nothing ever does.
func processRunning(pid int) bool {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	// "pid (comm) state ..." — comm can hold spaces, so scan past its ')'.
	i := strings.LastIndexByte(string(stat), ')')
	if i < 0 || i+2 >= len(stat) {
		return false
	}
	return stat[i+2] != 'Z'
}

func readPID(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading child pid: %v", err)
	}
	var pid int
	if _, err := fmt.Sscan(strings.TrimSpace(string(b)), &pid); err != nil {
		t.Fatalf("parsing child pid %q: %v", b, err)
	}
	return pid
}

func TestRunValidatesParams(t *testing.T) {
	r := &Runner{Binary: fakeCLI(t, "exit 0")}
	file := filepath.Join(t.TempDir(), "f")
	os.WriteFile(file, nil, 0o644)

	cases := []struct {
		name   string
		params Params
	}{
		{"no cwd", Params{Task: "x"}},
		{"no task", Params{Cwd: t.TempDir()}},
		{"blank task", Params{Cwd: t.TempDir(), Task: "  \n "}},
		{"missing cwd", Params{Cwd: filepath.Join(t.TempDir(), "nope"), Task: "x"}},
		{"cwd is a file", Params{Cwd: file, Task: "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := r.Run(context.Background(), tc.params); err == nil {
				t.Error("expected an error")
			}
		})
	}
}
