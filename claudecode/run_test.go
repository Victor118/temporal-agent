package claudecode

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"

	"github.com/victor/temporal-agent/subproc"
	"github.com/victor/temporal-agent/subproc/subproctest"
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
{"type":"system","subtype":"init","session_id":"sess-1","model":"claude-opus-5","cwd":"/w","apiKeySource":"none"}
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
	if res.SessionID != "sess-1" || res.Model != "claude-opus-5" || res.APIKeySource != "none" {
		t.Errorf("SessionID = %q, Model = %q, APIKeySource = %q", res.SessionID, res.Model, res.APIKeySource)
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

// An interrupted run never reaches the CLI's result line, so its duration has
// to come from the wall clock. Reporting "0s" to someone diagnosing a timeout
// is worse than reporting nothing.
func TestInterruptedRunReportsHowLongItRan(t *testing.T) {
	r := &Runner{Binary: fakeCLI(t, "echo '{\"type\":\"system\",\"subtype\":\"init\",\"session_id\":\"s\"}'\nsleep 60\n")}
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	defer cancel()

	res, err := r.Run(ctx, Params{Cwd: t.TempDir(), Task: "x"})
	if err == nil {
		t.Fatal("expected an interruption error")
	}
	if res.DurationMS < 1000 {
		t.Errorf("DurationMS = %d, want the wall time the run actually took", res.DurationMS)
	}
	if strings.Contains(err.Error(), "after 0s") {
		t.Errorf("error should say how long the run lasted, got: %v", err)
	}
}

// The run inherits the CLI's environment, so the worker's secrets must not
// reach it; what the CLI needs to work must.
func TestRunKeepsWorkerSecretsOut(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://agent:secret@db/agent")
	t.Setenv("CLAUDE_CODE_SSH_KEY", "/keys/id")
	t.Setenv("ANTHROPIC_API_KEY", "sk-test")
	t.Setenv("CLAUDE_CONFIG_DIR", "/config")

	script := `
printf '{"type":"result","subtype":"success","is_error":false,"result":"db=%s key=%s api=%s config=%s extra=%s home=%s","session_id":"s"}\n' \
  "$DATABASE_URL" "$CLAUDE_CODE_SSH_KEY" "$ANTHROPIC_API_KEY" "$CLAUDE_CONFIG_DIR" "$EXTRA" "$HOME"
`
	res, err := run(t, script, Params{Env: []string{"EXTRA=given"}})
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("db= key= api=sk-test config=/config extra=given home=%s", os.Getenv("HOME"))
	if res.Report != want {
		t.Errorf("CLI saw %q\nwant      %q", res.Report, want)
	}
}

func TestCLIEnv(t *testing.T) {
	got := cliEnv([]string{
		"PATH=/bin", "HOME=/root", "LC_ALL=C", "ANTHROPIC_BASE_URL=http://proxy", "GOPATH=/go",
		"DATABASE_URL=postgres://x", "LLM_API_KEY=k", "TEMPORAL_HOST=t", "PATHX=no", "=weird",
		"GOOGLE_APPLICATION_CREDENTIALS=/keys/gcp.json", "GOOGLE_API_KEY=g",
	})
	want := []string{"PATH=/bin", "HOME=/root", "LC_ALL=C", "ANTHROPIC_BASE_URL=http://proxy", "GOPATH=/go"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("cliEnv = %v, want %v", got, want)
	}
}

// With RunAs, the CLI and the shells it opens run as that user, at home in
// its own HOME rather than the worker's.
func TestRunAsAnotherUser(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("switching users takes root")
	}
	id := subproctest.Identity(t)
	bin := filepath.Join(subproctest.Dir(t, nil), "fake-claude")
	script := `printf '{"type":"result","subtype":"success","is_error":false,"result":"uid=%s home=%s","session_id":"s"}\n' "$(id -u)" "$HOME"`
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}

	r := &Runner{Binary: bin, RunAs: id, Runs: subproc.NewRuns(id)}
	res, err := r.Run(context.Background(), Params{Cwd: subproctest.Dir(t, id), Task: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("uid=%d home=%s", subproctest.UID, id.Home); res.Report != want {
		t.Errorf("CLI reported %q, want %q", res.Report, want)
	}
}

// What the CLI's shells start in the background dies with the run, even one
// that ends well: a dev server, a watcher, one that left the process group.
func TestRunLeavesNothingRunning(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("switching users takes root")
	}
	id := subproctest.Identity(t)
	bin := filepath.Join(subproctest.Dir(t, nil), "fake-claude")
	script := `sleep 300 >/dev/null 2>&1 &
setsid sleep 301 >/dev/null 2>&1 < /dev/null &
printf '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}\n'`
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}

	r := &Runner{Binary: bin, RunAs: id, Runs: subproc.NewRuns(id)}
	if _, err := r.Run(context.Background(), Params{Cwd: subproctest.Dir(t, id), Task: "x"}); err != nil {
		t.Fatal(err)
	}
	subproctest.NoProcessLeft(t, id.UID)
}

// A process the CLI leaves running with its stdout and stderr still open does
// not hold the run until the activity's timeout: Run returns killGrace after
// the CLI exits, with what it reported, and the process is gone.
func TestRunReturnsWhenTheOutputIsLeftOpen(t *testing.T) {
	defer func(d time.Duration) { killGrace = d }(killGrace)
	killGrace = 500 * time.Millisecond
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	script := fmt.Sprintf(`sleep 300 &
echo $! > %q
printf '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}\n'
`, pidFile)

	start := time.Now()
	res, err := run(t, script, Params{Cwd: dir})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("Run took %s, held by the output left open", took.Round(time.Millisecond))
	}
	if res.Report != "done" || res.Subtype != "success" {
		t.Errorf("result = %+v, want what the CLI reported", res)
	}
	pid := readPID(t, pidFile)
	deadline := time.Now().Add(5 * time.Second)
	for processRunning(pid) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if processRunning(pid) {
		syscall.Kill(pid, syscall.SIGKILL)
		t.Errorf("process %d, which held the output, outlived the run", pid)
	}
}

// A CLI run as another user without a count of the runs is refused: what it
// left running would never be ended.
func TestRunRefusesAnIdentityWithoutRuns(t *testing.T) {
	r := &Runner{Binary: fakeCLI(t, successStream), RunAs: &subproc.Identity{UID: 10001, GID: 10001}}
	if _, err := r.Run(context.Background(), Params{Cwd: t.TempDir(), Task: "x"}); err == nil || !strings.Contains(err.Error(), "Runner.Runs") {
		t.Errorf("err = %v, want a refusal", err)
	}
}

// A CLI may write nothing for minutes and be fine: a long Bash command, a
// long message being written. The run heartbeats on a timer all the same,
// with how far it got. Here the CLI waits for that heartbeat, with its tool
// call in it, before it writes anything more: beating on its lines alone,
// the run would wait in vain.
func TestRunHeartbeatsWhileTheCLIIsSilent(t *testing.T) {
	dir := t.TempDir()
	flag := filepath.Join(dir, "beaten")
	script := fmt.Sprintf(`
echo '{"type":"system","subtype":"init","session_id":"s"}'
echo '{"type":"assistant","message":{"content":[{"type":"tool_use","id":"tu_1","name":"Bash","input":{"command":"make"}}]},"session_id":"s"}'
i=0
while [ ! -e %q ]; do
  i=$((i+1)); [ $i -gt 400 ] && exit 3
  sleep 0.05
done
echo '{"type":"result","subtype":"success","is_error":false,"result":"built","session_id":"s"}'
`, flag)
	r := &Runner{Binary: fakeCLI(t, script), HeartbeatEvery: time.Second}

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	runCLI := func(ctx context.Context) (Result, error) {
		return r.Run(ctx, Params{Cwd: dir, Task: "build it"})
	}
	env.RegisterActivity(runCLI)
	env.SetOnActivityHeartbeatListener(func(_ *activity.Info, details converter.EncodedValues) {
		var p Progress
		if err := details.Get(&p); err != nil {
			t.Errorf("heartbeat details: %v", err)
			return
		}
		if p.ToolCalls == 1 && p.LastTool == "Bash" {
			os.WriteFile(flag, nil, 0o644)
		}
	})

	val, err := env.ExecuteActivity(runCLI)
	if err != nil {
		t.Fatalf("run: %v (no heartbeat while the CLI was silent?)", err)
	}
	var res Result
	if err := val.Get(&res); err != nil {
		t.Fatal(err)
	}
	if res.Report != "built" {
		t.Errorf("Report = %q", res.Report)
	}
	if res.Progress.ToolCalls != 1 || res.Progress.LastTool != "Bash" || res.Progress.Events != 3 {
		t.Errorf("Progress = %+v, want 3 events, 1 tool call, Bash last", res.Progress)
	}
}

// A CLI that writes nothing for StallTimeout is stuck, or waits on what
// never comes: its process group is ended, as on a cancellation, and the run
// says so, with how far it got.
func TestRunEndsAStalledCLI(t *testing.T) {
	defer func(d time.Duration) { killGrace = d }(killGrace)
	killGrace = 500 * time.Millisecond
	dir := t.TempDir()
	pidFile := filepath.Join(dir, "child.pid")
	script := fmt.Sprintf(`
echo '{"type":"system","subtype":"init","session_id":"s"}'
echo '{"type":"assistant","message":{"content":[{"type":"tool_use","id":"tu_1","name":"Bash","input":{"command":"sleep 60"}}]},"session_id":"s"}'
sleep 60 &
echo $! > %q
echo 'API overloaded, retrying' >&2
wait
`, pidFile)
	r := &Runner{Binary: fakeCLI(t, script), StallTimeout: 700 * time.Millisecond}

	start := time.Now()
	res, err := r.Run(context.Background(), Params{Cwd: dir, Task: "x"})
	var stall *StallError
	if !errors.As(err, &stall) {
		t.Fatalf("err = %v, want a StallError", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("the stalled run took %s to end", took.Round(time.Millisecond))
	}
	if stall.Silence != 700*time.Millisecond || !strings.Contains(err.Error(), "wrote nothing for 700ms") ||
		!strings.Contains(err.Error(), "API overloaded, retrying") {
		t.Errorf("err = %v (silence %s)", err, stall.Silence)
	}
	for _, p := range []Progress{stall.Progress, res.Progress} {
		if p.Events != 2 || p.ToolCalls != 1 || p.LastTool != "Bash" || p.SessionID != "s" {
			t.Errorf("Progress = %+v, want 2 events, 1 tool call, Bash last", p)
		}
	}
	if res.DurationMS < 700 {
		t.Errorf("DurationMS = %d, want the time the run lasted", res.DurationMS)
	}

	pid := readPID(t, pidFile)
	deadline := time.Now().Add(5 * time.Second)
	for processRunning(pid) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if processRunning(pid) {
		syscall.Kill(pid, syscall.SIGKILL)
		t.Errorf("process %d of the stalled CLI outlived the run", pid)
	}
}

// Silence is counted from the CLI's last line, not from the start: a run
// longer than StallTimeout that keeps writing is never taken for stuck.
func TestRunThatKeepsWritingIsNotStalled(t *testing.T) {
	script := `
for i in 1 2 3 4 5 6; do
  echo '{"type":"rate_limit_event","session_id":"s"}'
  sleep 0.2
done
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}'
`
	r := &Runner{Binary: fakeCLI(t, script), StallTimeout: 600 * time.Millisecond}
	res, err := r.Run(context.Background(), Params{Cwd: t.TempDir(), Task: "x"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Report != "done" || res.Progress.Events != 7 {
		t.Errorf("Report = %q, Progress = %+v", res.Report, res.Progress)
	}
}

// A CLI that went silent after its result line is not stuck: the run is
// whole, whatever the stall check did as the CLI exited.
func TestRunWithAResultIsNotStalled(t *testing.T) {
	defer func(d time.Duration) { killGrace = d }(killGrace)
	killGrace = 500 * time.Millisecond
	script := `
echo '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"s"}'
sleep 30
`
	r := &Runner{Binary: fakeCLI(t, script), StallTimeout: 300 * time.Millisecond}
	start := time.Now()
	res, err := r.Run(context.Background(), Params{Cwd: t.TempDir(), Task: "x"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Report != "done" {
		t.Errorf("Report = %q", res.Report)
	}
	// The silent CLI was ended all the same.
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("Run took %s", took.Round(time.Millisecond))
	}
}

// A negative StallTimeout turns the stall check off; a positive one fires
// on a CLI silent that long.
func TestMonitorStallCheck(t *testing.T) {
	for _, c := range []struct {
		timeout time.Duration
		stalls  bool
	}{{-1, false}, {50 * time.Millisecond, true}} {
		r := &Runner{StallTimeout: c.timeout}
		w := newWatch(time.Now().Add(-time.Hour))
		stalled := make(chan error, 1)
		done, returned := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(returned)
			r.monitor(context.Background(), w, func(err error) { stalled <- err }, done)
		}()
		select {
		case err := <-stalled:
			if !c.stalls {
				t.Errorf("timeout %s: stalled (%v)", c.timeout, err)
			}
		case <-time.After(300 * time.Millisecond):
			if c.stalls {
				t.Errorf("timeout %s: no stall", c.timeout)
			}
		}
		close(done)
		<-returned
	}
}
