package subproc

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestEnv(t *testing.T) {
	got := Env([]string{
		"PATH=/bin", "HOME=/root", "LC_ALL=C", "DATABASE_URL=postgres://x", "LLM_API_KEY=k",
		"SMTP_PASSWORD=p", "GOPATH=/go", "ANTHROPIC_API_KEY=a", "PATHX=no", "=weird",
		"GOOGLE_APPLICATION_CREDENTIALS=/keys/gcp.json", "GOCACHE=/cache",
	}, GoToolchainNames, nil)
	want := []string{"PATH=/bin", "HOME=/root", "LC_ALL=C", "GOPATH=/go", "GOCACHE=/cache"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Errorf("Env = %v, want %v", got, want)
	}

	got = Env([]string{"ANTHROPIC_API_KEY=a", "PATH=/bin"}, nil, []string{"ANTHROPIC_"})
	if strings.Join(got, " ") != "ANTHROPIC_API_KEY=a PATH=/bin" {
		t.Errorf("Env with a prefix = %v", got)
	}
}

// A cancelled command takes its children with it.
func TestKillGroupOnCancel(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", "sleep 30 & echo $! > child.pid; wait")
	cmd.Dir = dir
	KillGroupOnCancel(cmd, syscall.SIGKILL, time.Second)
	_ = cmd.Run()

	raw, err := os.ReadFile(dir + "/child.pid")
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
	if alive(pid) {
		t.Errorf("the command's child %d survived the cancellation", pid)
	}
}

// alive reports whether pid runs: neither gone, nor a zombie waiting for a
// parent that does not reap (PID 1 in a container often does not).
func alive(pid int) bool {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !runs(pid) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}

// runs reports whether pid runs now: neither gone nor a zombie.
func runs(pid int) bool {
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	// "pid (comm) S ...": the state follows the closing parenthesis.
	i := strings.LastIndexByte(string(stat), ')')
	return i < 0 || !strings.HasPrefix(string(stat[i+1:]), " Z")
}

// Under NewSession, KillGroup still ends what the command left running.
func TestKillGroup_NewSession(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "pid")
	cmd := exec.CommandContext(context.Background(), "sh", "-c", "sleep 60 & echo $! > "+pidFile+"; exit 0")
	KillGroupOnCancel(cmd, syscall.SIGTERM, time.Second)
	NewSession(cmd)
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
	if pid <= 0 || syscall.Kill(pid, 0) != nil {
		t.Fatalf("the sleep %d did not start", pid)
	}
	KillGroup(cmd)
	deadline := time.Now().Add(5 * time.Second)
	for {
		// Killed: gone, or a zombie waiting for its reaper.
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		if err != nil || strings.Contains(string(data), ") Z ") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the sleep %d outlived KillGroup", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
