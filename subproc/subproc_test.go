package subproc

import (
	"context"
	"os"
	"os/exec"
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
		stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		if err != nil {
			return false
		}
		// "pid (comm) S ...": the state follows the closing parenthesis.
		if i := strings.LastIndexByte(string(stat), ')'); i > 0 && strings.HasPrefix(string(stat[i+1:]), " Z") {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
	return true
}
