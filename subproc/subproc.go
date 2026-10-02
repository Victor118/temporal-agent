// Package subproc holds what every command the worker starts on behalf of a
// model shares: the environment it may inherit, and how it is stopped.
//
// The worker's environment holds the platform's credentials — DATABASE_URL
// with full rights on the database, the LLM key, SMTP, Telegram. A command an
// LLM chose, or a coding run with a shell, must not read them from its own
// environment; and a command that outlives its timeout must not leave its
// children running behind it.
package subproc

import (
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// baseNames is what any process needs to run, and nothing that authenticates
// anywhere.
var baseNames = []string{
	"PATH", "HOME", "USER", "LOGNAME", "SHELL", "TERM", "LANG", "TZ", "TMPDIR",
	"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy",
}

var basePrefixes = []string{"LC_"}

// GoToolchainNames are the Go toolchain's variables, for a command that builds
// Go code: without them `go build` fetches modules, or a whole toolchain, all
// over again. Named one by one rather than by a "GO" prefix, which would also
// let GOOGLE_* credentials through.
var GoToolchainNames = []string{
	"GOPATH", "GOROOT", "GOCACHE", "GOMODCACHE", "GOTOOLCHAIN", "GOFLAGS", "GOENV",
	"GOPROXY", "GOPRIVATE", "GONOPROXY", "GONOSUMDB", "GOSUMDB", "GOINSECURE",
}

// Env filters environ down to the base variables, plus the names and name
// prefixes a caller adds. Anything else stays out, whatever an operator adds
// to the worker later: the list says what is kept, not what is removed.
func Env(environ []string, names, prefixes []string) []string {
	keep := make(map[string]bool, len(baseNames)+len(names))
	for _, n := range baseNames {
		keep[n] = true
	}
	for _, n := range names {
		keep[n] = true
	}
	prefixes = append(append([]string(nil), basePrefixes...), prefixes...)

	var kept []string
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if name != "" && (keep[name] || hasAnyPrefix(name, prefixes)) {
			kept = append(kept, kv)
		}
	}
	return kept
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// KillGroupOnCancel makes cmd the leader of its own process group, and its
// cancellation signal the whole group. Without it, cancelling a command only
// reaches the shell: a build or a server it started keeps running after the
// activity is gone. grace bounds how long Wait then waits for the pipes a
// straggler may still hold. cmd must come from exec.CommandContext, the only
// kind os/exec cancels. Once cmd has been waited for, KillGroup ends what is
// left of the group.
func KillGroupOnCancel(cmd *exec.Cmd, sig syscall.Signal, grace time.Duration) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, sig) }
	cmd.WaitDelay = grace
}

// KillGroup kills what is left of cmd's process group once cmd has been
// waited for, however it ended: what the command started in the background
// (`server &`, a watcher) and left running when it returned. Cancellation
// alone only covers a command that did not return.
//
// The group is named by the leader's pid, which Wait has just released. While
// anything is left in the group, the pid still names it and is not handed out
// again; once nothing is, it could only name another group after the whole
// pid range went round. cmd must have been started with KillGroupOnCancel; a
// cmd that never started is left alone.
func KillGroup(cmd *exec.Cmd) {
	if cmd.Process == nil || cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setpgid {
		return
	}
	syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
}
