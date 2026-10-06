package subproc

import (
	"slices"
	"testing"
)

func TestGitEnv(t *testing.T) {
	environ := []string{"HOME=/h", "DATABASE_URL=x", "SSH_AUTH_SOCK=/s"}
	worker := GitEnv(environ)
	for _, want := range []string{"HOME=/h", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0"} {
		if !slices.Contains(worker, want) {
			t.Errorf("worker's git env lacks %s: %v", want, worker)
		}
	}
	user := GitEnvUser(environ, "SSH_AUTH_SOCK")
	if !slices.Contains(user, "GIT_TERMINAL_PROMPT=0") || !slices.Contains(user, "SSH_AUTH_SOCK=/s") ||
		slices.Contains(user, "GIT_CONFIG_GLOBAL=/dev/null") || slices.Contains(user, "DATABASE_URL=x") {
		t.Errorf("user's git env: %v", user)
	}
	if !slices.Contains(GitProtocolArgs, "protocol.ext.allow=never") || !slices.Contains(GitProtocolArgs, "protocol.allow=never") {
		t.Errorf("protocols %v", GitProtocolArgs)
	}
}
