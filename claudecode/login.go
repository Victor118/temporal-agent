package claudecode

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// A machine announces Claude Code only when a run could authenticate, which
// it finds out without a paid call (FindLogin): a login it cannot see, it
// does not claim.

// LoginStatus is what a machine says of its CLI: what « Mes machines » shows.
type LoginStatus string

const (
	LoginOK     LoginStatus = "ok"         // installed, a login found
	LoginAbsent LoginStatus = "absent"     // no CLI
	LoginNone   LoginStatus = "logged_out" // installed, no usable login
)

// keychainHasLogin reports a login the CLI keeps in the macOS keychain; a
// variable, for the tests. Elsewhere there is no keychain to ask.
var keychainHasLogin = func() bool {
	if runtime.GOOS != "darwin" {
		return false
	}
	// Without -w: the item's attributes, never its secret.
	return exec.Command("security", "find-generic-password", "-s", "Claude Code-credentials").Run() == nil
}

// ConfigDir is the CLI's configuration directory for environ: its
// CLAUDE_CONFIG_DIR, or ~/.claude.
func ConfigDir(environ []string, home string) string {
	if dir := envValue(environ, "CLAUDE_CONFIG_DIR"); dir != "" {
		return dir
	}
	return filepath.Join(home, ".claude")
}

// FindLogin says where a run would find its credentials under auth, and
// whether there is one, by looking, never by calling the API: the API key
// for AuthAPI; for a subscription, CLAUDE_CODE_OAUTH_TOKEN, the CLI's login
// file (.credentials.json of its configuration), or under macOS the
// keychain. The zero Auth is ResolveAuth's to settle first.
func FindLogin(auth Auth, environ []string, home string) (source string, ok bool) {
	switch auth {
	case AuthAPI:
		if envValue(environ, "ANTHROPIC_API_KEY") != "" {
			return "ANTHROPIC_API_KEY", true
		}
		return "", false
	case AuthSubscription:
		if envValue(environ, OAuthTokenEnv) != "" {
			return OAuthTokenEnv, true
		}
		path := filepath.Join(ConfigDir(environ, home), credentialsFile)
		if hasOAuthLogin(path) {
			return path, true
		}
		if keychainHasLogin() {
			return "macOS keychain", true
		}
	}
	return "", false
}

// hasOAuthLogin reports a login file with an access token in it.
func hasOAuthLogin(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil || len(data) > maxCredentialsBytes {
		return false
	}
	var creds struct {
		ClaudeAiOauth *struct {
			AccessToken  string `json:"accessToken"`
			RefreshToken string `json:"refreshToken"`
		} `json:"claudeAiOauth"`
	}
	return json.Unmarshal(data, &creds) == nil && creds.ClaudeAiOauth != nil &&
		(creds.ClaudeAiOauth.AccessToken != "" || creds.ClaudeAiOauth.RefreshToken != "")
}

// LoginStamp is when the login file last changed (zero: none): a machine
// that saw its runs refused waits for it to move (a new /login) before it
// announces Claude Code again.
func LoginStamp(environ []string, home string) time.Time {
	fi, err := os.Stat(filepath.Join(ConfigDir(environ, home), credentialsFile))
	if err != nil {
		return time.Time{}
	}
	return fi.ModTime()
}

// authFailureMarks are what the CLI writes when its credentials are refused.
var authFailureMarks = []string{
	"invalid api key", "run /login", "authentication_error", "token has expired",
	"not logged in", "invalid bearer token", "oauth token revoked",
}

// AuthFailed reports a run that ended because its credentials were refused
// (a login expired, a key revoked), as its result or its error says.
func AuthFailed(res Result, err error) bool {
	text := strings.ToLower(res.Stderr)
	if res.IsError || res.Subtype == "" {
		text += "\n" + strings.ToLower(res.Report)
	}
	if err != nil {
		text += "\n" + strings.ToLower(err.Error())
	}
	for _, mark := range authFailureMarks {
		if strings.Contains(text, mark) {
			return true
		}
	}
	return false
}
