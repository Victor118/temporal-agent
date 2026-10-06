package claudecode

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFindLogin(t *testing.T) {
	defer func(f func() bool) { keychainHasLogin = f }(keychainHasLogin)
	keychainHasLogin = func() bool { return false }
	home := t.TempDir()
	for _, c := range []struct {
		name    string
		auth    Auth
		environ []string
		want    bool
	}{
		{"api with its key", AuthAPI, []string{"ANTHROPIC_API_KEY=k"}, true},
		{"api without", AuthAPI, nil, false},
		{"subscription token", AuthSubscription, []string{OAuthTokenEnv + "=t"}, true},
		{"subscription, nothing", AuthSubscription, []string{"ANTHROPIC_API_KEY=k"}, false},
	} {
		if _, ok := FindLogin(c.auth, c.environ, home); ok != c.want {
			t.Errorf("%s: %v", c.name, ok)
		}
	}

	// The CLI's login file, in ~/.claude or CLAUDE_CONFIG_DIR.
	dir := filepath.Join(home, ".claude")
	os.MkdirAll(dir, 0o700)
	os.WriteFile(filepath.Join(dir, credentialsFile), []byte(`{"claudeAiOauth":{}}`), 0o600)
	if _, ok := FindLogin(AuthSubscription, nil, home); ok {
		t.Error("an empty login counts")
	}
	os.WriteFile(filepath.Join(dir, credentialsFile), []byte(`{"claudeAiOauth":{"accessToken":"a","refreshToken":"r"}}`), 0o600)
	if src, ok := FindLogin(AuthSubscription, nil, home); !ok || src != filepath.Join(dir, credentialsFile) {
		t.Errorf("login file: %q %v", src, ok)
	}
	if LoginStamp(nil, home).IsZero() {
		t.Error("no stamp")
	}
	other := t.TempDir()
	if _, ok := FindLogin(AuthSubscription, []string{"CLAUDE_CONFIG_DIR=" + other}, home); ok {
		t.Error("CLAUDE_CONFIG_DIR is not read")
	}
	if !LoginStamp([]string{"CLAUDE_CONFIG_DIR=" + other}, home).IsZero() {
		t.Error("stamp of a missing file")
	}
	keychainHasLogin = func() bool { return true }
	if src, ok := FindLogin(AuthSubscription, []string{"CLAUDE_CONFIG_DIR=" + other}, home); !ok || src != "macOS keychain" {
		t.Errorf("keychain: %q %v", src, ok)
	}
}

func TestAuthFailure(t *testing.T) {
	for _, c := range []struct {
		res  Result
		err  error
		want string
	}{
		{Result{IsError: true, Subtype: "success", Report: "Invalid API key · Please run /login"}, nil, "Invalid API key · Please run /login"},
		{Result{Stderr: "warming up\nOAuth token has expired. Please obtain a new token"}, errors.New("exited"), "OAuth token has expired. Please obtain a new token"},
		{Result{}, errors.New(`claudecode: CLI exited with exit status 1 and reported nothing: {"type":"authentication_error"}`), `claudecode: CLI exited with exit status 1 and reported nothing: {"type":"authentication_error"}`},
		// A run that answered: what a tool printed on stderr is not read.
		{Result{Subtype: "success", Report: "Done.", Stderr: "curl: Invalid API key"}, nil, ""},
		{Result{Subtype: "success", Report: "The login page uses an API key; run /login is in the docs"}, nil, ""},
		{Result{IsError: true, Subtype: "error_max_turns"}, nil, ""},
	} {
		if got := AuthFailure(c.res, c.err); got != c.want {
			t.Errorf("%+v %v: %q, want %q", c.res, c.err, got, c.want)
		}
	}
}
