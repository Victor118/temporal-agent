package claudecode

import (
	"strings"
	"testing"
)

// Who pays is settled from the configuration: one credential decides, both
// are refused unless the operator says which, and "api" without a key could
// only fail.
func TestResolveAuth(t *testing.T) {
	key, token := "ANTHROPIC_API_KEY=sk", OAuthTokenEnv+"=oat"
	for _, c := range []struct {
		mode    string
		environ []string
		want    Auth
		err     bool
	}{
		{"", nil, AuthSubscription, false},
		{"", []string{key}, AuthAPI, false},
		{"", []string{token}, AuthSubscription, false},
		{"", []string{key, token}, "", true},
		{"", []string{key, "ANTHROPIC_API_KEY="}, AuthSubscription, false}, // emptied: none
		{"api", []string{key, token}, AuthAPI, false},
		{"api", []string{token}, "", true},
		{"subscription", []string{key, token}, AuthSubscription, false},
		{"subscription", []string{key}, AuthSubscription, false},
		{"bedrock", nil, "", true},
	} {
		got, err := ResolveAuth(c.mode, c.environ)
		if got != c.want || (err != nil) != c.err {
			t.Errorf("ResolveAuth(%q, %v) = %q, %v; want %q, error %v", c.mode, c.environ, got, err, c.want, c.err)
		}
	}
}

// Each mode keeps the other's credential from the CLI.
func TestAuthFilter(t *testing.T) {
	environ := []string{"PATH=/bin", "ANTHROPIC_API_KEY=sk", "ANTHROPIC_AUTH_TOKEN=at", "ANTHROPIC_BASE_URL=http://proxy", OAuthTokenEnv + "=oat"}
	for auth, want := range map[Auth]string{
		AuthAPI:          "PATH=/bin ANTHROPIC_API_KEY=sk ANTHROPIC_AUTH_TOKEN=at ANTHROPIC_BASE_URL=http://proxy",
		AuthSubscription: "PATH=/bin ANTHROPIC_BASE_URL=http://proxy " + OAuthTokenEnv + "=oat",
		"":               strings.Join(environ, " "),
	} {
		if got := strings.Join(auth.Filter(environ), " "); got != want {
			t.Errorf("%q.Filter = %s\nwant %s", auth, got, want)
		}
	}
}

// A subscription run never sees the API key, even one left in the worker's
// environment; an API run never sees the subscription's token.
func TestRunAuthenticatesOneWay(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "sk-test")
	t.Setenv(OAuthTokenEnv, "oat-test")
	script := `
printf '{"type":"result","subtype":"success","is_error":false,"result":"api=%s token=%s","session_id":"s"}\n' "$ANTHROPIC_API_KEY" "$CLAUDE_CODE_OAUTH_TOKEN"
`
	for auth, want := range map[Auth]string{
		AuthSubscription: "api= token=oat-test",
		AuthAPI:          "api=sk-test token=",
	} {
		r := &Runner{Binary: fakeCLI(t, script), Auth: auth}
		res, err := r.Run(t.Context(), Params{Cwd: t.TempDir(), Task: "x"})
		if err != nil {
			t.Fatal(err)
		}
		if res.Report != want {
			t.Errorf("%s run: CLI saw %q, want %q", auth, res.Report, want)
		}
	}
}

// The coding tools tell the model what a run costs in the worker's mode.
func TestAuthCostNote(t *testing.T) {
	if !strings.Contains(AuthSubscription.CostNote(), "subscription") || !strings.Contains(AuthAPI.CostNote(), "billed") || Auth("").CostNote() != "" {
		t.Errorf("notes: %q / %q / %q", AuthSubscription.CostNote(), AuthAPI.CostNote(), Auth("").CostNote())
	}
}
