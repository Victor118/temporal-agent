package claudecode

import (
	"fmt"
	"strings"
)

// Auth is how the CLI of a run authenticates, the worker's choice
// (CLAUDE_CODE_AUTH): who pays for a run is the operator's decision, and the
// CLI must not make it by finding a credential lying in its environment.
type Auth string

const (
	// AuthAPI bills the Anthropic API: ANTHROPIC_API_KEY, required.
	AuthAPI Auth = "api"
	// AuthSubscription uses a Claude subscription: CLAUDE_CODE_OAUTH_TOKEN
	// (claude setup-token), or else the login in the CLI's configuration.
	// The cost a run reports is then an estimate, and the subscription's
	// usage limits are what bound it.
	AuthSubscription Auth = "subscription"
)

// OAuthTokenEnv holds a subscription's long-lived token. Unlike a login, it
// is never refreshed, so concurrent runs cannot retire each other's.
const OAuthTokenEnv = "CLAUDE_CODE_OAUTH_TOKEN"

// apiCredentialEnv are the variables through which the CLI bills the API.
var apiCredentialEnv = []string{"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"}

// ResolveAuth reads CLAUDE_CODE_AUTH (mode) against the worker's environment.
// Empty: the one credential present decides, the subscription's login when
// there is none; both present is refused, as the CLI would silently pick the
// API key. "api" without a key is refused too: the run could only fail.
func ResolveAuth(mode string, environ []string) (Auth, error) {
	hasKey := envValue(environ, "ANTHROPIC_API_KEY") != ""
	hasToken := envValue(environ, OAuthTokenEnv) != ""
	switch Auth(mode) {
	case AuthAPI:
		if !hasKey {
			return "", fmt.Errorf("CLAUDE_CODE_AUTH=api needs ANTHROPIC_API_KEY")
		}
		return AuthAPI, nil
	case AuthSubscription:
		return AuthSubscription, nil
	case "":
		switch {
		case hasKey && hasToken:
			return "", fmt.Errorf("both ANTHROPIC_API_KEY and %s are set: say which pays with CLAUDE_CODE_AUTH=api or subscription", OAuthTokenEnv)
		case hasKey:
			return AuthAPI, nil
		default:
			return AuthSubscription, nil
		}
	default:
		return "", fmt.Errorf("CLAUDE_CODE_AUTH=%q: want api or subscription", mode)
	}
}

// Filter removes from environ the credentials of the other mode, so the CLI
// has one way to authenticate: the API key never reaches a subscription run,
// nor the token an API run. The zero Auth keeps environ as it is.
func (a Auth) Filter(environ []string) []string {
	var drop []string
	switch a {
	case AuthAPI:
		drop = []string{OAuthTokenEnv}
	case AuthSubscription:
		drop = apiCredentialEnv
	default:
		return environ
	}
	kept := make([]string, 0, len(environ))
	for _, kv := range environ {
		name, _, _ := strings.Cut(kv, "=")
		if !contains(drop, name) {
			kept = append(kept, kv)
		}
	}
	return kept
}

// Describe says, for the worker's log, how its runs authenticate.
func (a Auth) Describe(environ []string) string {
	switch {
	case a == AuthAPI:
		return "bill the Anthropic API (ANTHROPIC_API_KEY)"
	case a == AuthSubscription && envValue(environ, OAuthTokenEnv) != "":
		return "use a Claude subscription (" + OAuthTokenEnv + "); the dollar cap is only an estimate there"
	case a == AuthSubscription:
		return "use the Claude subscription logged in to the CLI's configuration, if any; the dollar cap is only an estimate there"
	}
	return "authenticate as the CLI finds"
}

// CostNote tells the model calling a coding tool what a run costs, so it does
// not weigh the run against a cost it remembers but no longer applies.
func (a Auth) CostNote() string {
	switch a {
	case AuthAPI:
		return "Each run is billed to the Anthropic API, from a few cents to a few dollars."
	case AuthSubscription:
		return "Runs are paid by the operator's Claude subscription, not per call: they only count against its usage limits."
	}
	return ""
}

// Payer says what paid the run: the worker's Auth, when the CLI's
// apiKeySource agrees with it. A subscription run uses no API key ("none");
// an API run names where its key came from. "none" alone is not a
// subscription — the CLI also says it of a bearer token or a third-party cloud
// provider — so without the worker's Auth, or without the CLI's word, nothing
// is claimed. A disagreement claims nothing either, and is the error, for the
// log.
func (r Result) Payer() (Auth, error) {
	switch {
	case r.Auth == "" || r.APIKeySource == "":
		return "", nil
	case r.Auth == AuthSubscription && r.APIKeySource == "none":
		return AuthSubscription, nil
	case r.Auth == AuthAPI && r.APIKeySource != "none":
		return AuthAPI, nil
	}
	return "", fmt.Errorf("the worker authenticates runs as %q, but the CLI reports apiKeySource %q: what paid the run is not said", r.Auth, r.APIKeySource)
}

// envValue is name's value in environ, the last one winning as os/exec has it.
func envValue(environ []string, name string) string {
	value := ""
	for _, kv := range environ {
		if n, v, _ := strings.Cut(kv, "="); n == name {
			value = v
		}
	}
	return value
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
