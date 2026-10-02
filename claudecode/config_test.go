package claudecode

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/victor/temporal-agent/subproc"
)

// A run's configuration starts from the operator's login, settings and
// instructions, and nothing else of it.
func TestSeedConfigDir(t *testing.T) {
	base, root := t.TempDir(), t.TempDir()
	for name, content := range map[string]string{
		"settings.json": `{"model":"m"}`, ".credentials.json": `{}`, "CLAUDE.md": "be nice",
		"history.jsonl": "old", "projects/p/transcript": "old",
	} {
		os.MkdirAll(filepath.Dir(filepath.Join(base, name)), 0o755)
		os.WriteFile(filepath.Join(base, name), []byte(content), 0o644)
	}
	dir := filepath.Join(root, "run.claude")
	os.MkdirAll(filepath.Join(dir, "left-by-an-earlier-attempt"), 0o755)

	if err := SeedConfigDir(dir, base, nil); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(dir)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	if len(names) != 3 || names[0] != ".credentials.json" || names[1] != "CLAUDE.md" || names[2] != "settings.json" {
		t.Errorf("seeded %v", names)
	}
	if data, _ := os.ReadFile(filepath.Join(dir, "settings.json")); string(data) != `{"model":"m"}` {
		t.Errorf("settings = %q", data)
	}

	if err := SeedConfigDir(dir, "", nil); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Errorf("no base, yet %d entries", len(entries))
	}
}

// A renewed login comes back to the operator's configuration; nothing else
// does, and never through a link, nor a login the operator did not have.
func TestKeepCredentials(t *testing.T) {
	base, dir := t.TempDir(), t.TempDir()
	creds := func(p string) string {
		data, _ := os.ReadFile(filepath.Join(p, ".credentials.json"))
		return string(data)
	}

	// No login in base: the run cannot add one.
	os.WriteFile(filepath.Join(dir, ".credentials.json"), []byte(`{"token":"run's"}`), 0o600)
	if err := KeepCredentials(dir, base, nil); err != nil || creds(base) != "" {
		t.Errorf("a login was added: %q, %v", creds(base), err)
	}

	os.WriteFile(filepath.Join(base, ".credentials.json"), []byte(`{"token":1}`), 0o600)
	os.WriteFile(filepath.Join(dir, ".credentials.json"), []byte(`{"token":2}`), 0o600)
	if err := KeepCredentials(dir, base, nil); err != nil || creds(base) != `{"token":2}` {
		t.Errorf("renewed login: %q, %v", creds(base), err)
	}

	os.WriteFile(filepath.Join(dir, ".credentials.json"), []byte(`not json`), 0o600)
	if err := KeepCredentials(dir, base, nil); err == nil || creds(base) != `{"token":2}` {
		t.Errorf("not JSON: %q, %v", creds(base), err)
	}

	outside := filepath.Join(t.TempDir(), "secret.json")
	os.WriteFile(outside, []byte(`{"secret":true}`), 0o600)
	os.Remove(filepath.Join(dir, ".credentials.json"))
	os.Symlink(outside, filepath.Join(dir, ".credentials.json"))
	if err := KeepCredentials(dir, base, nil); err == nil || creds(base) != `{"token":2}` {
		t.Errorf("through a link: %q, %v", creds(base), err)
	}

	os.Remove(filepath.Join(dir, ".credentials.json"))
	if err := KeepCredentials(dir, base, nil); err != nil || creds(base) != `{"token":2}` {
		t.Errorf("no login in the run: %q, %v", creds(base), err)
	}
}

// A run cannot hand back a login emptied of what the operator's holds — which
// would log every next run out — and with an API key, the CLI has no login to
// renew: nothing comes back at all.
func TestKeepCredentials_OnlyWhatARenewalProduces(t *testing.T) {
	base, dir := t.TempDir(), t.TempDir()
	login := `{"claudeAiOauth":{"accessToken":"a1","refreshToken":"r1","expiresAt":1,"scopes":["x"]}}`
	os.WriteFile(filepath.Join(base, ".credentials.json"), []byte(login), 0o600)
	creds := func() string {
		data, _ := os.ReadFile(filepath.Join(base, ".credentials.json"))
		return string(data)
	}
	keep := func(renewed string, environ []string) error {
		os.WriteFile(filepath.Join(dir, ".credentials.json"), []byte(renewed), 0o600)
		return KeepCredentials(dir, base, environ)
	}

	for _, emptied := range []string{
		`{}`, `[]`, `"x"`, `{"claudeAiOauth":{}}`, `{"claudeAiOauth":null}`,
		`{"claudeAiOauth":{"accessToken":"a2","refreshToken":"","expiresAt":2,"scopes":["x"]}}`,
		`{"claudeAiOauth":{"accessToken":"a2","expiresAt":2,"scopes":["x"]}}`,
	} {
		if err := keep(emptied, nil); err == nil || creds() != login {
			t.Errorf("%s: kept (%v), login now %q", emptied, err, creds())
		}
	}

	renewed := `{"claudeAiOauth":{"accessToken":"a2","refreshToken":"r2","expiresAt":2,"scopes":["x","y"],"subscriptionType":"max"}}`
	withKey := []string{"ANTHROPIC_API_KEY=sk-1", "PATH=/bin"}
	if err := keep(renewed, withKey); err != nil || creds() != login {
		t.Errorf("with an API key: kept (%v), login now %q", err, creds())
	}
	// An empty key, or one a later entry empties, is no key.
	if err := keep(renewed, append(withKey, "ANTHROPIC_API_KEY=")); err != nil || creds() != renewed {
		t.Errorf("a renewal: %v, login now %q", err, creds())
	}
}

// A run gets a configuration of its own when there is one to copy or a user
// of its own to give it to; otherwise the CLI keeps its default one, the
// caller's login in it.
func TestOwnConfig(t *testing.T) {
	someone := &subproc.Identity{UID: 10001, GID: 10001}
	for _, c := range []struct {
		base  string
		runAs *subproc.Identity
		want  bool
	}{
		{"", nil, false},
		{"/config", nil, true},
		{"", someone, true},
		{"/config", someone, true},
	} {
		if got := OwnConfig(c.base, c.runAs); got != c.want {
			t.Errorf("OwnConfig(%q, %v) = %v, want %v", c.base, c.runAs, got, c.want)
		}
	}
}
