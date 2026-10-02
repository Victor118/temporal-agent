package claudecode

import (
	"os"
	"path/filepath"
	"testing"
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
	if err := KeepCredentials(dir, base); err != nil || creds(base) != "" {
		t.Errorf("a login was added: %q, %v", creds(base), err)
	}

	os.WriteFile(filepath.Join(base, ".credentials.json"), []byte(`{"token":1}`), 0o600)
	os.WriteFile(filepath.Join(dir, ".credentials.json"), []byte(`{"token":2}`), 0o600)
	if err := KeepCredentials(dir, base); err != nil || creds(base) != `{"token":2}` {
		t.Errorf("renewed login: %q, %v", creds(base), err)
	}

	os.WriteFile(filepath.Join(dir, ".credentials.json"), []byte(`not json`), 0o600)
	if err := KeepCredentials(dir, base); err == nil || creds(base) != `{"token":2}` {
		t.Errorf("not JSON: %q, %v", creds(base), err)
	}

	outside := filepath.Join(t.TempDir(), "secret.json")
	os.WriteFile(outside, []byte(`{"secret":true}`), 0o600)
	os.Remove(filepath.Join(dir, ".credentials.json"))
	os.Symlink(outside, filepath.Join(dir, ".credentials.json"))
	if err := KeepCredentials(dir, base); err == nil || creds(base) != `{"token":2}` {
		t.Errorf("through a link: %q, %v", creds(base), err)
	}

	os.Remove(filepath.Join(dir, ".credentials.json"))
	if err := KeepCredentials(dir, base); err != nil || creds(base) != `{"token":2}` {
		t.Errorf("no login in the run: %q, %v", creds(base), err)
	}
}
