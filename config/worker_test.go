package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "worker.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadWorkerConfig(t *testing.T) {
	p := writeFile(t, `
queue: tools-github
workflows: true
tools: ["github_*"]
mcp:
  - name: github
    url: http://mcp-github:3001
`)
	wc, err := LoadWorkerConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if wc.Queue != "tools-github" || !wc.Workflows || len(wc.Tools) != 1 || wc.MCP[0].Name != "github" {
		t.Errorf("unexpected config: %+v", wc)
	}
}

func TestLoadWorkerConfig_Invalid(t *testing.T) {
	cases := map[string]string{
		"missing queue":            `tools: ["*"]`,
		"no tools key":             `queue: q`,
		"invalid tool":             "queue: q\ntools: [\"[bad\"]",
		"requires name":            "queue: q\ntools: [\"*\"]\nmcp:\n  - url: http://x",
		"duplicate mcp":            "queue: q\ntools: [\"*\"]\nmcp:\n  - {name: a, url: u}\n  - {name: a, url: u}",
		"is not one of [http sse]": "queue: q\ntools: [\"*\"]\nmcp:\n  - {name: a, url: u, transport: websocket}",
	}
	for want, content := range cases {
		_, err := LoadWorkerConfig(writeFile(t, content))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got error %v", want, err)
		}
	}
}

// An explicit empty list publishes nothing, and loads.
func TestLoadWorkerConfig_NoTools(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.yaml")
	os.WriteFile(path, []byte("queue: q\nworkflows: false\ntools: []\n"), 0o644)
	wc, err := LoadWorkerConfig(path)
	if err != nil || wc.Tools == nil || len(wc.Tools) != 0 {
		t.Fatalf("%+v %v", wc, err)
	}
}

// The worker configs of the repository load: a container started with one
// would stop otherwise.
func TestLoadWorkerConfig_TheRepositorysFiles(t *testing.T) {
	files, err := filepath.Glob("../worker*.yaml")
	if err != nil || len(files) < 3 {
		t.Fatalf("worker configs: %v %v", files, err)
	}
	for _, f := range files {
		if _, err := LoadWorkerConfig(f); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}
