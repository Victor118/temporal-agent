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
		"missing queue":   `tools: ["*"]`,
		"no tools listed": `queue: q`,
		"invalid tool":    "queue: q\ntools: [\"[bad\"]",
		"requires name":   "queue: q\ntools: [\"*\"]\nmcp:\n  - url: http://x",
		"duplicate mcp":   "queue: q\ntools: [\"*\"]\nmcp:\n  - {name: a, url: u}\n  - {name: a, url: u}",
	}
	for want, content := range cases {
		_, err := LoadWorkerConfig(writeFile(t, content))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: got error %v", want, err)
		}
	}
}
