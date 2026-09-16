package config

import (
	"fmt"
	"os"
	"path"

	"gopkg.in/yaml.v3"
)

// WorkerConfig describes what a worker process executes: the task queue it
// serves (a capability) and the tools it exposes there. All workers sharing a
// queue must expose the same tools; keeping that consistent is the operator's job.
type WorkerConfig struct {
	Queue     string            `yaml:"queue"`
	Tools     []string          `yaml:"tools"`     // Tool name globs exposed by this worker
	MCP       []WorkerMCPServer `yaml:"mcp"`       // MCP servers loaded by this worker
	Workflows bool              `yaml:"workflows"` // Also serve the workflow queue (sessions, agents, LLM calls)
}

// WorkerMCPServer is an MCP server loaded by this worker. Its tools are
// registered as "<name>_<tool>".
type WorkerMCPServer struct {
	Name      string `yaml:"name"`
	URL       string `yaml:"url"`
	APIKey    string `yaml:"api_key"`
	Transport string `yaml:"transport"` // "http" (default) or "sse"
}

// LoadWorkerConfig reads and validates a worker config file.
func LoadWorkerConfig(file string) (*WorkerConfig, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", file, err)
	}

	var wc WorkerConfig
	if err := yaml.Unmarshal(raw, &wc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", file, err)
	}

	if wc.Queue == "" {
		return nil, fmt.Errorf("%s: missing queue", file)
	}
	if len(wc.Tools) == 0 {
		return nil, fmt.Errorf("%s: no tools listed (use \"*\" to expose all)", file)
	}
	for _, g := range wc.Tools {
		if _, err := path.Match(g, ""); err != nil {
			return nil, fmt.Errorf("%s: invalid tool pattern %q: %w", file, g, err)
		}
	}

	seen := make(map[string]bool, len(wc.MCP))
	for i, m := range wc.MCP {
		if m.Name == "" || m.URL == "" {
			return nil, fmt.Errorf("%s: mcp[%d] requires name and url", file, i)
		}
		if seen[m.Name] {
			return nil, fmt.Errorf("%s: duplicate mcp name %q", file, m.Name)
		}
		seen[m.Name] = true
	}
	return &wc, nil
}
