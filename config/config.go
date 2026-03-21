package config

import (
	"encoding/json"
	"os"
	"strings"
)

type Config struct {
	// Temporal
	TemporalHost      string
	TemporalNamespace string
	TemporalTLSCert   string
	TemporalTLSKey    string
	TaskQueues        []string
	TaskQueueSkills   map[string][]string // queue name → skill names

	// Store
	DatabaseURL string

	// LLM
	LLMProvider string
	LLMAPIKey   string
	LLMModel    string

	// Server
	HTTPAddr     string
	InternalAddr string // Internal endpoint for worker→server notifications
	NotifyURL    string // Base URL the worker POSTs notifications to

	// Workspace
	WorkspacePath string

	// Skills
	SkillsRepo          string // Git repo URL for skills (e.g. "https://github.com/org/agent-skills")
	SkillsBranch        string // Branch to use (default: "main")
	SkillsWebhookSecret string // GitHub webhook secret for signature verification

	// Web Search
	BraveSearchAPIKey string

	// MCP Servers
	MCPServers []MCPServer
}

type MCPServer struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	APIKey    string `json:"api_key,omitempty"`
	Transport string `json:"transport,omitempty"` // "http" or "sse"
}

func Load() *Config {
	return &Config{
		TemporalHost:      envOr("TEMPORAL_HOST", "localhost:7233"),
		TemporalNamespace: envOr("TEMPORAL_NAMESPACE", "default"),
		TemporalTLSCert:   os.Getenv("TEMPORAL_TLS_CERT"),
		TemporalTLSKey:    os.Getenv("TEMPORAL_TLS_KEY"),
		TaskQueues:        parseTaskQueues(envOr("TASK_QUEUES", envOr("TASK_QUEUE", "agent-default"))),

		DatabaseURL: envOr("DATABASE_URL", "postgres://agent:agent@localhost:5432/agent?sslmode=disable"),

		LLMProvider: envOr("LLM_PROVIDER", "anthropic"),
		LLMAPIKey:   os.Getenv("LLM_API_KEY"),
		LLMModel:    envOr("LLM_MODEL", "claude-sonnet-4-20250514"),

		HTTPAddr:     envOr("HTTP_ADDR", ":8888"),
		InternalAddr: envOr("INTERNAL_ADDR", ":9999"),
		NotifyURL:    envOr("NOTIFY_URL", "http://localhost:9999"),

		WorkspacePath: envOr("WORKSPACE_PATH", "./workspace"),

		SkillsRepo:          os.Getenv("SKILLS_REPO"),
		SkillsBranch:        envOr("SKILLS_BRANCH", "main"),
		SkillsWebhookSecret: os.Getenv("SKILLS_WEBHOOK_SECRET"),

		BraveSearchAPIKey: os.Getenv("BRAVE_SEARCH_API_KEY"),

		MCPServers: parseMCPServers(os.Getenv("MCP_SERVERS")),

		TaskQueueSkills: parseTaskQueueSkills(os.Getenv("TASK_QUEUE_SKILLS")),
	}
}

// parseMCPServers parses MCP_SERVERS env var as JSON array.
// Example: [{"name":"github","url":"http://localhost:3001","api_key":"xxx"}]
func parseMCPServers(raw string) []MCPServer {
	if raw == "" {
		return nil
	}
	var servers []MCPServer
	json.Unmarshal([]byte(raw), &servers)
	return servers
}

// PrimaryTaskQueue returns the first configured task queue.
func (c *Config) PrimaryTaskQueue() string {
	if len(c.TaskQueues) > 0 {
		return c.TaskQueues[0]
	}
	return "agent-default"
}

// parseTaskQueues splits a comma-separated list of task queue names.
// Example: "agent-default,agent-gpu,agent-tools"
func parseTaskQueues(raw string) []string {
	var queues []string
	for _, q := range strings.Split(raw, ",") {
		q = strings.TrimSpace(q)
		if q != "" {
			queues = append(queues, q)
		}
	}
	return queues
}

// parseTaskQueueSkills parses TASK_QUEUE_SKILLS env var as JSON object.
// Example: {"coding":["ddd","tdd","hexagonal-architecture"],"devops":["terraform","k8s"]}
func parseTaskQueueSkills(raw string) map[string][]string {
	if raw == "" {
		return nil
	}
	var mapping map[string][]string
	json.Unmarshal([]byte(raw), &mapping)
	return mapping
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
