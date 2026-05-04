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
	TaskQueueMCP      map[string][]string // queue name → MCP server names

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
	APIKey       string // Shared secret for auth (required in production)

	// Workspace
	WorkspacePath string

	// Skills
	SkillsRepo          string // Git repo URL for skills (e.g. "https://github.com/org/agent-skills")
	SkillsBranch        string // Branch to use (default: "main")
	SkillsWebhookSecret string // GitHub webhook secret for signature verification

	// Web Search
	BraveSearchAPIKey string

	// Telegram
	TelegramBotToken string

	// Email (SMTP)
	SMTPHost     string
	SMTPPort     string
	SMTPUsername string
	SMTPPassword string
	SMTPFrom     string // Default sender address

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
		APIKey:       os.Getenv("API_KEY"),

		WorkspacePath: envOr("WORKSPACE_PATH", "./workspace"),

		SkillsRepo:          os.Getenv("SKILLS_REPO"),
		SkillsBranch:        envOr("SKILLS_BRANCH", "main"),
		SkillsWebhookSecret: os.Getenv("SKILLS_WEBHOOK_SECRET"),

		BraveSearchAPIKey: os.Getenv("BRAVE_SEARCH_API_KEY"),

		TelegramBotToken: os.Getenv("TELEGRAM_BOT_TOKEN"),

		SMTPHost:     os.Getenv("SMTP_HOST"),
		SMTPPort:     envOr("SMTP_PORT", "587"),
		SMTPUsername: os.Getenv("SMTP_USERNAME"),
		SMTPPassword: os.Getenv("SMTP_PASSWORD"),
		SMTPFrom:     os.Getenv("SMTP_FROM"),

		MCPServers: parseMCPServers(os.Getenv("MCP_SERVERS")),

		TaskQueueSkills: parseTaskQueueSkills(os.Getenv("TASK_QUEUE_SKILLS")),
		TaskQueueMCP:    parseTaskQueueSkills(os.Getenv("TASK_QUEUE_MCP")),
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

// MCPServersForQueues returns the MCP servers assigned to the given task queues.
// If TaskQueueMCP is not configured, all MCP servers are returned (backward compat).
func (c *Config) MCPServersForQueues(queues []string) []MCPServer {
	if len(c.TaskQueueMCP) == 0 {
		return c.MCPServers
	}

	// Collect unique MCP server names for these queues
	needed := make(map[string]bool)
	for _, q := range queues {
		for _, name := range c.TaskQueueMCP[q] {
			needed[name] = true
		}
	}

	// Filter
	var result []MCPServer
	for _, s := range c.MCPServers {
		if needed[s.Name] {
			result = append(result, s)
		}
	}
	return result
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
