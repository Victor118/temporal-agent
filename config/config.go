package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	// Temporal
	TemporalHost      string
	TemporalNamespace string
	TemporalTLSCert   string
	TemporalTLSKey    string
	TaskQueues        []string
	TaskQueueMCP      map[string][]string // queue name → MCP server names

	// Agent definitions seed file (imported into the DB by the server)
	AgentsFile string

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

// AgentDefinition is the seed definition of a logical agent (persona), read from
// agents.yaml. The DB agents table is the source of truth; this file only seeds
// agents that don't exist yet.
type AgentDefinition struct {
	ID           string   `yaml:"id" json:"id"`
	Name         string   `yaml:"name" json:"name"`
	Description  string   `yaml:"description" json:"description"`
	Skills       []string `yaml:"skills" json:"skills"`
	Tools        []string `yaml:"tools" json:"tools"` // Allowed tool name globs; omitted = all tools
	DefaultQueue string   `yaml:"default_queue" json:"default_queue"`
}

func Load() *Config {
	return &Config{
		TemporalHost:      envOr("TEMPORAL_HOST", "localhost:7233"),
		TemporalNamespace: envOr("TEMPORAL_NAMESPACE", "default"),
		TemporalTLSCert:   os.Getenv("TEMPORAL_TLS_CERT"),
		TemporalTLSKey:    os.Getenv("TEMPORAL_TLS_KEY"),
		TaskQueues:        parseTaskQueues(envOr("TASK_QUEUES", envOr("TASK_QUEUE", "agent-default"))),

		AgentsFile: envOr("AGENT_DEFINITIONS_FILE", "./agents.yaml"),

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

		TaskQueueMCP: parseTaskQueueMap(os.Getenv("TASK_QUEUE_MCP")),
	}
}

// agentIDPattern enforces lowercase alphanumeric + hyphen, no leading/trailing hyphen.
// Single-letter IDs are allowed (e.g. "x").
var agentIDPattern = regexp.MustCompile(`^[a-z]([a-z0-9-]*[a-z0-9])?$`)

// LoadAgentDefinitions reads and validates the agents seed file.
func LoadAgentDefinitions(path string) ([]AgentDefinition, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	var doc struct {
		Agents []AgentDefinition `yaml:"agents"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	if len(doc.Agents) == 0 {
		return nil, fmt.Errorf("%s: no agents defined", path)
	}

	seen := make(map[string]bool, len(doc.Agents))
	for i, a := range doc.Agents {
		if !agentIDPattern.MatchString(a.ID) {
			return nil, fmt.Errorf("%s: agent[%d] invalid id %q (must match %s)", path, i, a.ID, agentIDPattern.String())
		}
		if seen[a.ID] {
			return nil, fmt.Errorf("%s: duplicate agent id %q", path, a.ID)
		}
		seen[a.ID] = true
		if a.Name == "" {
			return nil, fmt.Errorf("%s: agent %q missing name", path, a.ID)
		}
		if a.DefaultQueue == "" {
			return nil, fmt.Errorf("%s: agent %q missing default_queue", path, a.ID)
		}
	}
	return doc.Agents, nil
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

// parseTaskQueueMap parses a JSON object env var like TASK_QUEUE_MCP.
// Example: {"coding":["mcp-github"],"devops":["mcp-k8s"]}
func parseTaskQueueMap(raw string) map[string][]string {
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
