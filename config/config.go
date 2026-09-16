package config

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

type Config struct {
	// Temporal
	TemporalHost      string
	TemporalNamespace string
	TemporalTLSCert   string
	TemporalTLSKey    string
	WorkflowQueue     string // Task queue running SessionWorkflow, AgentWorkflow and LLM calls

	// Agent started when a session doesn't name one
	DefaultAgentID string

	// Agent definitions seed file (imported into the DB by the server)
	AgentsFile string

	// Worker config file (queue + exposed tools + MCP servers)
	WorkerFile string

	// Store
	DatabaseURL string

	// LLM
	LLMProvider string
	LLMAPIKey   string
	LLMModel    string // Default model, applied by workers to requests without an explicit model

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
	ID          string   `yaml:"id" json:"id"`
	Name        string   `yaml:"name" json:"name"`
	Description string   `yaml:"description" json:"description"`
	Skills      []string `yaml:"skills" json:"skills"`
	Tools       []string `yaml:"tools" json:"tools"` // Allowed tool name globs; omitted = all tools
}

func Load() *Config {
	return &Config{
		TemporalHost:      envOr("TEMPORAL_HOST", "localhost:7233"),
		TemporalNamespace: envOr("TEMPORAL_NAMESPACE", "default"),
		TemporalTLSCert:   os.Getenv("TEMPORAL_TLS_CERT"),
		TemporalTLSKey:    os.Getenv("TEMPORAL_TLS_KEY"),
		WorkflowQueue:     envOr("WORKFLOW_QUEUE", "agent"),

		DefaultAgentID: envOr("DEFAULT_AGENT_ID", "default"),

		AgentsFile: envOr("AGENT_DEFINITIONS_FILE", "./agents.yaml"),
		WorkerFile: envOr("WORKER_CONFIG", "./worker.yaml"),

		DatabaseURL: envOr("DATABASE_URL", "postgres://agent:agent@localhost:5432/agent?sslmode=disable"),

		LLMProvider: envOr("LLM_PROVIDER", "anthropic"),
		LLMAPIKey:   os.Getenv("LLM_API_KEY"),
		LLMModel:    envOr("LLM_MODEL", "claude-sonnet-5"),

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

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
