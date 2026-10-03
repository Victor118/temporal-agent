package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
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
	// SummaryModel writes the summary a forked session starts from; empty =
	// the workers' LLM_MODEL. A summary needs less than a conversation does.
	SummaryModel string

	// Server
	HTTPAddr     string
	InternalAddr string // Internal endpoint for worker→server notifications
	NotifyURL    string // Base URL the worker POSTs notifications to
	// InternalAPIKey is the secret a worker presents to the server's internal
	// endpoint. The server refuses every notification while it is empty: the
	// endpoint injects events into any session, so it is closed by default.
	InternalAPIKey string
	// TrustedProxies are the reverse proxies in front of the server, whose
	// X-Forwarded-For gives the client's address: CIDR ranges or addresses,
	// "none" when clients connect directly. Empty = unknown, and logins are
	// then limited per account only (auth.DefaultLoginLimits).
	TrustedProxies string

	// Workspace
	WorkspacePath string
	// Root for Claude Code run workspaces: one throwaway clone per run, kept
	// apart from WorkspacePath, which the exec and filesystem tools share.
	ClaudeCodeWorkspace string
	// ClaudeCodeRepos are the repositories a coding worker may clone and push
	// to, as globs (path.Match syntax: * stops at a slash). Empty = none: the
	// model picks the repository, and a push uses the worker's identity.
	ClaudeCodeRepos []string
	// ClaudeCodeModel is the model of this worker's coding runs (CLI
	// --model); empty = the CLI's default. ClaudeCodeMaxBudgetUSD caps what
	// one run may spend (--max-budget-usd), a decimal number of dollars;
	// empty = no cap. Both are the operator's: a run is paid by the worker's
	// key, so the model asking for it does not choose its price.
	ClaudeCodeModel        string
	ClaudeCodeMaxBudgetUSD string
	// RunAsUID and RunAsGID are the user commands chosen by a model (exec,
	// a coding run) run as, in place of the worker's: subproc.Identity. A
	// worker running as root refuses them without one. Empty GID = the UID.
	RunAsUID string
	RunAsGID string
	// ClaudeConfigDir is the operator's configuration of the coding CLI
	// (CLAUDE_CONFIG_DIR), the worker's alone: each run works on a copy of
	// its own (claudecode.SeedConfigDir).
	ClaudeConfigDir string
	// SSH identity a coding worker uses for git: cloning a private repository,
	// and pushing when the identity allows it. What the worker can do is a
	// property of the identity it is given, not of the code — the read-only
	// worker is meant to hold one that cannot write anywhere.
	ClaudeCodeSSHKey string

	// Skills
	SkillsRepo          string // Git repo URL for skills (e.g. "https://github.com/org/agent-skills")
	SkillsBranch        string // Branch to use (default: "main")
	SkillsWebhookSecret string // GitHub webhook secret for signature verification

	// Web Search
	BraveSearchAPIKey string

	// Telegram
	TelegramBotToken string
	// TelegramWebhookSecret is the secret_token given to setWebhook: Telegram
	// sends it back on every update. Empty = the webhook route is not served.
	TelegramWebhookSecret string

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
	Tools       []string `yaml:"tools" json:"tools"` // Allowed tool name globs; omitted = no tool, "*" = all
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

		LLMProvider:  envOr("LLM_PROVIDER", "anthropic"),
		LLMAPIKey:    os.Getenv("LLM_API_KEY"),
		LLMModel:     envOr("LLM_MODEL", "claude-sonnet-5"),
		SummaryModel: os.Getenv("SUMMARY_MODEL"),

		HTTPAddr:     envOr("HTTP_ADDR", ":8888"),
		InternalAddr: envOr("INTERNAL_ADDR", ":9999"),
		NotifyURL:    envOr("NOTIFY_URL", "http://localhost:9999"),

		InternalAPIKey: os.Getenv("INTERNAL_API_KEY"),
		TrustedProxies: os.Getenv("TRUSTED_PROXIES"),

		WorkspacePath:       envOr("WORKSPACE_PATH", "./workspace"),
		ClaudeCodeWorkspace: envOr("CLAUDE_CODE_WORKSPACE", "./claude-code-runs"),
		ClaudeCodeSSHKey:    os.Getenv("CLAUDE_CODE_SSH_KEY"),
		ClaudeConfigDir:     os.Getenv("CLAUDE_CONFIG_DIR"),
		RunAsUID:            os.Getenv("RUN_AS_UID"),
		RunAsGID:            os.Getenv("RUN_AS_GID"),
		ClaudeCodeRepos:     splitList(os.Getenv("CLAUDE_CODE_REPOS")),

		ClaudeCodeModel:        os.Getenv("CLAUDE_CODE_MODEL"),
		ClaudeCodeMaxBudgetUSD: os.Getenv("CLAUDE_CODE_MAX_BUDGET_USD"),

		SkillsRepo:          os.Getenv("SKILLS_REPO"),
		SkillsBranch:        envOr("SKILLS_BRANCH", "main"),
		SkillsWebhookSecret: os.Getenv("SKILLS_WEBHOOK_SECRET"),

		BraveSearchAPIKey: os.Getenv("BRAVE_SEARCH_API_KEY"),

		TelegramBotToken:      os.Getenv("TELEGRAM_BOT_TOKEN"),
		TelegramWebhookSecret: os.Getenv("TELEGRAM_WEBHOOK_SECRET"),

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

const maxAgentIDLen = 58

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
		if err := a.Validate(); err != nil {
			return nil, fmt.Errorf("%s: agent[%d]: %w", path, i, err)
		}
		if seen[a.ID] {
			return nil, fmt.Errorf("%s: duplicate agent id %q", path, a.ID)
		}
		seen[a.ID] = true
	}
	return doc.Agents, nil
}

// Validate checks an agent definition on its own, wherever it comes from: the
// seed file or the back-office.
func (a AgentDefinition) Validate() error {
	if !agentIDPattern.MatchString(a.ID) {
		return fmt.Errorf("invalid id %q (must match %s)", a.ID, agentIDPattern.String())
	}
	// The agent is also a tool, agent_<id>, and the LLM API caps tool names at
	// 64 characters.
	if len(a.ID) > maxAgentIDLen {
		return fmt.Errorf("id %q too long (%d characters, at most %d)", a.ID, len(a.ID), maxAgentIDLen)
	}
	if a.Name == "" {
		return fmt.Errorf("agent %q missing name", a.ID)
	}
	for _, g := range a.Tools {
		if err := CheckToolGlob(g); err != nil {
			return fmt.Errorf("agent %q: invalid tool pattern %q: %w", a.ID, g, err)
		}
	}
	return nil
}

// CheckToolGlob rejects a malformed tool pattern. Matching never reports it, so
// an unchecked typo would silently match nothing instead of failing on save.
func CheckToolGlob(g string) error {
	_, err := path.Match(g, "")
	return err
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

// splitList reads a comma-separated list, dropping blanks.
func splitList(raw string) []string {
	var out []string
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
