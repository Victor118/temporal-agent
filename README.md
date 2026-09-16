# Temporal Agent

An AI agent platform orchestrated by [Temporal](https://temporal.io/), built in Go. It provides a scalable, durable execution environment for LLM-powered agents with tool use, multi-agent collaboration, and persistent memory.

## Features

- **Durable AI workflows** — ReAct loop (LLM reasoning + tool execution) powered by Temporal, with automatic retries and fault tolerance
- **Multi-agent system** — Agents can spawn sub-agents for specialized tasks, each with isolated context
- **Pluggable skills** — Skills loaded from Git repositories or local files, assigned to agents for domain-specific expertise
- **Persistent memory** — PostgreSQL-backed storage for conversation history, key-value memory (user/project/session scoped), and task logs
- **Real-time streaming** — SSE (Server-Sent Events) hub for live updates to connected clients
- **Built-in tools** — File system operations, web access, shell execution, user interaction, workflow queries, scheduling, and MCP support

## Architecture

The system runs in three modes:

| Mode | Command | Description |
|------|---------|-------------|
| **Server** | `agent server` | HTTP API + SSE hub + agent catalog + skill versioning |
| **Worker** | `agent worker` | Temporal worker + activities + skill execution |
| **Dev** | `agent dev` | Combined server + worker for local development |

### Workflows

- **SessionWorkflow** — Long-lived orchestration managing context persistence (load/persist via PostgreSQL)
- **AgentWorkflow** — ReAct loop: calls LLM, executes tools, repeats until done. Loads the prompt, skills and allowed tools of its agent (`agent_id`)
- **Sub-agents** — One-shot AgentWorkflows with isolated context; the parent only sees the final response

### Agents, Tools & Task Queues

- **Agents** live in the `agents` table (source of truth). `agents.yaml` only seeds agents missing from the DB. Each agent has skills and an optional tool allowlist (globs, e.g. `github_*`).
- **Task queues are capabilities**: each worker declares in its `worker.yaml` the queue it serves and the tools it exposes there, and publishes them to the `tools` table. Every tool call is routed to its tool's queue.
- **Workflows** (sessions, agents, LLM calls) run on a dedicated queue (`WORKFLOW_QUEUE`).

See [docs/architecture.md](docs/architecture.md) for the full model.

## Getting Started

### Prerequisites

- Docker & Docker Compose
- A Temporal server (e.g. [Temporal CLI](https://docs.temporal.io/cli) or Temporal Cloud)
- An LLM API key (Anthropic)

### Setup

1. Clone the repository and start services:

```bash
docker compose up -d
```

2. Copy and configure environment variables:

```bash
cp agent/.env.example agent/.env
# Edit agent/.env with your API keys and configuration
```

3. Build and run:

```bash
docker compose exec agent go build ./...
docker compose exec agent ./agent dev
```

The API will be available at `http://localhost:8888`.

### Environment Variables

| Variable | Description |
|----------|-------------|
| `TEMPORAL_HOST` | Temporal server address |
| `TEMPORAL_NAMESPACE` | Temporal namespace |
| `DATABASE_URL` | PostgreSQL connection string |
| `LLM_PROVIDER` | LLM provider (`anthropic`) |
| `LLM_API_KEY` | LLM API key |
| `LLM_MODEL` | Model to use |
| `HTTP_ADDR` | Public HTTP server address |
| `WORKFLOW_QUEUE` | Task queue for sessions, agents and LLM calls (default `agent`) |
| `DEFAULT_AGENT_ID` | Agent used when a session doesn't name one (default `default`) |
| `AGENT_DEFINITIONS_FILE` | Agents seed file (default `./agents.yaml`) |
| `WORKER_CONFIG` | Worker config: tool queue, exposed tools, MCP servers (default `./worker.yaml`, see `worker.example.yaml`) |
| `MCP_SERVERS` | JSON array of MCP servers, used only without a worker config |

## Project Structure

```
agent/
├── activity/       # Temporal activities (LLM calls, tool exec, notifications)
├── api/            # HTTP API types
├── cmd/agent/      # CLI entrypoints (server, worker, dev)
├── config/         # Configuration loading
├── provider/       # LLM provider abstraction (Anthropic)
├── skill/          # Skill loading (Git, filesystem)
├── sse/            # Server-Sent Events hub
├── store/          # PostgreSQL persistence (messages, memory, task logs)
├── tool/           # Tool implementations (fs, web, exec, spawn, schedule)
├── web/            # Web UI templates
└── workflow/       # Temporal workflows (session, agent, scheduled)
```

## License

MIT
