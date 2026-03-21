# Temporal Agent

An AI agent platform orchestrated by [Temporal](https://temporal.io/), built in Go. It provides a scalable, durable execution environment for LLM-powered agents with tool use, multi-agent collaboration, and persistent memory.

## Features

- **Durable AI workflows** — ReAct loop (LLM reasoning + tool execution) powered by Temporal, with automatic retries and fault tolerance
- **Multi-agent system** — Agents can spawn sub-agents for specialized tasks, each with isolated context
- **Pluggable skills** — Skills loaded from Git repositories or local files, mapped to task queues for domain-specific expertise
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
- **AgentWorkflow** — ReAct loop: calls LLM, executes tools, repeats until done. Loads skills based on its task queue
- **Sub-agents** — One-shot AgentWorkflows with isolated context; the parent only sees the final response

### Skills & Task Queues

Skills are domain-specific prompt augmentations mapped to task queues:

```json
{"coding": ["ddd", "tdd"], "devops": ["terraform", "k8s"]}
```

Workers register at startup and receive the full agent catalog, enabling cross-agent delegation.

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
| `TASK_QUEUES` | Comma-separated task queue names |
| `TASK_QUEUE_SKILLS` | JSON mapping of queues to skills |

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
