# Temporal Agent

An AI agent platform orchestrated by [Temporal](https://temporal.io/), built in Go. It provides a scalable, durable execution environment for LLM-powered agents with tool use, multi-agent collaboration, and persistent memory.

## Why

Getting an LLM to call tools in a loop is easy. Running that loop as a
production system is not. Temporal Agent exists to close four specific gaps.

**Long-running agents crash, and crashes are expensive.** A ReAct loop that
dies at minute eight loses every tool result it has accumulated and starts
over. Here the loop *is* a Temporal workflow: state is durable, the LLM call
is retried with exponential backoff, a tool call that no worker can serve
fails in a minute instead of hanging forever, and a worker going down does not
lose the run — it resumes on another one.

**Agents that delegate can run away.** An agent never sees itself in its own
agents directory, which rules out the trivial case, but A delegating to B,
which delegates back to A, is still a cycle — and its cost is exponential in
depth. Every delegation therefore carries the chain of agents that led to it.
Enforcing a depth and cycle bound on that chain is the next step, with the
refusal returned to the model as a normal tool outcome — so it can pick another
route, reason it through itself, or ask, instead of failing.

**Waiting for a human should cost nothing.** Approval steps, clarifications
and escalations are measured in hours, not seconds. A blocked agent parks on
a Temporal signal: no process held open, no polling, and the run resumes
exactly where it stopped whenever the answer arrives.

**Workers are not interchangeable.** Some tools only run where the capability
lives — a GPU, a restricted network segment, a licensed binary, a machine
cleared for sensitive data. Tool execution is therefore distributed by design:
each capability is served by its own pool of workers, and every tool call is
routed to a pool that can actually run it. A capability scales by adding
machines to its pool and keeps working when one of them goes away, and what an
agent is offered is what the fleet publishes — not whatever the worker that
happens to answer has locally.

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
- **Task queues are capabilities**: each worker declares in its `worker.yaml` the queue it serves and the tools it exposes there, and publishes them to the `tools` table. Every tool call is routed to its tool's queue. Every tool call becomes a Temporal activity task, persisted on the queue of the capability it needs and picked up by any worker in that pool. A tool scales by adding workers to its pool; a call is never lost — if the worker running it dies, Temporal hands it to another one. Delivery is at-least-once, so tools with side effects are expected to be idempotent.
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

Create the first account, an admin, before logging in:

```bash
docker compose exec -it agent ./tmp/main user create --email you@example.com --name You --admin
```

Admins manage the other accounts in the back-office, under `/admin/users`.

### Environment Variables

| Variable | Description |
|----------|-------------|
| `TEMPORAL_HOST` | Temporal server address |
| `TEMPORAL_NAMESPACE` | Temporal namespace |
| `DATABASE_URL` | PostgreSQL connection string |
| `LLM_PROVIDER` | LLM provider (`anthropic`) |
| `LLM_API_KEY` | LLM API key |
| `LLM_MODEL` | Model to use |
| `SUMMARY_MODEL` | Model that summarizes a session for a fork (default: `LLM_MODEL`) |
| `HTTP_ADDR` | Public HTTP server address |
| `WORKFLOW_QUEUE` | Task queue for sessions, agents and LLM calls (default `agent`) |
| `DEFAULT_AGENT_ID` | Agent used when a session doesn't name one (default `default`) |
| `AGENT_DEFINITIONS_FILE` | Agents seed file (default `./agents.yaml`) |
| `WORKER_CONFIG` | Worker config: tool queue, exposed tools, MCP servers (default `./worker.yaml`, see `worker.example.yaml`) |
| `MCP_SERVERS` | JSON array of MCP servers, used only without a worker config |
| `CLAUDE_CODE_REPOS` | Comma-separated globs of the repositories a coding worker (`analyze_repo`, `implement_feature`) may clone and push to, e.g. `git@github.com:acme/*,https://github.com/acme/*` (`*` stops at a `/`). Empty = every repository is refused |
| `INTERNAL_ADDR` | Address of the internal API that receives worker notifications (default `:9999`). Keep it off the public network |
| `NOTIFY_URL` | Base URL a worker posts its notifications to (default `http://localhost:9999`) |
| `INTERNAL_API_KEY` | Secret shared by the server and its workers for `/internal/notify` (`Authorization: Bearer …`). Empty = the server refuses every notification |
| `SKILLS_REPO`, `SKILLS_BRANCH` | Git repository (and branch) the skills are loaded from |
| `SKILLS_WEBHOOK_SECRET` | GitHub webhook secret for `/webhooks/skills`. Empty = the route is not served |
| `TELEGRAM_BOT_TOKEN` | Telegram bot token, to send messages |
| `TELEGRAM_WEBHOOK_SECRET` | The `secret_token` passed to Telegram's `setWebhook`, checked on every update of `/webhooks/telegram`. Empty = the route is not served |

## Project Structure

```
agent/
├── activity/       # Temporal activities (LLM calls, tool exec, notifications)
├── api/            # HTTP API types
├── cmd/agent/      # CLI entrypoints (server, worker, dev)
├── config/         # Configuration loading
├── provider/       # LLM provider abstraction (Anthropic)
├── session/        # Session rules: open, deliver, fork, members, Temporal lookups
├── skill/          # Skill loading (Git, filesystem)
├── sse/            # Server-Sent Events hub
├── store/          # PostgreSQL persistence (messages, memory, task logs)
├── tool/           # Tool implementations (fs, web, exec, spawn, schedule)
├── web/            # Web UI: chat (web/chat) and back-office (web/admin)
└── workflow/       # Temporal workflows (session, agent, scheduled)
```

## Security notes

- **`exec`** runs a shell command chosen by the model. It gets a filtered
  environment (no `DATABASE_URL`, no API keys: see `subproc.Env`) and its whole
  process group is killed at the timeout, but it is **not a sandbox**: it runs
  as the worker's user, can leave the workspace, and can read whatever that user
  can, the worker's own `/proc/<pid>/environ` included. Only expose `exec` (in
  `worker.yaml` and in an agent's allowlist) on a worker whose user and
  filesystem hold nothing an agent must not reach.

- **`web_fetch`** fetches a URL the model chose, so it only connects to public
  addresses: loopback, private, link-local (cloud metadata), CGNAT and reserved
  ranges are refused after name resolution, on every redirect too, and only
  `http`/`https` URLs are followed.

- **Logins**: failures are limited in memory, 20 per client address and 10 per
  account in 15 minutes (then `429`), and logged with the address. The address
  is the connection's peer: behind a reverse proxy, every client shares it.

## License

MIT
