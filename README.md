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

This repository holds the Go module only. The Docker Compose files that run it with PostgreSQL and Temporal (`docker-compose.yml`, `docker-compose.build.yml`) live in its parent directory, outside the repository, and mount it as `./agent`:

```
temporal-agent/              # not a git repository
├── docker-compose.yml       # postgres, temporal, temporal-ui, agent
├── docker-compose.build.yml
└── agent/                   # this repository
```

Run the commands below from that parent directory.

1. Clone the repository as `agent/` next to the compose files, and start services:

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
| `TEMPORAL_TLS` | `true` = TLS to Temporal, the server's certificate checked against the system's CAs (a public certificate, no client certificate). Also on with the settings below; `false` with them is an error. Nothing set = plaintext |
| `TEMPORAL_TLS_CERT`, `TEMPORAL_TLS_KEY` | Client certificate and key (PEM files) for mTLS to Temporal. Both or neither |
| `TEMPORAL_TLS_CA` | CA (PEM file) that signs the Temporal server's certificate, for a private CA; turns TLS on even without a client certificate. Empty = the system's CAs |
| `TEMPORAL_TLS_SERVER_NAME` | Name the Temporal server's certificate is checked for, when it differs from the host of `TEMPORAL_HOST` (needs TLS on: `TEMPORAL_TLS=true` is enough) |
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
| `RUN_AS_UID`, `RUN_AS_GID` | User (and group, default: the uid) that `exec` and coding runs run as. Set to `10001` (`agent-run`) by both images. Empty on a worker running as root = `exec` and coding runs are refused. Must be a uid of its own: its processes are killed whenever no command runs |
| `CLAUDE_CODE_REPOS` | Comma-separated globs of the repositories a coding worker (`analyze_repo`, `implement_feature`) may clone and push to, e.g. `git@github.com:acme/*,https://github.com/acme/*` (`*` stops at a `/`). Empty = every repository is refused |
| `CLAUDE_CODE_MODEL` | Model of a coding worker's runs, e.g. `sonnet`. The calling model cannot choose it. Empty = the CLI's default |
| `CLAUDE_CODE_MAX_BUDGET_USD` | Spending cap of each coding run, in US dollars; `implement_feature`'s `max_budget_usd` can only lower it. Empty = no cap; not a positive number = the worker does not start |
| `INTERNAL_ADDR` | Address of the internal API that receives worker notifications (default `:9999`). Keep it off the public network |
| `NOTIFY_URL` | Base URL a worker posts its notifications to (default `http://localhost:9999`) |
| `INTERNAL_API_KEY` | Secret shared by the server and its workers for `/internal/notify` (`Authorization: Bearer …`). Empty = the server refuses every notification; a worker checks it at startup and logs a refusal as an error |
| `TRUSTED_PROXIES` | Comma-separated addresses or CIDR ranges of the reverse proxies in front of the server, whose `X-Forwarded-For` gives the client's address; `none` when clients connect directly. Empty (default) = the client's address is unknown, and failed logins are limited per account only; so is a login a trusted proxy forwards without naming the client |
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

- **`exec`** and **coding runs** execute commands chosen by a model. They get a
  filtered environment (no `DATABASE_URL`, no API keys: see `subproc.Env`),
  their whole process group is killed when they return or time out, and they run as the user
  `RUN_AS_UID` names, never as the worker's: a command run as the worker's user
  reads the platform's credentials back from the worker's
  `/proc/<pid>/environ`, and its 0600 files (the git key). Both images create
  that user (`agent-run`, uid 10001) and set `RUN_AS_UID`; a worker running as
  root without it refuses `exec` and coding runs. The worker hands it the
  workspace at startup, and each clone for the length of a run. The coding
  CLI's configuration (`CLAUDE_CONFIG_DIR`) stays the worker's: each run
  works on a copy of its own (settings, `CLAUDE.md`, login), thrown away with
  it, so that nothing a run writes there (a hook, an instruction, an MCP
  server) is read by the next. Only a renewed OAuth login comes back, logged
  each time: a JSON file that keeps every field of the current login, which a
  run could still replace by another account's. With `ANTHROPIC_API_KEY` (the
  compose default) the CLI uses the key, nothing comes back, and that channel
  does not exist: prefer it to an OAuth login for unattended runs. Without
  `CLAUDE_CONFIG_DIR` nor `RUN_AS_UID` (a development machine), the CLI, run by
  the worker or by `agent claude-code-run`, keeps its own default configuration
  and login (`~/.claude`). Its Go caches are its own (`$HOME/go`, `$HOME/.cache`): the
  worker's are what the agent itself is built from. Once no command of the
  worker runs as that user, every process of it is killed, one that left the
  process group included: the uid must be dedicated to this. It is still **not
  a sandbox**: a command can leave the workspace and read whatever that user
  can.

- **Coding runs** edit their clone, `.git/config` included. The worker's own
  git commands after the run (inspection, push) first take all of `.git` back
  from the run's user, write the configuration the clone had before the run
  back as a new file, never run hooks or a filesystem monitor, and read no
  system or global configuration; a run that changed `.git/config` gets nothing
  pushed. A file of `.git` another path shares (a hard link the run made to
  keep rewriting it) stops the run as tampered. What is pushed is the commit
  the inspection listed, not whatever the branch's ref says by then.

- **`web_fetch`** fetches a URL the model chose, so it only connects to public
  addresses: loopback, private, link-local (cloud metadata), CGNAT and reserved
  ranges are refused after name resolution, on every redirect too, and only
  `http`/`https` URLs are followed.

- **Logins**: failures are limited in memory, 10 per account in 15 minutes
  (then `429`), and logged with the client's address. The limit of 20 per
  client address only applies when that address is known, which takes
  `TRUSTED_PROXIES`: behind a proxy nobody declared, every client has the
  proxy's address, and a limit per address would let anyone lock every account
  out with twenty wrong passwords. Set it to the proxies' addresses (the
  client's is then read from `X-Forwarded-For`, from the right, past the
  trusted proxies) or to `none` for a server that faces its clients directly.

## License

MIT
