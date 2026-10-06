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
- **Built-in tools** — File system operations, web access, shell execution, user interaction, workflow queries, scheduling
- **Published files** — An agent hands the members a file (`publish_file` for a text it writes, `exec`'s `publish` for a file a command made), attached to its answer and downloaded by the session's members only
- **Documents** — `render_pdf` (Markdown or typst to PDF) and `make_slides` (Markdown to an editable pptx or a PDF deck), rendered by pandoc and typst on the main worker and published like any file
- **Machines** — A user's own machine, outside the private network, connects to the server with `agent connect` (an outgoing WebSocket, no VPN, no database nor Temporal access) and runs the directives of their agents' turns; enrolled by a code typed in « Mes machines ». `analyze_repo` runs there, with the user's own Claude Code login and git access, and falls back to the installation's coding workers when no machine of theirs is connected: see [docs/design/machines.md](docs/design/machines.md)
- **Remote MCP servers** — A worker declares MCP servers in its `worker.yaml` (Streamable HTTP, or the older HTTP+SSE) and publishes their tools; a server that is down is retried in the background, and tools it adds or removes are picked up within 30 s

## Architecture

The system runs in three modes:

| Mode | Command | Description |
|------|---------|-------------|
| **Server** | `agent server` | HTTP API + SSE hub + agent catalog + skill versioning |
| **Worker** | `agent worker` | Temporal worker + activities + skill execution |
| **Dev** | `agent dev` | Combined server + worker for local development |
| **Machine** | `agent connect` | A user's machine: connects to a server's gateway and runs what it is sent (no database, no Temporal) |

### Workflows

- **ParticipantWorkflow** — An agent in a session: answers its messages in order, one at a time, in parallel with the other agents, and runs only while it has messages (a session has no workflow of its own)
- **AgentWorkflow** — ReAct loop: calls LLM, executes tools, repeats until done. Loads the prompt, skills and allowed tools of its agent (`agent_id`)
- **Sub-agents** — One-shot AgentWorkflows with isolated context; the parent only sees the final response

### Agents, Tools & Task Queues

- **Agents** live in the `agents` table (source of truth). `agents.yaml` only seeds agents missing from the DB. Each agent has skills and an optional tool allowlist (globs, e.g. `github_*`).
- **Task queues are capabilities**: each worker declares in its `worker.yaml` the queue it serves and the tools it exposes there, and publishes them to the `tools` table. Every tool call is routed to its tool's queue. Every tool call becomes a Temporal activity task, persisted on the queue of the capability it needs and picked up by any worker in that pool. A tool scales by adding workers to its pool; a call is never lost — if the worker running it dies, Temporal hands it to another one. Delivery is at-least-once, so tools with side effects are expected to be idempotent.
- **Workflows** (participants, agents, LLM calls) run on a dedicated queue (`WORKFLOW_QUEUE`).

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

The agent image holds pandoc, typst and the typst packages the document tools
use (pinned by version and SHA256 in the `Dockerfile`): after a change there,
`docker compose build agent` before restarting it.

Create the first account, an admin, before logging in:

```bash
docker compose exec -it agent ./tmp/main user create --email you@example.com --name You --admin
```

Admins manage the other accounts in the back-office, under `/admin/users`.

To try a machine (phase 0: an `echo` directive only), enroll one against the
dev server, type the code it shows in « Machines › Ajouter une machine », then
send it an echo:

```bash
docker compose exec -it agent ./tmp/main connect --join http://localhost:8888 --name essai
docker compose exec agent ./tmp/main machine-echo --email you@example.com --text bonjour --duration 10s
```

`agent connect` keeps its token in `$XDG_CONFIG_HOME/agent/machine` (or
`--dir`) and reconnects by itself; plain `http` is accepted only to this very
host, `https` otherwise.

To run `analyze_repo` on your machine (Linux; macOS should work, untested),
install the `claude` CLI and log in (`claude`, then `/login`, or `claude
setup-token`), then start `agent connect` with what it may do. These settings
are the machine's, never the server's:

| Flag (env) | Meaning |
|---|---|
| `--repos` (`AGENT_CONNECT_REPOS`) | Repositories an analysis may clone, comma-separated globs (`*` stops at a `/`), e.g. `git@github.com:me/*`. Empty = every analysis is refused |
| `--max-budget-usd` (`AGENT_CONNECT_MAX_BUDGET_USD`) | What one run may spend, in dollars; 0 = no cap |
| `--claude-auth` (`CLAUDE_CODE_AUTH`) | Who pays: `subscription` (your CLI's login or `CLAUDE_CODE_OAUTH_TOKEN`; an `ANTHROPIC_API_KEY` in your shell is then not passed to the CLI) or `api` (`ANTHROPIC_API_KEY`). Empty = the one credential set; both set = `agent connect` refuses to start |
| `--claude-model` (`CLAUDE_CODE_MODEL`) | Model of the runs; empty = the CLI's default |
| `--work-dir` | Where clones go, one per run, deleted after it (default: your cache, `agent/runs/<machine>`) |

The machine announces Claude Code only when the CLI is there and a login is
found without any paid call (`ANTHROPIC_API_KEY`, `CLAUDE_CODE_OAUTH_TOKEN`,
the CLI's login file, or the macOS keychain). Amazon Bedrock and Google
Vertex AI are not supported on a machine: a CLI set up for them is taken for
logged out. A run refused for its login withdraws Claude Code (you are told
in your notifications): the machine announces it again as soon as its login
file changes (a new `claude` then `/login`), and otherwise tries again by
itself after 10 minutes or at its next connection; a run refused before it
did anything goes to the installation's fallback, in the same request.

An analysis is read-only (the CLI's `plan` mode), and loads your own Claude
settings only, never the repository's (`--setting-sources user`,
`--strict-mcp-config`: a branch's `.claude/settings.json` hooks or
`.mcp.json` servers are not run). It clones with your git identity and your
git configuration (credential helpers, `core.sshCommand`), but never waits on
a prompt (git runs with no terminal): a repository that asks for a password, or an ssh host not in your
`known_hosts`, fails at once, saying what to do. Only ssh, https and local
paths are cloned (never `ext::`, `git://` or plain `http://`); git hooks are
off.

Every worker of a coding queue (the queue `analyze_repo` and `implement_feature` are published on) must have the `claude` CLI installed. A worker without it on that queue still answers a run's first check, and the run fails at once saying so: with N workers there of which one lacks the CLI, about one run in N fails that way.

A worker that stops ends its coding runs first, then gives the tasks under way 30 seconds to answer before it exits: each run's answer, that its worker stopped, is recorded by Temporal before the process ends, and read by the next worker of the queue (another replica, or this one once restarted). A stop takes 30 to 50 seconds in all. Give a worker's container a `stop_grace_period` of 60 seconds: Docker's default, 10 seconds, kills it before the answer goes out, and the workflow then waits a minute or two for the missed heartbeats.

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
| `LLM_MAX_CONTEXT_BYTES` | Worker: largest request one LLM call may send (system prompt, tools and conversation, as JSON bytes; default `2000000`, about 600K tokens or less, under the 1M-token window of `claude-sonnet-5`; about `400000` for a 200K-token model). Past it, the turn fails without retrying and tells the members to fork the session; the model's own "prompt is too long" is reported the same way. Not a positive number = the worker does not start |
| `HTTP_ADDR` | Public HTTP server address |
| `WORKFLOW_QUEUE` | Task queue for participants, agents and LLM calls (default `agent`) |
| `DEFAULT_AGENT_ID` | Agent used when a session doesn't name one (default `default`) |
| `AGENT_DEFINITIONS_FILE` | Agents seed file (default `./agents.yaml`) |
| `WORKER_CONFIG` | Worker config: tool queue, exposed tools, MCP servers (default `./worker.yaml`, see `worker.example.yaml`) |
| `MCP_SERVERS` | JSON array of MCP servers (`name`, `url`, `api_key`, `transport`), used only without a worker config |
| `FILES_MAX_BYTES` | Worker: largest file `exec` may publish (its `publish` parameter), and largest document `render_pdf` and `make_slides` render (and the most their `files` may weigh together), in bytes (default `20971520`, 20 MiB); `publish_file`, for a short text the model writes, has its own bound (1 MiB). Files are stored in PostgreSQL. Not a positive number = the worker does not start |
| `TYPST_PACKAGES` | Worker: directory of the typst packages a rendered document may import (default `/usr/local/share/typst/packages`, where the agent image puts touying). Typst never downloads one |
| `RUN_AS_UID`, `RUN_AS_GID` | User (and group, default: the uid) that `exec`, the document tools and coding runs run as. Set to `10001` (`agent-run`) by both images. Empty on a worker running as root = `exec`, documents and coding runs are refused. Must be a uid of its own, used by one worker process per pid namespace (one container): its processes are killed whenever no command runs, and at the startup of a coding worker |
| `CLAUDE_CODE_REPOS` | Comma-separated globs of the repositories a coding worker (`analyze_repo`, `implement_feature`) may clone and push to, e.g. `git@github.com:acme/*,https://github.com/acme/*` (`*` stops at a `/`). Empty = every repository is refused |
| `CLAUDE_CODE_MODEL` | Model of a coding worker's runs, e.g. `sonnet`. The calling model cannot choose it. Empty = the CLI's default |
| `CLAUDE_CODE_MAX_BUDGET_USD` | Spending cap of each coding run, in US dollars; `implement_feature`'s `max_budget_usd` can only lower it. Empty = no cap; not a positive number = the worker does not start |
| `CLAUDE_CODE_MAX_CONCURRENT_RUNS` | How many coding runs one worker takes at a time (default `1`). Only `1` is accepted for now: runs sharing the one `RUN_AS_UID` could reach each other's clone and credentials, and a process one left behind would outlive it (strays are killed only once no run is going). More takes one uid per run slot, not built yet. Anything else = the worker does not start |
| `CLAUDE_CODE_QUEUE_WAIT` | How long a coding run waits for a worker of its queue with a run to spare, as a Go duration (default `30m`). A run waiting more than a minute tells its user so on the turn's channel; past the wait it fails, saying the workers are busy. A run first checks that some worker answers on the queue: none within a minute, it fails at once saying no worker is available. Not a positive duration = the worker does not start |
| `CLAUDE_CODE_STALL_TIMEOUT` | How long the CLI of a coding run may write nothing before the run is ended as stuck, as a Go duration (default `12m`, above the CLI's 10 min maximum for a Bash command; raise it with `BASH_MAX_TIMEOUT_MS`). `0` = never. The run's result then says how far it got. Not a duration = the worker does not start |
| `CLAUDE_CODE_WORKSPACE` | Directory of a coding worker's clones, one per run, all of a run's steps on that worker (default `./claude-code-runs`); keep it apart from `WORKSPACE_PATH`. At startup the worker deletes the `run-*` entries a worker that died left there; if another live worker process shares the directory, only those older than a run's longest lifetime. Every worker process holds a lock on `.workers.lock` there; one that cannot take it does not start (one sweeping it is waited for up to 2 minutes, then the worker exits, to be restarted by whatever runs it) |
| `MACHINES_ENABLED` | `true` (default) or `false`: coding runs go to the users' machines first, and the server serves their gateway (`/machines/*`); `false` = no gateway, no machine route, no routing to machines. Set the same value on the server and every worker. Anything else = the process does not start |
| `CLAUDE_CODE_ANALYZE_QUEUE` | Main worker: where `analyze_repo` runs when no machine of the user's takes it, a coding queue serving `AnalyzeRepoWorkflow` (default `tools-claude-code-ro`, the read-only coding container's); `none` = no fallback (the run then fails saying the user's machine is not connected) |
| `CLAUDE_CODE_AUTH` | How coding runs authenticate: `api` (bills `ANTHROPIC_API_KEY`) or `subscription` (`CLAUDE_CODE_OAUTH_TOKEN`, made with `claude setup-token`, or the CLI's login). The other mode's credential never reaches the CLI. Empty = the one credential set; both set = the worker does not start |
| `CLAUDE_CODE_OAUTH_TOKEN` | A Claude subscription's long-lived token, for `CLAUDE_CODE_AUTH=subscription`. The runs then count against the subscription's usage limits, and the dollar cap is only an estimate |
| `INTERNAL_ADDR` | Address of the internal API that receives worker notifications and the directives workers hand to the machines' gateway (default `:9999`). Keep it off the public network |
| `NOTIFY_URL` | Base URL a worker posts its notifications and its machines' directives to (default `http://localhost:9999`) |
| `INTERNAL_API_KEY` | Secret shared by the server and its workers for `/internal/notify` and `/internal/machines/directives` (`Authorization: Bearer …`). Empty = the server refuses every notification and every directive; a worker checks it at startup and logs a refusal as an error |
| `TRUSTED_PROXIES` | Comma-separated addresses or CIDR ranges of the reverse proxies in front of the server, whose `X-Forwarded-For` gives the client's address; `none` when clients connect directly. Empty (default) = the client's address is unknown, and failed logins are limited per account only; so is a login a trusted proxy forwards without naming the client. The same goes for machines' enrollment requests (20 per address in 10 minutes only when it is known; 500 pending at most in any case) |
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
├── gateway/        # The machines' gateway: WebSocket, enrollment, heartbeats and completion as a Temporal client
├── conversation/   # What a model reads of a session: order and conversion of the history
├── machine/        # Machines: protocol, tokens, directives; machine/connect is `agent connect`
├── provider/       # LLM provider abstraction (Anthropic)
├── session/        # Session rules: open, deliver, fork, members, Temporal lookups
├── skill/          # Skill loading (Git, filesystem)
├── sse/            # Server-Sent Events hub
├── store/          # PostgreSQL persistence (messages, memory, task logs)
├── tool/           # Tool implementations (fs, web, exec, documents, schedule)
├── web/            # Web UI: chat (web/chat) and back-office (web/admin)
└── workflow/       # Temporal workflows (participant, agent, scheduled)
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

- **Published files** are written by a model, and may be made to attack their
  reader (an HTML page, an SVG with a script). `/files/{id}` serves one to the
  members of its session only (404 to anyone else), always as an attachment,
  with `X-Content-Type-Options: nosniff` and `Content-Security-Policy:
  sandbox`, and any type a browser could run (HTML, SVG, XML…) as
  `application/octet-stream`. `exec` reads the files to publish through the
  workspace's `os.Root`: no absolute path, no link leading out, regular files
  only.

- **Documents** (`render_pdf`, `make_slides`) run pandoc and typst on a
  source the model wrote, as `RUN_AS_UID`, through the same `subproc` path as
  `exec`. Each call has a directory of its own, the worker's (the files it
  uses, copied from the store; the run's user cannot change it, but any
  process of that user can read them meanwhile, as it can the workspace),
  removed afterwards, and swept at startup when a killed worker left it. The
  programs read their input on stdin and write the document on stdout; the
  document, and the `files` it uses taken together, are bounded by
  `FILES_MAX_BYTES`; 60 s per rendering, two at a time per worker. pandoc
  runs with `--sandbox` (it reads no file but those on its command line; a
  Lua filter of the tool's reads the images, bare file names in the call's
  directory, for a pptx) and a bounded heap; typst with `--root` on the
  directory, the image's packages as its package path and cache, an
  unreachable proxy (`127.0.0.1:0`: a package it lacks is never even
  requested), two threads and a bounded address space. A source can
  therefore read nothing outside its own files, and reach no network.

- **Machines** (`agent connect`) never get a database address, a Temporal
  address or a task token: the gateway holds the token and completes the
  activity itself; a machine knows a directive's ID alone, and only its
  owner's. Its token is long, kept as a hash on the server and in a 0600 file
  on the machine, and rotated on every connection: a replaced token presented
  again revokes the machine, and two live connections with one machine's
  token are both cut; both alert the owner (their notifications). Enrollment
  is by a code the machine shows and its owner types, logged in, into « Mes
  machines » (10 minutes, wrong codes limited per user, a warning against
  approving someone else's code), or by a single-use enrollment token read on
  standard input. What a machine sends is untrusted and bounded (256 KiB per
  message, a rate scaled to its number of directives). `agent connect` needs `https`, its certificate
  checked, except to this very host (development). Revoking a machine cuts it
  and ends its directives at once.

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

[PolyForm Noncommercial 1.0.0](LICENSE.md): free for personal use and any
other noncommercial purpose, for individuals and for noncommercial
organizations (charities, schools, public research, government). Any
commercial use, a company's included, needs a commercial license: contact
the author.

Versions up to commit `0b97727` were published under the MIT license and
remain available under it.
