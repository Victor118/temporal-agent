# CLAUDE.md

## Build & Run

- Go n'est pas installé localement. Toujours utiliser Docker :
  - `cd /home/victor/dev/temporal-agent && docker compose up -d`
  - `docker compose exec agent go build ./...`
  - `docker compose exec agent go test ./...`
- Hot-reload via air dans le container
- Port 8888 exposé pour l'API HTTP

## Architecture

- 3 modes : `agent server`, `agent worker`, `agent dev` (les deux combinés)
- Server = API HTTP + SSE hub + catalogue agents + skills versioning
- Worker = Temporal worker + activities + skills par task queue
- Communication inter-services via endpoints `/internal/*` (notify, skills/version, workers/register, agents)

## Workflows

- **SessionWorkflow** : orchestration long-lived, gère la persistance (LoadContext/PersistContext via SQLite)
- **AgentWorkflow** : boucle ReAct (LLM + tools), charge ses skills via `workflow.GetInfo(ctx).TaskQueueName`
- **Les sous-agents** (spawn_session) sont des AgentWorkflow one-shot, sans persistance, contexte isolé du parent
- Le parent ne voit que la réponse finale du sous-agent (string)

## Skills & Task Queues

- Skills chargés depuis un repo Git (prod) ou `./skills` (dev)
- Mapping queue -> skills via `TASK_QUEUE_SKILLS` env var (JSON) : `{"coding":["ddd","tdd"],"devops":["terraform","k8s"]}`
- Catalogue des agents centralisé sur le serveur (`POST /internal/workers/register`, `GET /internal/agents`)
- Chaque agent voit dans son prompt : ses propres skills + le catalogue complet des autres agents
- Les workers s'enregistrent au demarrage et fetchent le catalogue

## Store (PostgreSQL)

- `messages` : historique de conversation par session (JSONB)
- `memory` : key-value scope (user/project/session)
- `task_logs` : suivi des taches schedulees
- Persistance geree par SessionWorkflow, pas par AgentWorkflow
- Config via `DATABASE_URL` env var
- Migration automatique au demarrage (CREATE TABLE IF NOT EXISTS)

## Conventions

- `SystemPrompt` dans les workflow inputs = override manuel ; si vide, charge depuis la task queue
- Les tool results remontent comme string au parent
- Les notifications SSE passent par `/internal/notify` (prod) ou in-memory hub (dev)
- ask_user fonctionne pour les sous-agents (SSE route vers le bon sessionID via le workflowID)
