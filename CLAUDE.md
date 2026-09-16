# CLAUDE.md

## Build & Run

- Go n'est pas installé localement. Toujours utiliser Docker :
  - `cd /home/victor/dev/temporal-agent && docker compose up -d`
  - `docker compose exec agent go build ./...`
  - `docker compose exec agent go test ./...`
- Pas de hot-reload : le `CMD` du Dockerfile compile une fois au démarrage → `docker compose restart agent` après une modification
- Port 8888 exposé pour l'API HTTP

## Architecture

- 3 modes : `agent server`, `agent worker`, `agent dev` (les deux combinés)
- Server = API HTTP + SSE hub + catalogue agents + skills versioning
- Worker = Temporal worker + activities + tools publiés sur sa queue (`worker.yaml`)
- Communication worker → serveur via `/internal/notify` ; le reste passe par PostgreSQL (agents, tools, skills_version)

## Workflows

- **SessionWorkflow** : orchestration long-lived, gère la persistance (LoadContext/PersistContext via PostgreSQL)
- **AgentWorkflow** : boucle ReAct (LLM + tools), `agent_id` obligatoire (prompt, skills, allowlist)
- **Les sous-agents** (`spawn_session(agent_id)`) sont des AgentWorkflow one-shot, sans persistance, contexte isolé du parent, avec l'allowlist de leur propre agent
- Le parent ne voit que la réponse finale du sous-agent (string)

## Agents, Tools & Task Queues

- Modèle cible et état : `docs/architecture.md`
- Agent = persona logique identifiée par `agent_id` (prompt, skills, allowlist d'outils). Table `agents` = source de vérité ; `agents.yaml` = seed (insère les agents absents, n'écrase jamais)
- Queue = capacité, jamais un agent ni une machine. Chaque worker lit `worker.yaml` (`WORKER_CONFIG`) : `queue`, `tools` (globs), `mcp`, `workflows`, et publie ses tools dans la table `tools`
- Sans `worker.yaml` : le worker sert workflows + tous les tools sur `WORKFLOW_QUEUE` (défaut `agent`)
- Workflows (Session/Agent/LLM) sur `WORKFLOW_QUEUE`. Chaque appel d'outil part sur la queue de l'outil (`ExecuteTool` générique, queue fixée dans `ActivityOptions`)
- Les workers gardent un catalogue en mémoire (agents + tools) rechargé toutes les 30 s ; `ListTools(agentID)` applique l'allowlist
- Skills chargés depuis un repo Git (prod) ou `./skills` (dev)

## Store (PostgreSQL)

- `messages` : historique de conversation par session (JSONB)
- `memory` : key-value scope (user/project/session)
- `task_logs` : suivi des taches schedulees
- Persistance geree par SessionWorkflow, pas par AgentWorkflow
- Config via `DATABASE_URL` env var
- Migration automatique au demarrage (CREATE TABLE IF NOT EXISTS)

## Conventions

- `SystemPrompt` dans les workflow inputs = override manuel ; si vide, prompt construit depuis l'agent (`agent_id`)
- Les tool results remontent comme string au parent
- Les notifications SSE passent par `/internal/notify` (prod) ou in-memory hub (dev)
- ask_user fonctionne pour les sous-agents (SSE route vers le bon sessionID via le workflowID)
