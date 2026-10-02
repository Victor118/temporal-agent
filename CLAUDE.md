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
- Back-office `/admin` (htmx + `html/template`, `web/admin`) : agents et utilisateurs, réservé au rôle `admin`. Le JSON du panneau admin du chat est sous `/api/admin` (admin aussi)
- Comptes (`auth/`) : login email + mot de passe argon2id, cookie `session_token` (token aléatoire, seul son hash est en base dans `login_sessions`). Premier admin : `agent user create --email … --admin`. Une route de session vérifie que l'utilisateur en est membre (`requireMember`)
- Worker = Temporal worker + activities + tools publiés sur sa queue (`worker.yaml`)
- Communication worker → serveur via `/internal/notify` ; le reste passe par PostgreSQL (agents, tools, skills_version)

## Workflows

- **SessionWorkflow** : orchestration long-lived, gère la persistance (LoadContext/PersistContext via PostgreSQL)
- **AgentWorkflow** : boucle ReAct (LLM + tools), `agent_id` obligatoire (prompt, skills, allowlist)
- **Les sous-agents** : chaque agent est un tool `agent_<id>(task)` généré par le catalogue (pas publié par un worker), soumis à l'allowlist comme les autres (`agent_*` = tous). L'appeler lance un AgentWorkflow one-shot, sans persistance, contexte isolé du parent, avec l'allowlist de son propre agent
- Le parent ne voit que la réponse finale du sous-agent (string)

## Agents, Tools & Task Queues

- Modèle cible et état : `docs/architecture.md`
- Agent = persona logique identifiée par `agent_id` (prompt, skills, allowlist d'outils). Table `agents` = source de vérité, éditée via `/admin` ; `agents.yaml` = seed, appliqué seulement si la table est vide
- Queue = capacité, jamais un agent ni une machine. Chaque worker lit `worker.yaml` (`WORKER_CONFIG`) : `queue`, `tools` (globs), `mcp`, `workflows`, et publie ses tools dans la table `tools`
- Sans `worker.yaml` : le worker sert workflows + tous les tools sur `WORKFLOW_QUEUE` (défaut `agent`)
- Workflows (Session/Agent/LLM) sur `WORKFLOW_QUEUE`. Chaque appel d'outil part sur la queue de l'outil (`ExecuteTool` générique, queue fixée dans `ActivityOptions`)
- Les workers gardent un catalogue en mémoire (agents + tools) rechargé toutes les 30 s ; `ListTools(agentID)` applique l'allowlist
- Skills chargés depuis un repo Git (prod) ou `./skills` (dev)

## Store (PostgreSQL)

- `messages` : historique de conversation par session (JSONB), append-only ; `msg_key` = cle d'idempotence (`{run id}-{tour}:{index}`, ou `sched:{id}:{run}` pour un resultat de tache), lecture `ORDER BY id`
- `users` (email unique insensible à la casse, rôle `admin`|`user`, `disabled_at`), `login_sessions`
- `sessions` (`created_by`) + `session_members` : une session est partagée entre ses membres ; tout membre peut en ajouter, chacun peut la quitter, seul le créateur la supprime
- Chaque message utilisateur porte son auteur (`user_id`, `author`). Le serveur enregistre **chaque message humain dès réception** (`deliverMessage`, web et Telegram), qu'il appelle l'agent ou non ; l'agent ne voit que ce que contient l'historique, et le recharge en entier à chaque tour
- `sessions.agent_mode` décide si un message appelle l'agent : `auto` (seul dans la session : chaque message ; à plusieurs : sur `@agent` ou `@<agent_id>`), `always`, `mention`. S'il l'appelle, le signal `user-message` transporte `{text, user_id, user_name, stored: true}` et le tour n'ajoute pas le message (déjà en base). Le tour répond à son auteur : c'est **sa** mémoire qui est chargée, et ses tools (`save_user_memory`, `schedule_task`) agissent pour lui
- Un message humain peut être stocké entre un tool call et son résultat (un membre écrit pendant le tour) : `convertMessages` le replace après les résultats (`deferInterleaved`), l'API LLM l'exige
- Fork : `sessions.parent_session_id` + `forked_at_message_id` + `forked_by`. `POST /sessions/{id}/fork {message_id}` crée la session (même agent, seul membre = celui qui forke) puis `ForkSessionWorkflow` résume le parent jusqu'à ce message (`SummarizeConversation`, modèle `SUMMARY_MODEL` ou `LLM_MODEL`) et l'écrit comme premier message (`kind = fork_summary`). Tant que le résumé n'est pas là, le fork refuse les messages (409). Supprimer le parent laisse le fork entier, sans lien
- `memory` : key-value scope (user/project/session)
- `task_logs` : suivi des taches schedulees
- AgentWorkflow ecrit ses messages au fil du tour quand `TurnKey` est fourni ; SessionWorkflow reecrit le meme delta en fin de tour (les memes cles, donc sans effet si deja ecrit). Un sous-agent n'a pas de `TurnKey` et ne persiste rien
- Un tour qui echoue ne doit pas perdre son transcript : `AgentWorkflow` renvoie `Error` dans sa sortie plutot qu'une erreur de workflow (un workflow en echec ne rend aucun resultat)
- Ne jamais persister un message assistant portant des tool calls sans ses tool results : le tour suivant serait rejete par l'API LLM
- Config via `DATABASE_URL` env var
- Migration automatique au demarrage (CREATE TABLE IF NOT EXISTS)

## Conventions

- `SystemPrompt` dans les workflow inputs = override manuel ; si vide, prompt construit depuis l'agent (`agent_id`)
- Les tool results remontent comme string au parent
- Les notifications SSE passent par `/internal/notify` (prod) ou in-memory hub (dev)
- ask_user fonctionne pour les sous-agents (SSE route vers le bon sessionID via le workflowID)
