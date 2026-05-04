# TODO — Temporal Agent

## Infra & Déploiement

- [x] Séparer l'API Go du worker — Cobra avec `agent server`, `agent worker`, `agent dev`
- [x] Pont de notification worker→server via `HTTPNotifier` + `/internal/notify`
- [x] Docker Compose complet pour déployer le projet sur une machine (server, worker, Temporal, PostgreSQL, Temporal UI)
- [ ] Outil de management des workers : déployer, scaler, monitorer par task queue
- [ ] Images Docker spécialisées par type de worker : `worker-base` (Go + Temporal SDK), `worker-coder` (+ CGC, git, toolchain, MCP CGC local en sidecar, clone repo → index → mcp start au démarrage), `worker-devops` (+ terraform, kubectl, etc.)
- [ ] Temporal Cloud pour la prod (serveur local suffisant pour le dev)
- [ ] Dashboard Grafana sur les métriques Temporal (queue depth, schedule-to-start latency)

## Sessions & Utilisateurs

- [x] Ajouter la notion de `user_id` (table `sessions` avec `user_id`, `session_id`, `created_at`, `title`) — cookie pseudo côté front, `POST /sessions` avec `user_id`
- [x] Permettre la reprise de session — `sendMessage` détecte un workflow terminé et en relance un nouveau automatiquement (workflow ID suffixé par timestamp, même `session_id`)
- [x] Endpoint `GET /users/{id}/sessions` pour lister les sessions précédentes (avec statut actif/inactif via Temporal)

## Tools & Workflows

- [x] `spawn_agent` — child workflow pour lancer un sous-agent autonome (`tool/spawn.go`)
- [x] `ask_user` — child workflow pour poser une question à l'utilisateur et attendre sa réponse via signal (`tool/ask_user.go`, `workflow/ask_user.go`)
- [x] Fire-and-forget — flag `FireAndForget` sur le Tool, skip le `.Get()` dans `agent.go`, retourne le workflow ID au LLM
- [x] `query_workflow` — tool activity pour query l'état d'un workflow par ID (`tool/query_workflow.go`)
- [x] `TaskQueue` par tool pour router vers des workers spécialisés — déjà supporté via `TaskQueue` sur le `Tool` struct
- [x] `schedule_task`, `list_schedules`, `cancel_schedule` — tâches planifiées via Temporal Schedules (`tool/schedule.go`, `workflow/scheduled_agent.go`, `activity/delivery.go`)
- [x] Support multi task queues par worker (`TASK_QUEUES=q1,q2,q3` → lance N workers Temporal)
- [ ] Système de hooks dans la boucle ReAct (`beforeCallLLM`/`afterCallLLM`/`beforeUseTool`/`afterUseTool`) — interface unique `HookInput → HookOutput`, deux modes d'exécution : déterministe (fonction pure, exécutée dans le workflow) ou non-déterministe (activity Temporal, pour RAG, API externes, etc.). Use cases : filtrage/troncation des résultats tools, injection de contexte RAG, métriques, validation inputs, blocage conditionnel
- [ ] Système de webhooks entrants avec abonnement : tool `subscribe_webhook` pour qu'un workflow s'abonne à un type d'événement (ex: `github.pull_request.merged` + filtre), registre d'abonnements (table ou mémoire), endpoint générique `POST /webhooks/{source}` qui parse l'événement et signale tous les workflows abonnés
- [ ] Worker registration & dynamic task queue assignment — les workers se déclarent avec un ID + tags, le server assigne les queues dynamiquement (spec: `docs/worker-registration.md`)
- [ ] Queue partagée `llm` pour les calls LLM — tous les workers y écoutent, `CallLLM` routed via `TaskQueue: "llm"` dans les activity options, les tools/skills restent sur les queues spécialisées

## Agents & Skills dynamiques

- [ ] **"Recrutement" d'agents** — permettre à un agent de proposer la création d'un nouvel agent spécialisé quand il identifie un manque de compétence dans l'équipe. Flow : agent rédige une fiche (nom, description, skill/prompt) → `ask_user` pour validation CEO → skill créé dans le repo → agent ajouté au catalogue. Pattern CEO/employé : l'agent propose, l'humain approuve.
- [ ] Workflow de validation humaine pour la création de skills (éviter prompt injection et dérive)
- [ ] Versioning et historique des skills auto-générés (registre immuable)

## Front

- [x] Front HTMX servi directement par l'API Go (`web/web.go`, `web/templates/layout.html`)
- [x] SSE natif HTMX (`hx-ext="sse"`) pour le streaming des réponses
- [x] Rendu Markdown des réponses (marked.js)
- [x] Affichage de la chaîne d'agents (breadcrumb) dans les `ask_user`
- [x] Écran de login (pseudo) + liste des sessions (actives/inactives) + navigation entre sessions
- [ ] **Mode vocal push-to-talk (v1)** — bouton micro dans le chat, enregistrement audio via MediaRecorder, envoi au serveur, transcription via Whisper (container Docker self-hosted), le texte transcrit est envoyé comme message classique dans la session. Réponse de l'agent en TTS (Piper, self-hosted) avec playback audio côté front. Pas de VAD/endpointing, l'utilisateur contrôle le micro manuellement.
