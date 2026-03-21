# TODO — Temporal Agent

## Infra & Déploiement

- [x] Séparer l'API Go du worker — Cobra avec `agent server`, `agent worker`, `agent dev`
- [x] Pont de notification worker→server via `HTTPNotifier` + `/internal/notify`
- [x] Docker Compose complet pour déployer le projet sur une machine (server, worker, Temporal, PostgreSQL, Temporal UI)
- [ ] Outil de management des workers : déployer, scaler, monitorer par task queue
- [ ] Temporal Cloud pour la prod (serveur local suffisant pour le dev)
- [ ] Dashboard Grafana sur les métriques Temporal (queue depth, schedule-to-start latency)

## Store & Persistance

- [x] Migrer SQLite → PostgreSQL managé (l'interface `Store` est déjà abstraite, implémenter `PostgresStore`)
- [x] Stocker skills sur un repo Git distant (remplace S3) — `GitStore` + webhook + polling workers
- [ ] Charger les définitions de tools depuis S3/base plutôt qu'en dur dans le code

## Sessions & Utilisateurs

- [ ] Ajouter la notion de `user_id` (table `sessions` avec `user_id`, `session_id`, `created_at`, `title`)
- [ ] Permettre la reprise de session : accepter un `session_id` existant dans `POST /sessions` pour relancer un workflow sur un historique existant
- [ ] Endpoint `GET /users/{id}/sessions` pour lister les sessions précédentes

## Tools & Workflows

- [x] `spawn_agent` — child workflow pour lancer un sous-agent autonome (`tool/spawn.go`)
- [x] `ask_user` — child workflow pour poser une question à l'utilisateur et attendre sa réponse via signal (`tool/ask_user.go`, `workflow/ask_user.go`)
- [x] Fire-and-forget — flag `FireAndForget` sur le Tool, skip le `.Get()` dans `agent.go`, retourne le workflow ID au LLM
- [x] `query_workflow` — tool activity pour query l'état d'un workflow par ID (`tool/query_workflow.go`)
- [x] `TaskQueue` par tool pour router vers des workers spécialisés — déjà supporté via `TaskQueue` sur le `Tool` struct
- [x] `schedule_task`, `list_schedules`, `cancel_schedule` — tâches planifiées via Temporal Schedules (`tool/schedule.go`, `workflow/scheduled_agent.go`, `activity/delivery.go`)
- [x] Support multi task queues par worker (`TASK_QUEUES=q1,q2,q3` → lance N workers Temporal)
- [ ] Worker registration & dynamic task queue assignment — les workers se déclarent avec un ID + tags, le server assigne les queues dynamiquement (spec: `docs/worker-registration.md`)

## Agents & Skills dynamiques

- [ ] **"Recrutement" d'agents** — permettre à un agent de proposer la création d'un nouvel agent spécialisé quand il identifie un manque de compétence dans l'équipe. Flow : agent rédige une fiche (nom, description, skill/prompt) → `ask_user` pour validation CEO → skill créé dans le repo → agent ajouté au catalogue. Pattern CEO/employé : l'agent propose, l'humain approuve.
- [ ] Workflow de validation humaine pour la création de skills (éviter prompt injection et dérive)
- [ ] Versioning et historique des skills auto-générés (registre immuable)

## Front

- [x] Front HTMX servi directement par l'API Go (`web/web.go`, `web/templates/layout.html`)
- [x] SSE natif HTMX (`hx-ext="sse"`) pour le streaming des réponses
- [x] Rendu Markdown des réponses (marked.js)
- [x] Affichage de la chaîne d'agents (breadcrumb) dans les `ask_user`
