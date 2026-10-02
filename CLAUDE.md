# CLAUDE.md

## Build & Run

- Go n'est pas installé localement. Toujours utiliser Docker :
  - `cd /home/victor/dev/temporal-agent && docker compose up -d`
  - `docker compose exec agent go build ./...`
  - `docker compose exec agent go test ./...`
- Pas de hot-reload : le `CMD` du Dockerfile compile une fois au démarrage → `docker compose restart agent` après une modification
- Port 8888 exposé pour l'API HTTP

## Architecture

- 3 modes : `agent server`, `agent worker`, `agent dev` (les deux combinés). `worker` et `dev` construisent leurs workers par le même `newWorkerRuntime` (`cmd/agent/runtime.go`) : mêmes workflows, activities et outils ; seuls diffèrent la destination des notifications web et la source des skills. Les outils Claude Code ne sont enregistrés que là où la CLI `claude` est installée. Fournisseur LLM choisi par `provider.New(LLM_PROVIDER)` (registre, `provider.Register`)
- Server = API HTTP + SSE hub + catalogue agents + skills versioning
- Back-office `/admin` (htmx + `html/template`, `web/admin`) : agents et utilisateurs, réservé au rôle `admin`. Le JSON du panneau admin du chat est sous `/api/admin` (admin aussi)
- Règles des sessions dans `session.Service` (`session/`) : ouvrir, livrer un message, forker, inviter, quitter, supprimer, annuler, répondre, états lus dans Temporal. `cmd/agent` n'a que des adaptateurs minces : `api` (JSON), `ui` (htmx), `telegramChannel` ; ils changent une session par le service et ne lisent le store que pour afficher (`readStore`)
- Interface de chat (`web/chat` + `cmd/agent/ui.go`) : htmx + `html/template`, trois colonnes (arbre des sessions et forks, fil, rail) et vue carte (`/s/{id}/map`). Rendu côté serveur ; le flux SSE de la session (`/sessions/{id}/stream`) ne sert que de sonnette pour recharger le fil, qui renvoie la zone de saisie en hors-bande. États des sessions lus dans Temporal (4 requêtes de visibilité), partagés 3 s entre toutes les requêtes et invalidés après une action. Construction des vues pure et testée (`web/chat/views.go`). Jamais de HTML construit côté client depuis du JSON : le Markdown est rendu côté serveur (`chat.Markdown`, HTML brut échappé)
- Comptes (`auth/`) : login email + mot de passe argon2id, cookie `session_token` (token aléatoire, seul son hash est en base dans `login_sessions`). Premier admin : `agent user create --email … --admin`. Échecs de connexion limités en mémoire (`auth.LoginLimits` : 10 par compte sur 15 min, au-delà 429) et journalisés avec l'adresse. La limite par adresse (20) n'existe que si l'adresse du client est connue : `TRUSTED_PROXIES` (CIDR/adresses des reverse proxies, dont on lit `X-Forwarded-For` depuis la droite ; `none` = pas de proxy). Vide (défaut) = pas de limite par adresse : derrière un proxy, tous les clients partagent la sienne et vingt échecs bloqueraient tout le monde. De même pour une requête d'un proxy de confiance qui ne nomme aucun client (`ClientAddrs.Of` = `""`, journalisé une fois). Une route de session vérifie que l'utilisateur en est membre (`requireMember`)
- Worker = Temporal worker + activities + tools publiés sur sa queue (`worker.yaml`)
- Communication worker → serveur via `/internal/notify` ; le reste passe par PostgreSQL (agents, tools, skills_version). `/internal/notify` exige `Authorization: Bearer $INTERNAL_API_KEY` (vide = tout refusé) ; le port interne ne doit pas être publié
- Webhooks : `/webhooks/telegram` vérifie `X-Telegram-Bot-Api-Secret-Token` = `TELEGRAM_WEBHOOK_SECRET`, `/webhooks/skills` la signature GitHub avec `SKILLS_WEBHOOK_SECRET`. Sans secret, la route n'est pas montée (défaut fermé)

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
- Chaque message utilisateur porte son auteur (`user_id`, `author`). Le serveur enregistre **chaque message humain dès réception** (`session.Service.Deliver`, web et Telegram), qu'il appelle l'agent ou non ; l'agent ne voit que ce que contient l'historique, et le recharge en entier à chaque tour
- `sessions.agent_mode` décide si un message appelle l'agent : `auto` (seul dans la session : chaque message ; à plusieurs : sur `@agent` ou `@<agent_id>`), `always`, `mention`. S'il l'appelle, le signal `user-message` transporte `{text, user_id, user_name, stored: true}` et le tour n'ajoute pas le message (déjà en base). Le tour répond à son auteur : c'est **sa** mémoire qui est chargée, et ses tools (`save_user_memory`, `schedule_task`) agissent pour lui
- Un message humain peut être stocké entre un tool call et son résultat (un membre écrit pendant le tour) : `convertMessages` le replace après les résultats (`deferInterleaved`), l'API LLM l'exige
- Fork : `sessions.parent_session_id` + `forked_at_message_id` + `forked_by`. `POST /sessions/{id}/fork {message_id}` crée la session (même agent, seul membre = celui qui forke) puis `ForkSessionWorkflow` résume le parent jusqu'à ce message (`SummarizeConversation`, modèle `SUMMARY_MODEL` ou `LLM_MODEL`) et l'écrit comme premier message (`kind = fork_summary`). Tant que le résumé n'est pas là, le fork refuse les messages (409). Supprimer le parent laisse le fork entier, sans lien
- `memory` : key-value scope (user/project/session)
- `task_logs` : suivi des taches schedulees, chacune a son `user_id` : `list_schedules` et `cancel_schedule` ne voient que celles de l'utilisateur du tour (aucun outil sans utilisateur identifie). `query_workflow` n'interroge que les workflows de la session appelante
- AgentWorkflow ecrit ses messages au fil du tour quand `TurnKey` est fourni ; SessionWorkflow reecrit le meme delta en fin de tour (les memes cles, donc sans effet si deja ecrit). Un sous-agent n'a pas de `TurnKey` et ne persiste rien
- Un tour qui echoue ne doit pas perdre son transcript : `AgentWorkflow` renvoie `Error` dans sa sortie plutot qu'une erreur de workflow (un workflow en echec ne rend aucun resultat)
- Ne jamais persister un message assistant portant des tool calls sans ses tool results : le tour suivant serait rejete par l'API LLM
- Config via `DATABASE_URL` env var
- Migration automatique au demarrage (CREATE TABLE IF NOT EXISTS)

## Conventions

- `SystemPrompt` dans les workflow inputs = override manuel ; si vide, prompt construit depuis l'agent (`agent_id`)
- Les tool results remontent comme string au parent. Un workflow-tool renvoie un `tool.Result` (`content`, `is_error`) ou un type qui en porte les champs (`ClaudeCodeOutput.Content`) ; seul un sous-agent (`agent_<id>`) est décodé par type (`AgentWorkflowOutput.Response`)
- Ce qu'est un outil est déclaré avec lui (`tool.Tool`) et publié dans la table `tools` : `Sensitive` (affiché dans `/admin`), `PrivateInput` (entrée cachée aux membres et aux résumés de fork, ex. `save_user_memory`), `NeedsCallContext` (le workflow-tool reçoit `tool.CallContext` : chaîne d'agents, canal, ex. `ask_user`). Aucune liste de noms d'outils dans le code
- Tout sous-processus lancé pour un modèle (`exec`, Claude Code) passe par `subproc` : environnement en liste blanche (`subproc.Env`, variables Go : `subproc.GoToolchainNames`), groupe de processus tué au retour de la commande comme à l'annulation (`subproc.KillGroup`), tous les processus de `RUN_AS_UID` tués dès qu'aucune commande du worker ne tourne sous cet uid (`Identity.Hold`/`KillStrays`, aussi avant de reprendre un clone) — cet uid doit donc être dédié à ce rôle, et exécution sous l'utilisateur `RUN_AS_UID`/`RUN_AS_GID` (`subproc.Identity` : `Apply`, `Env` = son HOME et ses caches Go, jamais ceux du worker). Les deux images créent `agent-run` (uid 10001) et posent `RUN_AS_UID=10001`. Worker root sans `RUN_AS_UID` = `exec` et Claude Code refusés (`subproc.CheckRunAs`). Au démarrage le worker donne à cet utilisateur son HOME, le workspace et `CLAUDE_CONFIG_DIR` ; les outils fichiers (`read_file`, `write_file`, `edit_file`, `list_directory`, `grep`, `glob`), qui tournent sous l'utilisateur du worker, n'ouvrent rien par un chemin joint : tout passe par un `os.Root` sur le workspace (`tool/workspace.go`), qui refuse à l'ouverture un lien qui en sort (lien absolu compris), et ils donnent ce qu'ils créent par descripteur (`Identity.GiveFile`) ; `PrepareWorkspace` lui donne le clone, `InspectWorkspace`/`PushBranch` le reprennent (`subproc.Reclaim` du dossier, `subproc.ReclaimTree` de tout `.git` : propriétaire, droits fermés aux autres) avant tout git du worker. Ce n'est toujours pas un bac à sable (lecture de ce que cet utilisateur peut lire)
- Claude Code : `repo` (et `ref`/`base`) viennent du modèle. Le worker n'accepte que les dépôts de `CLAUDE_CODE_REPOS` (globs, vide = tout refusé), vérifiés dans `PrepareWorkspace` et `PushBranch` (là où vit la clé), refuse ce qui commence par `-`, et passe `--` / `--end-of-options` à git. Le git du worker ne lit jamais la configuration que le run a pu écrire : `PrepareWorkspace` garde une copie de `.git/config` hors du workspace (`<dir>.gitconfig`) et supprime `.git/hooks` ; `InspectWorkspace` et `PushBranch` la réécrivent toujours, en fichier neuf du worker même inchangée (et retirent `commondir`/`config.worktree`), avant toute commande. Une configuration modifiée par le run = rien n'est poussé. Toute commande git du worker : `-c core.hooksPath=/dev/null -c core.fsmonitor=false`, `GIT_CONFIG_NOSYSTEM=1`, `GIT_CONFIG_GLOBAL=/dev/null`, environnement filtré (`subproc.Env`)
- `web_fetch` ne se connecte qu'à des adresses publiques (vérifiées dans le dialer, après résolution DNS et à chaque redirection), en http(s) uniquement
- Canaux : un canal = un nom (`web`, `telegram`) porté par la session et les workflows. Sortie : un `activity.Notifier` par canal, enregistré au démarrage du worker dans `NotificationActivities.Notifiers` (`HubNotifier` pour le web, `telegram.Notifier`) ; entrée : un `inboundChannel` (`Name()` + `http.Handler`) monté sur `/webhooks/<nom>`. Ajouter un canal = deux implémentations, sans toucher aux workflows
- Les notifications SSE passent par `/internal/notify` (prod, `activity.HTTPNotifier`, dont l'échec fait échouer l'activity : la politique de retry s'applique ; le worker vérifie sa clé au démarrage) ou in-memory hub (dev, `HubNotifier`)
- Telegram : une réponse longue part en plusieurs messages, chacun retenté seul ; une fois un message parti, un échec est un `activity.PartialDelivery`, jamais retenté (il renverrait le début). `NotifyStep` vers un canal : `channelNotifyTimeout` (1 min)
- Une tâche planifiée tourne pour son utilisateur (`UserID`, `LoadUserMemory` : sa mémoire, sans historique de session)
- ask_user fonctionne pour les sous-agents, web comme Telegram : un sous-agent hérite `Channel`/`ChannelID` de son parent (ses questions vont à l'utilisateur ; sa réponse finale ne part que vers le parent, seul le tour de session — `TurnKey` non vide — répond sur le canal), et une réponse Telegram cherche les `AskUserWorkflow` en cours dont l'ID commence par `<session>-`
