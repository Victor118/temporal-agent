# Modèle cible — temporal-agent

Ce document fixe le modèle vers lequel le projet converge. Il sert de repère :
toute évolution doit pouvoir se situer par rapport aux concepts et aux règles
ci-dessous. Les écarts avec le code actuel sont listés en fin de document.

> Statut : **cible**. Une partie n'est pas encore implémentée (voir « État actuel »).

## But

Faire tourner des agents LLM de manière **durable** grâce à Temporal : un agent
reçoit un message, raisonne, appelle des outils, délègue à d'autres agents,
pose des questions à l'utilisateur, planifie des tâches — et tout cela survit
aux redémarrages, aux pannes de workers et aux attentes longues.

## Concepts

| Concept | Ce que c'est | Source de vérité |
|---|---|---|
| **Agent** | Une persona : nom, description, skills, outils autorisés (allowlist). Ne dit rien de *où* il s'exécute. | Table `agents` (éditable via l'UI). `agents.yaml` sert uniquement de seed. |
| **Skill** | Des instructions métier (markdown) injectées dans le prompt d'un agent. | Repo Git (`SKILLS_REPO`) ou `./skills` en dev. |
| **Tool** | Une action exécutable : nom, description, schéma d'entrée (JSON Schema). Codé en Go ou découvert via un serveur MCP. | Déclaré par les workers, publié dans la table `tools`. |
| **Worker** | Un processus qui expose un ensemble d'outils sur **une queue**. | Sa propre config locale (`worker.yaml`). |
| **Queue** | Une **capacité** : un pool de workers équivalents capables d'exécuter les mêmes outils. | Déclarée par la config worker. |
| **Session** | Une conversation et son historique (`SessionWorkflow`, long-lived). | Temporal + table `messages`. |
| **Exécution** | Un tour d'agent (`AgentWorkflow`) et ses enfants : sous-agents, questions, workflows déterministes (ex. Claude Code). | Temporal + arbre d'exécution en base (prévu). |

## Règles

1. **Le LLM décide *quoi*, le code décide *comment*.**
   Toute séquence d'étapes obligatoires (cloner, brancher, pousser, nettoyer…)
   est un workflow déterministe exposé comme un seul outil. On ne compte pas sur
   le LLM pour enchaîner correctement des étapes.

2. **Une queue = une capacité. Jamais un agent, jamais une machine.**
   - Tous les workers d'une même queue exposent exactement les mêmes outils.
   - Des outils identiques s'exécutent sur la même queue. La cohérence est de la
     responsabilité de celui qui paramètre les workers.
   - Le nom de la queue ne dépend pas de l'instance (pas de hostname) : plusieurs
     réplicas partagent la queue, Temporal répartit la charge.

3. **La cohérence d'état est gérée par l'appelant, via une session Temporal.**
   Pour les outils à état (fichiers, exec, workspace), le workflow appelant ouvre
   une session Temporal sur la queue de l'outil ; Temporal épingle l'instance.
   Aucune queue n'identifie une machine.

4. **Un agent est logique et peut tourner n'importe où.**
   `AgentWorkflow` est de l'orchestration pure. Ce qu'un agent peut faire est
   défini par son allowlist, pas par la queue sur laquelle il tourne.

5. **Une seule source de vérité par donnée, un seul écrivain.**
   - Agents : le serveur écrit (UI / API / import du seed), les workers lisent.
   - Tools : chaque worker écrit ses propres outils, tout le monde lit.
   - Skills : Git.

6. **L'allowlist est appliquée par le code, pas par le prompt.**
   Un appel à un outil hors allowlist (hallucination, injection) produit un
   `tool_result` d'erreur sans exécuter d'activity. L'accès est refusé par
   défaut : un agent sans allowlist, avec `[]`, ou inconnu n'a aucun outil ;
   tout donner exige un `"*"` explicite.

## Composants

```mermaid
flowchart LR
    UI[UI web / Telegram] --> S[Serveur<br/>API + SSE + config agents]
    S -->|signal / start| T[(Temporal)]
    S <--> DB[(PostgreSQL)]

    subgraph WF[Workers — queue workflows]
        SW[SessionWorkflow]
        AW[AgentWorkflow]
        LLM[CallLLM]
    end

    subgraph TC[Worker — queue tools-core]
        TC1[memory_*, web_*, schedule_*...]
    end

    subgraph TG[Worker — queue tools-github]
        TG1[MCP github_*]
    end

    subgraph TCC[Worker — queue tools-claude-code]
        TCC1[ClaudeCodeWorkflow + activities]
    end

    T <--> WF
    T <--> TC
    T <--> TG
    T <--> TCC
    WF <--> DB
    TC -->|publie ses tools| DB
    TG -->|publie ses tools| DB
    TCC -->|publie ses tools| DB
```

- **Serveur** : API HTTP, SSE, UI de configuration. Seul écrivain de la table
  `agents`. Importe `agents.yaml` si la table est vide ou sur commande explicite.
- **Workers** : lisent leur `worker.yaml`, enregistrent leurs outils dans la
  table `tools` (nom, schéma, kind, queue), écoutent leur queue. Un même binaire
  peut aussi écouter la queue des workflows. Un outil déjà publié sur une autre
  queue n'est repris que si Temporal ne voit plus de worker sur cette queue
  (`DescribeTaskQueue`) ; sinon c'est un conflit, loggé en erreur. Pas de
  heartbeat maison : Temporal fait foi sur la vivacité des queues.
- **Mode dev** : serveur + worker dans le même processus.

### Configuration

`agents.yaml` (seed, lu par le serveur) :

```yaml
agents:
  - id: code-reviewer
    name: Code Reviewer
    description: Reviews code changes...
    skills: [code-review]
    tools: [github_*, claude_code, web_fetch]   # allowlist (globs)
```

`worker.yaml` (un par déploiement) :

```yaml
queue: tools-github
workflows: false        # true = sert aussi la queue des workflows (WORKFLOW_QUEUE)
mcp:
  - name: github
    url: http://mcp-github:3001
    transport: http
tools: [github_*]
```

### Données

| Table | Contenu | Écrivain |
|---|---|---|
| `agents` | définition des agents (+ `tools` allowlist) | serveur |
| `tools` | outil → queue, schéma, kind, propriétés (`sensitive`, `private_input`, `needs_call_context`), `schema_hash` | workers |
| `messages`, `sessions`, `memory`, `users`, `task_logs` | données runtime | workflows / serveur |
| `skills_version` | signal de rechargement des skills | serveur |
| `agent_executions`, `user_questions` | arbre d'exécution, questions (prévu) | workflows |

## Flux principaux

### Tour de conversation

1. L'utilisateur envoie un message → le serveur le stocke et signale
   `SessionWorkflow`.
2. `SessionWorkflow` prend pour instantané l'ID du message (dans le signal)
   et lance `AgentWorkflow(agentID)` avec une clé de tour. L'historique ne
   passe jamais par les workflows.
3. `AgentWorkflow` charge le nom de l'agent et la liste des outils
   autorisés : `tools` ∩ allowlist, triée par nom (préfixe de cache stable).
4. Boucle ReAct : `CallLLM` → appels d'outils → résultats → `CallLLM`…
   Chaque `CallLLM` reçoit des références (instantané, clé du tour, noms des
   outils, de quoi bâtir le prompt) et construit la requête : historique
   chargé et ordonné, définitions d'outils et prompt système relus du
   catalogue. Une requête trop grosse (`LLM_MAX_CONTEXT_BYTES`) arrête le
   tour : il faut forker. L'agent écrit ses messages au fil du tour.
5. Réponse finale → `SessionWorkflow` réécrit le tour (sans effet si déjà
   écrit) → SSE.

### Plusieurs agents dans une session

Un message appelle les agents qu'il mentionne (`@<mention>`, au plus trois),
résolus par le serveur ; sans mention, l'agent de la session selon son mode.
`SessionWorkflow` lance un tour par agent, l'un après l'autre, sur le même
instantané : chacun lit aussi les tours précédents du message, et voit donc
leurs réponses. Un message écrit pendant ces tours n'est lu ni par eux ni
entre eux : il vient après le bloc qu'ils forment, et attend son tour. Un échec ou un arrêt
coupe la suite. Chaque agent lit les tours des autres comme du texte signé
(`[agent Nom (@mention)]`, appels d'outils et résultats tronqués compris),
jamais comme ses propres messages ni comme des blocs d'outils. Une note en fin
de prompt cite le message et dit à chacun sa part. Toute l'installation peut
être appelée : restreindre les agents d'une session est à venir.

### Appel d'un outil

1. Le LLM appelle `github_list_issues`.
2. Le workflow vérifie l'allowlist, sinon `tool_result` d'erreur.
3. Il résout la queue de l'outil (`tools-github`) et dispatche `ExecuteTool`
   sur cette queue, avec un `ScheduleToStartTimeout` → « outil indisponible »
   si aucun worker n'écoute.
4. Outil de type workflow → child workflow sur la queue de l'outil.

### Outils à état

Le workflow appelant ouvre une session Temporal (`CreateSession`) sur la queue
des outils concernés ; tous les appels de la session vont sur la même instance.
Les workers de ces queues activent `EnableSessionWorker`.

### Sous-agent

Chaque agent est aussi un outil, `agent_<id>(task, model?)`, généré par le
catalogue depuis la table `agents` (aucun worker ne le publie). L'appeler lance
un `AgentWorkflow` enfant sur la queue de workflows du parent, contexte isolé,
avec **l'allowlist de l'agent enfant** (jamais celle du parent). Le parent ne
reçoit que la réponse finale.

La délégation passe donc par l'allowlist comme le reste : `agent_code-reviewer`
autorise un agent, `agent_*` tous les autres. Un agent ne reçoit jamais son
propre outil. La cible vient du nom de l'outil, pas d'un paramètre rempli par
le LLM : il n'y a pas d'`agent_id` à inventer.

### Workflow déterministe : Claude Code

Outil `claude_code {repo, base, task, mode: dev|debug, title}` →
`ClaudeCodeWorkflow` sur `tools-claude-code` :

1. `CreateSession` (même machine pour toutes les étapes)
2. `PrepareWorkspace` : clone ou worktree, branche `agent/<title>-<id>`
3. `RunClaudeCode` : heartbeat, `ask_user` via MCP
4. `PushBranch` (+ PR optionnelle), exécuté par le code, pas par Claude Code
5. `CleanupWorkspace` : toujours exécuté (defer, contexte déconnecté)
6. Retour au parent : branche, commits, lien PR, compte-rendu

Mode `debug` : lecture seule, push seulement s'il y a un correctif, résultat =
diagnostic.

## État actuel (écarts avec la cible)

- **Pas de sessions d'outils à état** : `session_tools` est parti avec
  `spawn_session` (il ne fonctionnait pas : pas d'`EnableSessionWorker`). À
  refaire avec un flag « outil à état » sur le tool.
- **Back-office** (`/admin`) : édite les agents (allowlist comprise) et les
  utilisateurs, réservé au rôle `admin`. Pas encore d'historique des
  modifications.
- **Fork de session** : depuis un message, la nouvelle session part d'un résumé
  de la conversation jusqu'à ce message, orienté par le but du fork quand il est
  donné (« Pour quoi faire ? »). Le résumé est la première brique de la
  compaction du contexte, pas encore faite. Suppression d'une session qui a des
  forks : autorisée pour l'instant (le fork perd son lien), à revoir pour la
  traçabilité.
- **Rapport d'un fork au parent** : on planifie dans une session, on ouvre un
  fork par chantier, et chaque fork rapporte au parent ce qui s'y est fait
  depuis son dernier rapport (`ReportToParentWorkflow` : résumé en quatre
  parties — fait, décisions, écarts au plan, points ouverts — posté dans le
  parent comme message du membre qui l'envoie, `kind = fork_report`). Le
  rapport n'appelle aucun agent : les membres mentionnent l'agent ensuite s'ils
  veulent sa réaction. Seul le dernier rapport est suivi sur le fork ; pas
  encore d'historique des rapports côté fork.
- **Comptes** : connexion par email et mot de passe (argon2id), sessions de
  connexion en base. Pas encore d'envoi d'emails (réinitialisation du mot de
  passe par un admin seulement).
- **Tous les workflows et activities sont enregistrés sur toutes les queues**
  d'un worker, y compris sa queue d'outils. Les outils de type workflow
  (`ask_user`) tournent donc sur la queue de l'outil.
- **`CallLLM` peut encore être routé par type** via `activity_queues`.
- **Parsing silencieux** de `MCP_SERVERS` (JSON invalide = aucun serveur).
- **Outil MCP retiré pendant que le worker est arrêté** : sa ligne reste dans
  `tools` (seul un retrait vu par un worker en marche la supprime).
- **Déplacer un outil de queue** juste après l'arrêt de l'ancien worker est
  refusé tant que Temporal voit encore ses pollers (~5 min).

## Feuille de route

1. **Modèle** : ce document.
2. **Backend minimal** (fait)
   - table `tools`, publiée par les workers depuis `worker.yaml` ;
   - table `agents` qui fait foi (+ colonne `tools`), écrite par le serveur
     seul, seed depuis `agents.yaml` (appliqué seulement si la table est vide).
3. **Dispatch par outil** (fait) : catalogue en mémoire sur les workers,
   `ListTools(agentID)`, allowlist appliquée, routage vers la queue de l'outil.
4. **Agent identifié par `agent_id`** (fait) : délégation par agent (aujourd'hui
   un outil `agent_<id>` par agent, qui a remplacé `spawn_session(agent_id)`),
   sessions avec `agent_id`, queue de workflows dédiée (`WORKFLOW_QUEUE`),
   suppression de `default_queue`, `TASK_QUEUES`, `TASK_QUEUE_MCP`.
   Sessions d'outils à état sur la queue des outils, validées.
5. **UI en lecture seule** (fait) : `/admin`, agents, outils par queue, queues
   et leurs pollers, skills, alertes de configuration.
6. **UI d'édition des agents** (fait) : création, nom, description, skills,
   allowlist avec aperçu, suppression ; seed seulement sur table vide ;
   révision pour refuser une modification faite sur une copie périmée.
7. **`ClaudeCodeWorkflow`.**
8. **Arbre d'exécution et questions utilisateur** (`agent_executions`,
   `user_questions`, refonte d'`AskUserWorkflow`).

## Questions ouvertes

- Un processus peut-il exposer plusieurs queues d'outils ?
- Outils présents dans chaque binaire (fs, exec) : activés seulement là où
  `worker.yaml` les déclare ?
- Claude Code : clone ou worktree sur miroir ; absence de commit = échec ou
  résultat normal ; PR automatique. (La liste blanche de dépôts existe :
  `CLAUDE_CODE_REPOS`, vérifiée par le worker qui clone et pousse.)
