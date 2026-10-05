# Conception : des machines hors du réseau privé

Statut : **version 1**, proposition, rien n'est fait. Le 5 octobre 2026, à partir de la note « Workers hors réseau privé — orientation d'architecture » et de la discussion qui a suivi : l'option retenue n'est pas celle de la note (Temporal exposé), mais une passerelle (§3).

## 1. Pourquoi

Aujourd'hui, un worker ne fonctionne que dans le réseau privé, où il atteint Temporal **et** PostgreSQL. C'est voulu pour nos propres workers ; ça ne marche pas pour la machine d'un utilisateur :

- **Installation** : rejoindre suppose un VPN ou équivalent, en plus du worker. C'est le principal frein pour un groupe de particuliers, et pour une vitrine SaaS où chacun apporte sa machine.
- **Sécurité** : un worker détient l'accès à PostgreSQL (sessions, mémoires, fichiers de tous les utilisateurs) et à Temporal (tous les historiques). Une machine qu'on ne contrôle pas ne doit avoir ni l'un ni l'autre, VPN ou pas.

Ce qu'on veut sur la machine d'Alice : ce qui a besoin d'elle. Son Claude Code (ou Codex) et son abonnement, ses dépôts et ses identifiants git, ses fichiers, plus tard son modèle (clé d'API ou CLI) pour les tours qui lui répondent.

## 2. Objectifs et non-objectifs

**Objectifs**
- Une **machine** se connecte par une seule commande, `agent connect`, après une inscription par jeton. Connexions sortantes uniquement : ni port ouvert, ni VPN.
- Une machine n'a **aucun accès** à PostgreSQL ni à Temporal. Elle ne voit que les tâches qui lui sont adressées, et ce dont chacune a besoin.
- Temporal garde son rôle : historique, reprise sur erreur, délais. Une machine qui décroche est un worker perdu comme aujourd'hui.
- Le Claude Code de la machine peut appeler les outils de la plateforme (`ask_user`, `publish_file`, `web_search`, un autre agent…) par un MCP local (§8).
- Les mêmes briques servent : runs Claude Code sur la machine de l'utilisateur, `CallLLM` sur sa machine (mode dégradé, feuille de route 3a/3b), `CLIAgentWorkflow`.

**Non-objectifs**
- Nos workers (réseau privé) ne changent pas : ils restent des workers Temporal avec accès à la base, y compris les conteneurs Claude Code actuels.
- Pas de multi-tenant ici : une installation, des utilisateurs. Le SaaS multi-tenant (un namespace par tenant) est un chantier à part ; ce modèle n'y fait pas obstacle.
- Pas de partage d'abonnement : une machine ne travaille que pour son propriétaire (§9), sauf publication explicite en mode API, plus tard.

## 3. Pourquoi pas exposer Temporal

La note proposait d'exposer Temporal aux machines, avec une authentification par worker et un *authorizer* par task queue. Trois façons de faire ont été pesées.

**Le fond du problème : Temporal cloisonne par namespace, pas par task queue.** Un client autorisé à écrire dans un namespace peut, sans code d'autorisation sur mesure :
- écouter n'importe quelle queue, y compris celle des workflows (`agent`) : ses tâches portent l'historique entier des conversations ;
- lire l'historique de n'importe quel workflow, signaler un participant, terminer une session ;
- terminer l'activity d'un autre workflow par son ID (`RespondActivityTaskCompletedById`) : une fausse réponse de `CallLLM` dans la session d'un autre. Les IDs de workflow sont prévisibles, ceux des activities sont de petits entiers ;
- s'il exécute des **workflows** (aujourd'hui tout worker enregistre tous les workflows, `workerWorkflows`, et `AnalyzeRepoWorkflow` tourne sur la queue de l'outil), rendre n'importe quelle commande : signaler, démarrer un workflow, planifier un `exec` sur nos workers.

| | A. Temporal exposé + authorizer maison | B. Un namespace par utilisateur + Nexus | **C. Passerelle, Temporal privé** |
|---|---|---|---|
| Principe | La machine est un worker Temporal, activities seulement ; un authorizer compilé dans notre serveur Temporal n'autorise que le poll des queues `u-<user>:*` et les réponses à ses propres tâches | Sa machine vit dans son namespace ; le nôtre l'appelle par une opération Nexus | La machine ouvre une connexion sortante vers notre serveur ; une activity de notre worker lui relaie la tâche |
| Cloisonnement | Notre code, dans le serveur Temporal (jetons de tâche à décoder, queues de session `<id>@<hôte>`) | Celui de Temporal (namespace) | Notre code, une seule frontière, en HTTPS |
| Temporal Cloud | Impossible (pas d'authorizer maison) | Oui | Oui (Temporal reste privé) |
| Coût | Build de Temporal sur mesure, code critique | Un namespace et un endpoint par utilisateur ; Nexus récent en auto-hébergé | La passerelle, le protocole, l'activity de relais |

**Choix : C.** Une seule frontière d'authentification, la nôtre ; aucune dépendance au mode d'hébergement de Temporal ; et l'API authentifiée qu'elle demande nous faut de toute façon (fichiers §7, conversation d'un tour §10). C'est aussi le principe posé plus tôt : Temporal est privé, au même titre que la base.

## 4. Le modèle

```
Machine d'Alice                                         Notre infra (réseau privé)
┌──────────────────────────────────┐    WSS sortant    ┌─────────────┐          ┌──────────┐
│ agent connect (jeton machine)    │ ────────────────► │ Passerelle  │          │ Temporal │
│  - exécute les directives        │ ◄── directives ── │ (serveur)   │          │          │
│  - outils locaux, Claude Code    │ ─── résultats ──► │             │          │          │
│  - MCP local pour la CLI (§8)    │                   └──────┬──────┘          └────┬─────┘
└──────────────────────────────────┘                          │   API Temporal       │
                                                              └── worker cloud : ────┘
                                                                  workflows + activity de relais
```

- **Machine** : une installation d'`agent connect` (le même binaire `agent`), inscrite au nom d'un utilisateur, avec ses **capacités** (`claude-code`, `codex`, `llm`, `exec`…) détectées au démarrage et annoncées à la connexion.
- **Passerelle** : partie du serveur HTTP. Elle authentifie les machines, garde leurs connexions, leur envoie des directives et reçoit leurs résultats.
- **Directive** : une tâche pour une machine (« exécute cet outil », « fais ce run »), avec un identifiant, une échéance et ce qu'il faut pour l'exécuter.
- **Activity de relais** (`RunOnMachine`, sur nos workers) : le pont entre un workflow et une machine (§6).

La machine n'est **pas** un worker Temporal : elle n'a ni le SDK, ni l'adresse de Temporal. C'est un client qui attend des directives, comme un runner auto-hébergé de GitHub Actions.

## 5. Inscription et connexion

1. Dans « Mes machines » (nouvelle page), l'utilisateur crée un **jeton d'inscription** : court (15 min), à usage unique.
2. Sur sa machine : `agent connect --join <url> <jeton>`. Le serveur rend un **jeton machine** (long, révocable, propre à cette machine), que la machine garde dans son dossier de configuration (droits 0600). Seul son hash est en base (`machines` : id, utilisateur, nom, capacités annoncées, version, créée, vue pour la dernière fois, révoquée).
3. Ensuite, `agent connect` suffit : connexion WSS à `/machines/connect` avec le jeton machine, puis un message `hello` (version du protocole, capacités, directives en cours après une reconnexion).
4. « Mes machines » liste les machines, en ligne ou non, leurs capacités ; on peut en révoquer une (connexion coupée, jeton refusé).

Ce que la machine n'a pas : `DATABASE_URL`, `TEMPORAL_HOST`, `INTERNAL_API_KEY`, la clé LLM du serveur.

## 6. Une directive, de bout en bout

1. Un workflow (chez nous) a une tâche pour une machine : il planifie `RunOnMachine` sur une queue de nos workers, avec une **référence** à la tâche (jamais un gros contenu : §10).
2. `RunOnMachine` choisit la machine (§9), enregistre la directive (ID, machine, jeton de tâche Temporal de l'activity) et la confie à la passerelle, puis rend `activity.ErrResultPending` : **complétion asynchrone**, aucun emplacement de worker tenu pendant les 45 min d'un run.
3. La passerelle envoie la directive à la machine. La machine l'exécute, envoie des signes de vie (`progress`), puis le résultat.
4. La passerelle retransmet chaque signe de vie en heartbeat de l'activity (`client.RecordActivityHeartbeat` avec le jeton de tâche) et, au résultat, la termine (`client.CompleteActivity`). La réponse à un heartbeat dit si l'activity a été annulée : la passerelle envoie alors `cancel` à la machine.
5. Machine déconnectée, serveur redémarré : les heartbeats cessent, l'activity expire (`HeartbeatTimeout`), le workflow relance ou rend l'erreur, comme pour un worker perdu. Un résultat qui arrive pour une directive dont l'activity est finie est jeté.

Garantie : au moins une fois, comme aujourd'hui. Une directive exécutée deux fois doit être sans dommage (clés d'idempotence des fichiers et des messages, déjà en place).

**Plusieurs répliques du serveur.** Une machine est connectée à une réplique. `RunOnMachine` doit l'atteindre : chaque réplique sert une queue Temporal à elle (`gateway-<réplique>`) où elle exécute la remise de la directive, et la connexion d'une machine est notée en base (`machines.connected_to`). Le choix de la machine donne la queue. Phase 1 : une seule réplique.

**Un run Claude Code devient une seule directive.** Aujourd'hui, `AnalyzeRepoWorkflow` enchaîne clone, run, inspection, push, nettoyage dans une session Temporal sur le worker qui porte le clone. Sur une machine, tout se fait localement en une directive : elle clone avec **ses** identifiants git, lance **sa** CLI, inspecte, pousse si demandé, nettoie, et rend le rapport. Plus besoin d'affinité entre étapes. Les règles de sécurité du clone (configuration git réécrite, `CLAUDE_CODE_REPOS`…) protègent nos workers des runs ; sur sa machine, l'utilisateur protège lui-même ses secrets, et on garde les mêmes garde-fous par défaut.

## 7. Fichiers

Le contenu d'un fichier ne passe jamais par Temporal (décision de `publish_file`). Une machine publie par l'API : `PUT /machines/files` avec le **jeton de tour** de la directive (§11) et la clé d'idempotence `(session, tour, appel, nom)` ; le serveur vérifie, écrit (`store.FileStore`, S3 plus tard, avec une URL présignée) et rend la référence (`tool.FileRef`), seule chose qui remonte au workflow. Même règle que `SaveFile` : même clé et même sha256 = l'existant, autre contenu = refus.

Sur une machine, `publish_file(path)` redevient possible (la machine qui a le fichier est celle qui publie).

## 8. Montant : les outils de la plateforme pour la CLI

Le Claude Code de la machine reçoit un serveur MCP **stdio**, lancé avec lui (`--mcp-config`) : `agent mcp-bridge`. La CLI tourne sous un utilisateur isolé ; le pont aussi, donc il ne détient aucun secret.

```
claude (CLI)  ─stdio─►  agent mcp-bridge  ─socket Unix + jeton du run─►  agent connect  ─WSS─►  Passerelle  ─Update─►  workflow du tour
```

1. La CLI appelle un outil MCP ; le pont le passe à `agent connect` par une socket Unix, avec le jeton du run.
2. `agent connect` l'envoie à la passerelle, qui le remet au workflow du tour par une **Workflow Update** (`call_tool`) : une requête-réponse synchrone dans un workflow en cours.
3. Le workflow valide (allowlist de l'agent du tour), exécute l'outil comme d'habitude (`ExecuteTool` sur la queue de l'outil, `ask_user` sur le canal de la session), persiste l'appel et son résultat comme un tour normal, et répond à l'Update.
4. La réponse redescend jusqu'à la CLI.

Tout est dans l'historique, dans le fil et dans les résumés, avec les mêmes droits que les outils d'un tour ordinaire. `publish_file(path)` du pont est traité par `agent connect` lui-même : lecture du workspace du run (`os.Root`), upload (§7), puis la référence passe par l'Update.

Les outils propres à la CLI (Bash, Read, Edit…) restent locaux, sous les permissions Claude Code de l'utilisateur. Seuls les outils de la plateforme passent par le pont.

## 9. Qui exécute quoi

- **Règle** (cas entre particuliers) : un tour tourne pour l'humain qui l'a demandé, sur **sa** machine. Une directive n'est envoyée qu'aux machines de l'auteur du message du tour. Un agent qui en appelle un autre reste dans les machines de cette même personne.
- **Routage** : parmi les machines en ligne de l'auteur qui ont la capacité demandée, la moins occupée (directives en cours, plafond par machine annoncé au `hello`).
- **Aucune machine** : la tâche passe sur nos workers si la capacité y existe et si la politique de l'installation le permet (`runQueue` est déjà le point unique de ce choix), sinon une erreur claire au modèle (« ta machine n'est pas connectée »). La politique (repli permis ou non, qui paie) est à trancher par installation.
- **Plus tard** : un membre « publie » un de ses agents au groupe, qui tourne alors sur sa machine même quand d'autres l'appellent, en mode API seulement (consentement et plafond).

## 10. Ce que la machine reçoit

La directive porte des références ; le contenu se lit par l'API, avec le jeton de tour :
- un run Claude Code : dépôt, ref, tâche, options, le tout petit ;
- `CallLLM` sur la machine (3b) : la requête entière (prompt système, conversation convertie, définitions d'outils), **construite par le serveur** comme aujourd'hui dans `CallLLM`, peut atteindre 2 Mo (`LLM_MAX_CONTEXT_BYTES`), au-delà de la limite d'une entrée Temporal : la machine la lit par `GET /machines/turns/<directive>/request`. Elle n'y trouve que ce que le tour peut lire : sa mémoire à elle (le tour répond à son auteur) et la conversation où les entrées et résultats `PrivateInput` d'autres utilisateurs sont déjà masqués (`conversation.Convert`). La réponse du modèle remonte par le résultat, comme la sortie actuelle de `CallLLM`.

## 11. Sécurité

- **La machine n'est pas de confiance pour le serveur.** Tout ce qu'elle renvoie est vérifié comme une saisie d'utilisateur : tailles, références de fichiers (session et tour du jeton), résultats d'outils (texte pour le modèle, déjà traité comme tel).
- **Jetons** : jeton machine (long, révocable, un par machine, hash en base) pour la connexion ; **jeton de tour** (court, émis avec chaque directive, limité à une session, un tour, un utilisateur et l'allowlist de l'agent, expiré à la fin de la directive) pour l'API et le pont. Le jeton de run du pont local est un autre jeton, propre au run, connu de la CLI seulement par le pont.
- **Ce qu'une machine voit** : ses directives, rien d'autre. Elle ne peut ni lister, ni lire une autre session ; l'API n'accepte que le jeton de tour de la directive en cours.
- **Abus** : plafonds de taille (messages WSS, uploads), débit par machine, directives en cours par machine, révocation immédiate.
- **Le serveur, vu de la machine** : le propriétaire fait confiance à l'installation qu'il rejoint (elle lui envoie des tâches à exécuter avec sa CLI). `agent connect` garde les garde-fous actuels (utilisateur isolé, `CLAUDE_CODE_REPOS` local, plafond de dépense local) que le propriétaire règle chez lui, jamais le serveur.

## 12. Ce qui change dans le code

Inventaire de ce qu'un worker touche aujourd'hui, et que la machine n'aura pas :
- **PostgreSQL** : publication de ses outils (`publishTools`, table `tools`), catalogue rechargé toutes les 30 s (`pollCatalog`, `pollActivityQueues`), `skills_version`, publication de fichiers (`tool.Publisher`), outils de mémoire et de planification (`tool/memory.go`, `tool/schedule.go`), fichiers des documents (`tool/documents.go`).
- **Temporal** : tous les workflows et toutes les activities enregistrés sur chaque queue (`workerWorkflows`), `query_workflow`, client des tâches planifiées, relais.
- **Serveur interne** : `/internal/notify` (un worker d'outils n'en a déjà pas besoin : les avis partent par la queue du tour).

Pour la machine : un nouveau mode `agent connect` qui n'ouvre ni base ni Temporal, n'enregistre que les outils locaux sans dépendance à la base (`exec`, outils fichiers, Claude Code, Codex, plus tard le moteur `llm`), et les annonce au `hello` ; la passerelle les publie dans `tools` sous une queue virtuelle `m-<user>:<capacité>` que `RunOnMachine` reconnaît. Côté serveur : la passerelle, les tables `machines` et `machine_directives`, l'API (§7, §10), `RunOnMachine`, l'Update `call_tool`, la page « Mes machines ».

**Versions** : les machines tournent avec le binaire que leur propriétaire a installé. C'est le premier endroit où une incompatibilité entre versions est réelle même sans prod : le `hello` négocie une version de protocole, et le serveur refuse proprement une machine trop vieille (« mets à jour agent »).

## 13. Phases

| Phase | Contenu | Résultat |
|---|---|---|
| **1** | Passerelle (une réplique), inscription, « Mes machines », `RunOnMachine` avec complétion asynchrone, directive « run Claude Code » (analyse) en une seule directive | Alice lance une analyse avec son abonnement, depuis chez elle, sans VPN |
| **2** | API d'upload et publication depuis la machine ; `implement_feature` sur la machine (push avec ses identifiants) | Les runs rendent des fichiers ; le code part de sa machine |
| **3** | `CallLLM` sur la machine (3a : CLI sans outils comme moteur ; 3b : clé locale), requête lue par l'API | Un utilisateur sans clé d'API sur le serveur a quand même un agent |
| **4** | `agent mcp-bridge`, Update `call_tool`, `CLIAgentWorkflow` | La CLI de l'utilisateur fait tout le tour avec les outils de la plateforme |
| **5** | Plusieurs répliques de passerelle, publication d'un agent au groupe | Montée en charge, partage consenti |

## 14. Questions ouvertes

1. Transport : WebSocket, flux gRPC ou HTTP/2 en long-poll ? WebSocket est le plus simple derrière un proxy ; à confirmer avec le reverse proxy visé.
2. Repli sur nos workers quand la machine de l'auteur est hors ligne : permis par défaut ou non, et qui paie (§9).
3. Une machine sert-elle plusieurs utilisateurs (un serveur d'équipe) ? Exclu en phase 1 : une machine = un propriétaire.
4. La politique des dépôts sur la machine : `CLAUDE_CODE_REPOS` local obligatoire, ou « tout ce que mes identifiants atteignent » par défaut ?
5. Directives en cours après un redémarrage de la passerelle : on les laisse expirer (simple) ou la machine les rattache à la reconnexion (`hello` les liste) pour éviter un run perdu ? La reprise demande que `RunOnMachine` retrouve le jeton de tâche en base.
