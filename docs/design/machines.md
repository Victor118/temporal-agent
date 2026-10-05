# Conception : des machines hors du réseau privé

Statut : **version 2**, proposition, rien n'est fait. Version 1 le 5 octobre 2026, révisée le même jour après une relecture contre le code et le SDK Temporal (v1.33). Les points issus de la relecture sont marqués *[rev. 1…15]*.

Origine : une note proposait d'exposer Temporal aux machines des utilisateurs (sans base de données pour elles). L'option retenue est différente : une passerelle, Temporal et la base restent privés (§3).

## 1. Pourquoi

Aujourd'hui, un worker ne fonctionne que dans le réseau privé, où il atteint Temporal **et** PostgreSQL. C'est voulu pour nos propres workers ; ça ne marche pas pour la machine d'un utilisateur :

- **Installation** : rejoindre suppose un VPN ou équivalent, en plus du worker. C'est le principal frein pour un groupe de particuliers, et pour une vitrine SaaS où chacun apporte sa machine.
- **Sécurité** : un worker détient l'accès à PostgreSQL (sessions, mémoires, fichiers de tous les utilisateurs) et à Temporal (tous les historiques). Une machine qu'on ne contrôle pas ne doit avoir ni l'un ni l'autre, VPN ou pas.

Ce qu'on veut sur la machine d'Alice : ce qui a besoin d'elle. Son Claude Code (ou Codex) et son abonnement, ses dépôts et ses identifiants git, ses fichiers ; plus tard son modèle (clé d'API, ou sa CLI comme moteur) pour les tours qui lui répondent, ce que la feuille de route appelle le mode dégradé : un utilisateur sans clé d'API sur le serveur a quand même un agent.

## 2. Objectifs et non-objectifs

**Objectifs**
- Une **machine** se connecte par `agent connect` (ou le client de bureau, §13), après une inscription. Connexions sortantes uniquement : ni port ouvert, ni VPN.
- Une machine n'a **aucun accès** à PostgreSQL ni à Temporal : ni SDK, ni adresse, ni identifiant. Elle ne voit que les tâches qui lui sont adressées, et ce dont chacune a besoin.
- Temporal garde son rôle côté serveur : historique, délais, issue claire d'un run perdu.
- Les mêmes briques servent : runs Claude Code sur la machine de l'utilisateur, plus tard `CallLLM` sur sa machine, puis `CLIAgentWorkflow` avec les outils de la plateforme (§10).

**Non-objectifs**
- Nos workers (réseau privé) ne changent pas : workers Temporal avec accès à la base, conteneurs Claude Code compris.
- Pas de multi-tenant ici : une installation, des utilisateurs. Le SaaS multi-tenant est un chantier à part ; ce modèle n'y fait pas obstacle.
- Pas de partage d'abonnement : une machine ne travaille que pour son propriétaire (§9).
- Phase 1 : Linux seulement côté machine (§8).

## 3. Pourquoi pas exposer Temporal

**Temporal cloisonne par namespace, pas par task queue.** Un client autorisé à écrire dans un namespace peut, sans code d'autorisation sur mesure :
- écouter n'importe quelle queue, y compris celle des workflows (`agent`) : une tâche de workflow porte l'historique de ce workflow, donc les sorties de `CallLLM` et les résultats d'outils, le contenu des conversations *[rev. 10]* ;
- lire l'historique de n'importe quel workflow, signaler ou terminer un participant ;
- terminer l'activity d'un autre workflow par son ID (`RespondActivityTaskCompletedById`), par exemple une fausse réponse de `CallLLM` dans une autre session : les IDs de workflow sont prévisibles, ceux des activities sont de petits entiers ;
- s'il exécute des workflows (aujourd'hui tout worker enregistre tous les workflows, `workerWorkflows`), rendre n'importe quelle commande : signaler, démarrer un workflow, planifier un `exec` sur nos workers.

| | A. Temporal exposé + authorizer maison | B. Un namespace par utilisateur + Nexus | **C. Passerelle, Temporal privé** |
|---|---|---|---|
| Principe | La machine est un worker Temporal, activities seulement ; un authorizer compilé dans notre serveur Temporal n'autorise que ses queues et ses réponses | Sa machine vit dans son namespace ; le nôtre l'appelle par Nexus | La machine ouvre une connexion sortante vers notre serveur ; une activity lui relaie la tâche |
| Cloisonnement | Notre code, dans le serveur Temporal (jetons de tâche à décoder, queues de session) | Celui de Temporal (namespace) | Notre code, une seule frontière, en HTTPS |
| Atteint l'objectif « ni SDK ni adresse » | Non | **Non** : Nexus auto-hébergé est en preview depuis 1.25 et un handler synchrone est limité à ~10 s, donc la machine exécuterait des workflows dans son namespace *[rev. 10]* | Oui |
| Temporal Cloud | Impossible (pas d'authorizer maison ; Cloud n'a que des rôles par namespace) | Oui | Oui |

**Choix : C.** Une seule frontière d'authentification, la nôtre ; aucune dépendance au mode d'hébergement de Temporal ; et l'API authentifiée qu'elle demande nous faut de toute façon (fichiers, requête d'un `CallLLM` déporté).

## 4. Le modèle

```
Machine d'Alice (Linux)                       Notre infra (réseau privé)
┌───────────────────────────────┐   WSS    ┌────────────────────────┐   HTTP interne   ┌─────────────────────┐
│ agent connect (jeton machine) │ ───────► │ Passerelle (serveur)   │ ◄─────────────── │ Workers (privés)    │
│  exécute les directives       │ ◄─────── │  connexions, routage,  │                  │  CodingRunWorkflow  │
│  Claude Code, git, fichiers   │          │  heartbeats, réponses  │ ── API Temporal ►│  RunOnMachine       │
└───────────────────────────────┘          └────────────────────────┘   (client)       └─────────────────────┘
```

- **Machine** : une installation d'`agent connect` (le même binaire `agent`), inscrite au nom d'un utilisateur, avec ses **capacités** (`claude-code`, plus tard `codex`, `llm`) détectées au démarrage et annoncées à la connexion, et son plafond de directives simultanées.
- **Passerelle** : partie du serveur HTTP. Elle authentifie les machines, garde leurs connexions, leur envoie les directives, reçoit leurs résultats, et parle à Temporal en **client** (heartbeats, complétion), pas en worker.
- **Directive** : une tâche pour une machine, avec un identifiant, une échéance, et ce qu'il faut pour l'exécuter.
- **`RunOnMachine`** : une activity, sur nos workers, qui confie une directive à la passerelle et se termine plus tard (§6).

**Les machines ne sont ni des outils ni des queues** *[rev. 1]*. La table `tools` a une ligne par nom d'outil (une queue), et une queue que personne n'écoute bloquerait le tour jusqu'au délai. Le choix « machine ou worker » se fait donc **dans un workflow**, au moment de l'appel :

- `analyze_repo` et `implement_feature` deviennent des workflow-tools publiés par le **worker principal** (`WORKFLOW_QUEUE`) quand les machines sont activées sur l'installation, ou qu'une queue de repli est configurée. Leur workflow, `CodingRunWorkflow`, commence par l'activity `PickMachine` (une machine en ligne de l'auteur du tour, `CallContext.UserID`, avec la capacité) ;
- machine trouvée : `RunOnMachine` ;
- sinon : l'`AnalyzeRepoWorkflow` / `ImplementFeatureWorkflow` d'aujourd'hui, **inchangé**, en enfant sur la queue de repli (`CLAUDE_CODE_QUEUE`, celle des conteneurs Claude Code), avec sa session Temporal, sa sonde et son attente ; ou une erreur claire au modèle si l'installation n'a pas de repli.

Les conteneurs Claude Code ne publient plus `analyze_repo` ni `implement_feature` : ils servent seulement leurs workflows sur leur queue. « Queue = capacité » reste vrai pour nos workers ; les machines sont une couche de routage au-dessus, dans `CodingRunWorkflow`, et `runQueue` reste le choix d'une queue de workers à l'intérieur du repli.

## 5. Inscription et connexion

1. Dans « Mes machines » (nouvelle page), l'utilisateur crée un **jeton d'inscription** : 15 min, à usage unique.
2. Sur sa machine : `agent connect --join <url>`, qui lit le jeton sur l'entrée standard (jamais en argument : il resterait dans l'historique du shell) *[rev. 13]*. Le serveur rend un **jeton machine** (long, révocable, propre à cette machine), gardé dans le dossier de configuration (0600). Seul son hash est en base (`machines` : id, utilisateur, nom, capacités, plafond, version, créée, vue pour la dernière fois, réplique qui tient la connexion, révoquée).
3. Ensuite, `agent connect` : connexion WSS à `/machines/connect`, certificat du serveur vérifié (aucune option pour s'en passer hors dev), puis `hello` (version du protocole, capacités, plafond, directives en cours).
4. « Mes machines » liste les machines, en ligne ou non, leurs capacités et leurs directives ; on peut en révoquer une (connexion coupée, jeton refusé, directives en cours annulées).

Ce que la machine n'a pas : `DATABASE_URL`, `TEMPORAL_HOST`, `INTERNAL_API_KEY`, la clé LLM du serveur, ni aucun jeton de tâche Temporal (§6).

**Versions.** Les machines tournent avec le binaire que leur propriétaire a installé : c'est le seul endroit du projet où une incompatibilité entre versions est réelle même sans prod. Le `hello` négocie une version de protocole ; trop vieille, la machine est refusée avec « mets à jour agent ».

## 6. Une directive, de bout en bout

1. `CodingRunWorkflow` a choisi une machine (`PickMachine`, §9). Il planifie `RunOnMachine` sur sa propre queue, avec la directive (petite : dépôt, ref, tâche, options ; jamais un gros contenu).
2. `RunOnMachine` (n'importe quel worker) écrit la directive en base (`machine_directives` : id, machine, utilisateur, session, tour, appel, nature, **jeton de tâche** de l'activity, état, échéance), puis la remet à la passerelle par l'API interne (`POST /internal/machines/directives`, `Authorization: Bearer $INTERNAL_API_KEY`, comme `/internal/notify`), et rend `activity.ErrResultPending` : **complétion asynchrone**, aucun emplacement de worker tenu pendant un run *[rev. 5]*.
3. La passerelle envoie la directive à la machine (sur la réplique qui tient sa connexion, §12). Le jeton de tâche **ne part jamais** vers la machine : elle ne connaît que l'ID de directive. Qui détient ce jeton et un client du namespace peut terminer l'activity avec n'importe quel résultat *[rev. 11]*.
4. **Heartbeats portés par la passerelle** *[rev. 6]* : toutes les 30 s pour chaque directive en cours, tant que la connexion de la machine répond au ping (`client.RecordActivityHeartbeat` avec le jeton). Les `progress` de la machine sont regroupés (le dernier seulement, 1 Kio au plus, ils finissent dans l'historique) et joints au heartbeat suivant ; ils alimentent aussi la note du tour (« Analyse sur la machine de Victor — 34 outils »), par la queue du tour comme les avis d'aujourd'hui.
5. **Arrêt** : la réponse à un heartbeat rend `CanceledError` quand le workflow a annulé l'activity (vérifié dans le SDK : `recordActivityHeartbeat`), ou `NotFound` quand le workflow n'existe plus (session supprimée) : dans les deux cas, `cancel` à la machine. `activity_paused` est ignoré. Comme les heartbeats partent toutes les 30 s, un « Arrêter » arrive en 30 s au plus, même quand la CLI se tait.
6. **Résultat** : la passerelle termine l'activity (`client.CompleteActivity`), avec le rapport et la progression. Un résultat pour une directive que la base ne tient plus pour ouverte est jeté.

**Options de `RunOnMachine`** *[rev. 2]* : `MaximumAttempts: 1` (un run n'est jamais rejoué : il se paie, et un second `implement_feature` produirait d'autres commits) ; `StartToCloseTimeout` = la durée d'un run (45 min pour une analyse) ; `HeartbeatTimeout` = **5 min**, au-delà d'un redémarrage du serveur ; `WaitForCancellation: true`, pour que l'annulation du workflow devienne `cancel_requested` côté activity.

**Garanties** *[rev. 7]* : une directive de run est exécutée **au plus une fois** ; perdue, c'est un échec clair au modèle (« la machine de Victor a décroché pendant le run ; rien n'a été poussé ; relancer repart de zéro et se paie à nouveau »), jamais une relance automatique. Seul ce qui est idempotent est « au moins une fois » : upload d'un fichier, `CompleteActivity`.

**Redémarrage de la passerelle (déploiement)** *[rev. 2]* : les connexions tombent, les directives restent en base avec leur jeton. La machine se reconnecte et son `hello` liste ses directives en cours ; la passerelle rattache celles que la base attribue à cette machine (jamais une autre : le `hello` ne ressuscite rien *[rev. 13]*) et reprend leurs heartbeats. Sans reconnexion dans les 5 min, l'activity expire : échec clair, comme ci-dessus.

**Directive orpheline** *[rev. 11]* : écrite en base mais dont l'activity a échoué avant `ErrResultPending`. La passerelle le découvre au premier heartbeat (`NotFound`) et l'annule ; un balayage périodique couvre celles jamais remises.

**Arrêt d'`agent connect`** : comme `RunStop` aujourd'hui, la machine tue ses runs et répond `machine_stopping` à chaque directive ; l'activity échoue avec ce type, lu comme une machine perdue.

## 7. Fichiers

Le contenu d'un fichier ne passe jamais par Temporal. Une machine publie par l'API : `PUT /machines/files`, avec l'ID de directive (qui donne session, tour, appel et utilisateur, vérifiés en base) et le nom ; même idempotence que `SaveFile` (`(session, tour, appel, nom)` : même sha256 = l'existant, autre contenu = refus, dit au modèle dans le résultat) *[rev. 15]*. Le serveur écrit (`store.FileStore`, S3 plus tard avec une URL présignée) et rend la référence, seule chose qui remonte au workflow, dans le résultat de la directive.

Un run qui produit un fichier (un rapport, un patch) le publie de sa machine. Un résultat jeté (directive close entre-temps) laisse ses fichiers déjà publiés rattachés au tour : ils apparaissent dans le fil, ce qui est acceptable.

## 8. Ce qui tourne sur la machine *[rev. 8]*

Phase 1 : **Linux**, sous l'utilisateur courant, sans root.
- **La CLI tourne sous l'utilisateur**, avec **son** login (abonnement dans `~/.claude`, ou `claude setup-token`) : c'est ce qui rend l'abonnement utilisable, et c'est comme s'il la lançait lui-même. Pas de `RUN_AS_UID`, pas de `KillStrays`, pas de verrou de racine, pas de copie de configuration (`claudecode.OwnConfig`).
- **Garde-fous**, réglés chez lui et jamais par le serveur : les modes de permission de la CLI (une analyse reste en lecture seule, mêmes options qu'aujourd'hui), une liste locale de dépôts autorisés (vide = tout refusé, comme `CLAUDE_CODE_REPOS`), un plafond de dépense local, un dossier de travail sous son cache.
- **Réutilisé tel quel** : `claudecode.Runner` (lecture du flux, minuterie de heartbeat qui devient les `progress`, détection de blocage), `subproc.KillGroup`, le filtrage d'environnement, la logique de clone, d'inspection et de push des activities de code (sans `Reclaim`, `Identity.Apply`, `SeedConfigDir`).
- **Un run = une seule directive** : clone avec ses identifiants git, run, inspection, push si demandé, nettoyage, rapport. Plus d'affinité entre étapes à garantir.

## 9. Qui exécute quoi

- **Règle** : un tour tourne pour l'humain qui l'a demandé, sur **sa** machine. `PickMachine` ne regarde que les machines de l'auteur du message du tour.
- **Choix** : parmi ses machines en ligne qui ont la capacité, la moins occupée, réservée en base en une seule requête (`UPDATE machines SET running = running + 1 WHERE … AND running < plafond … RETURNING`), pour que deux runs simultanés ne dépassent pas le plafond d'une machine *[rev. 14]*.
- **Aucune machine** : repli sur `CLAUDE_CODE_QUEUE` si l'installation en a un et le permet, sinon « ta machine n'est pas connectée ». Qui paie le repli (la clé de l'installation) est une politique de l'installation, à afficher dans « Mes machines ».
- **Plus tard** : un membre publie un de ses agents au groupe ; il tourne alors sur sa machine même quand d'autres l'appellent, en mode API seulement (consentement et plafond).

## 10. Plus tard : le modèle sur la machine, et la CLI comme moteur

Ces deux étapes sont esquissées ici ; elles auront leur propre conception.

**`CallLLM` sur la machine (phase 3).** La requête entière (prompt système, conversation convertie, définitions d'outils), construite par le serveur comme aujourd'hui, peut atteindre 2 Mo (`LLM_MAX_CONTEXT_BYTES`) : la machine la lit par `GET /machines/directives/<id>/request`, jamais par Temporal. **Modèle de menace** *[rev. 4]* : la machine qui fait tourner le modèle **a l'autorité de l'agent**. Sa réponse est une suite de tool calls que le workflow exécute avec toute l'allowlist (`send_email`, `exec` sur nos workers, `schedule_task`, sous-agents) : ce n'est pas « une saisie d'utilisateur à valider ». Dans l'autre sens, le prompt de l'agent, ses skills, les définitions d'outils et les messages des autres membres partent sur sa machine (sa mémoire à lui seulement ; les entrées et résultats `PrivateInput` des autres déjà masqués par `conversation.Convert`). D'où :
- une politique **par agent**, « peut tourner sur une machine », **non** par défaut, choisie par l'admin en connaissance de cause ;
- un sous-agent appelé depuis un tour exécuté sur une machine ne retombe jamais sur la clé du serveur (même règle, ou refus) ;
- l'origine tracée : `machine_id` sur les messages du tour.

**`CLIAgentWorkflow` et le pont MCP (phase 4)** *[rev. 3]*. La CLI de l'utilisateur fait tout le tour ; les outils de la plateforme lui sont offerts par un MCP stdio local (`agent mcp-bridge`), qui passe par `agent connect` et la passerelle jusqu'au workflow du run, par une **Workflow Update** `call_tool`. Ce n'est **pas** possible dans `AgentWorkflow` : sa boucle numérote elle-même les étapes et écrit chaque appel avec son résultat. À trancher dans la conception de `CLIAgentWorkflow`, avant d'écrire une ligne :
- la forme persistée d'un appel venu de la CLI (il n'a pas de `tool_use` d'un message assistant de la plateforme : bloc d'outil, avec quelle définition au tour suivant, ou texte ?) ;
- un validateur d'Update (allowlist, run encore ouvert), `workflow.Await(AllHandlersFinished)` avant la fin, un « Arrêter » pendant un Update en vol (erreur rendue à la CLI) ;
- les limites du serveur : `history.maxInFlightUpdates` (10 par run par défaut, qu'une CLI qui lance plusieurs outils en parallèle atteint), `history.maxTotalUpdates` (2000 par run), la taille d'historique sans remise à neuf possible au milieu d'un tour ;
- vérifier l'Update sur notre serveur 1.25 (image `auto-setup`) par un test `RealServer`.
Le pont tourne sous le même utilisateur que la CLI : le jeton de run de la socket n'est **pas** une frontière de sécurité, seulement de quoi refuser un appel hors run *[rev. 8]*.

## 11. Sécurité

- **Ce que la machine renvoie n'est pas de confiance** : tailles, références de fichiers (session et tour de la directive, vérifiés en base), rapports (texte pour le modèle, traité comme tel). Pour une machine qui fait tourner le modèle, voir §10 : elle a l'autorité de l'agent.
- **Jetons** : jeton machine (long, révocable, hash en base) pour la connexion et l'API ; une requête de l'API n'est acceptée que pour une directive ouverte de cette machine. Jamais de jeton de tâche Temporal hors du réseau privé.
- **Ce qu'une machine voit** : ses directives, et pour chacune ce qu'elle exige. Ni liste, ni lecture d'autres sessions.
- **Abus** : plafonds de taille (messages WSS, `progress`, uploads), débit par machine, directives simultanées par machine, révocation immédiate.
- **Le serveur vu de la machine** : son propriétaire fait confiance à l'installation qu'il rejoint, puisqu'elle lui envoie des tâches pour sa CLI ; les garde-fous locaux (§8) restent les siens.

## 12. Ce qui change dans le code

Ce qu'un worker touche aujourd'hui et que la machine n'aura pas *[rev. 12]* :
- **PostgreSQL** : publication des outils (`publishTools`), catalogue et queues rechargés toutes les 30 s (`pollCatalog`, `pollActivityQueues`), `skills_version`, publication de fichiers (`tool.Publisher`), outils de mémoire, de planification et de documents.
- **Temporal** : tous les workflows et activities sur chaque queue (`workerWorkflows`), `query_workflow`, client des tâches planifiées, relais, sessions de run (`openRun`, `ProbeRunWorker`, `CLAUDE_CODE_QUEUE_WAIT`, remplacés côté machine par le routage), avis par `CallContext.NotifyQueue` (remplacés par les `progress`).
- **Processus** : `subproc.Runs`, `claimRunsRoot` et son verrou, `CLAUDE_CONFIG_DIR`/`SeedConfigDir`, `cliRuns`, `RunStop` (équivalent : `machine_stopping`).
- **Configuration** : `CLAUDE_CODE_AUTH`/`ResolveAuth` reste utile sur la machine (abonnement ou clé de l'utilisateur), lu chez elle.

Nouveau :
- côté serveur : la passerelle (`/machines/connect`, `/machines/files`, `/internal/machines/directives`), les tables `machines` et `machine_directives`, la page « Mes machines » ;
- côté workers : `CodingRunWorkflow`, les activities `PickMachine` et `RunOnMachine`, la queue de repli `CLAUDE_CODE_QUEUE` ;
- côté machine : le mode `agent connect`, sans base ni Temporal.

**Plusieurs répliques du serveur** *[rev. 5]* : la machine est connectée à une réplique (`machines.connected_to`) ; `/internal/machines/directives` arrive sur n'importe laquelle, qui transmet à la bonne (adresse interne de la réplique, ou LISTEN/NOTIFY). Heartbeats et `CompleteActivity` partent de la réplique qui tient la connexion, avec le jeton lu en base. Phases 0 à 2 : une seule réplique.

## 13. Le client de bureau

Une application (Wails : Go, webview du système) qui embarque `agent connect` et affiche le front de l'installation dans une fenêtre. Une seule chose à lancer.
- **Inscription sans copier-coller** : on se connecte dans la fenêtre, le serveur inscrit la machine dans la foulée (flux du type `gh auth login`) ; le jeton machine va dans le trousseau du système.
- **Barre système** : en ligne ou non, runs en cours, CLI détectées (Claude Code, Codex) et ce qui manque ; démarrage automatique.
- **Mise à jour automatique** : obligatoire, puisque le serveur refuse les machines trop vieilles (§5). Wails ne la fournit pas.
- **Pas un verrou** : le web reste utilisable sans l'application (téléphone compris). Ce qui a besoin de la machine dit « ta machine n'est pas connectée » ; lire, écrire, forker marchent partout. Le serveur ne peut de toute façon pas distinguer l'application d'un navigateur.
- **Coût** : builds par système avec cgo (WebKitGTK, WebView2, WKWebView), signature (notarisation Apple, SmartScreen), et l'exécution hors Linux (§8 est Linux seulement en phase 1).

C'est une couche au-dessus d'`agent connect` : aucun protocole nouveau, sauf le flux de connexion qui inscrit la machine.

## 14. Phases *[rev. 9]*

| Phase | Contenu | Résultat |
|---|---|---|
| **0** | Passerelle (une réplique), inscription par jeton, tables, `RunOnMachine` avec complétion asynchrone, une directive triviale (`echo`), un test `RealServer` : complétion, heartbeat par client, `cancel`, redémarrage de la passerelle et rattachement, machine perdue | Le mécanisme est prouvé contre le vrai serveur |
| **1** | `CodingRunWorkflow`, `PickMachine`, repli `CLAUDE_CODE_QUEUE`, `analyze_repo` sur la machine (Linux), « Mes machines » | Alice lance une analyse avec son abonnement, depuis chez elle, sans VPN |
| **1 bis** | Client de bureau (Linux d'abord) | Une seule chose à lancer |
| **2** | Upload de fichiers depuis la machine ; `implement_feature` sur la machine (push avec ses identifiants) | Les runs rendent des fichiers ; le code part de sa machine |
| **3** | `CallLLM` sur la machine, avec sa conception et la politique par agent (§10) | Un utilisateur sans clé sur le serveur a un agent |
| **4** | Conception puis code de `CLIAgentWorkflow`, `agent mcp-bridge`, Update `call_tool` | La CLI fait tout le tour avec les outils de la plateforme |
| **5** | Plusieurs répliques, publication d'un agent au groupe, macOS et Windows | Montée en charge, partage consenti |

## 15. Questions ouvertes

1. Transport : WebSocket (le plus simple derrière un reverse proxy), flux gRPC ou HTTP/2 en long-poll ? À confirmer avec le proxy visé.
2. Repli sur `CLAUDE_CODE_QUEUE` quand la machine de l'auteur est hors ligne : permis par défaut ou non ?
3. Une machine pour plusieurs utilisateurs (serveur d'équipe) : exclu en phase 1.
4. Liste locale des dépôts : vide par défaut (tout refusé, comme aujourd'hui), ou proposée à l'inscription ?
