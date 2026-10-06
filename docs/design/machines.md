# Conception : des machines hors du réseau privé

Statut : **version 2.2** ; **phase 0 faite** le 6 octobre 2026 (§16 : ce qui est fait, et les écarts à ce document), phases 1 et suivantes à faire. Version 1 le 5 octobre 2026, révisée le même jour après deux relectures contre le code et le SDK Temporal (v1.33) : les points de la première sont marqués *[rev. 1…15]*, ceux de la seconde *[rev2 A…J]*. La 2.2 (6 octobre) ajoute ce qu'une discussion a précisé : inscription par code, jetons, exécutable natif, plusieurs machines, transport.

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
- sinon : l'`AnalyzeRepoWorkflow` / `ImplementFeatureWorkflow` d'aujourd'hui, **inchangé**, en enfant sur la queue de repli **de cet outil** (`CLAUDE_CODE_ANALYZE_QUEUE`, `CLAUDE_CODE_IMPLEMENT_QUEUE` : une par outil, pour garder la séparation actuelle des identifiants, clé git en lecture seule d'un côté, clé de push de l'autre *[rev2 A]*), avec sa session Temporal, sa sonde et son attente ; ou une erreur claire au modèle si l'installation n'a pas de repli.
- Avant l'enfant, `CodingRunWorkflow` lance lui-même `ProbeRunWorker` sur la queue de repli (`ScheduleToStartTimeout` 1 min, comme `openRun`) : si personne ne la sert, l'enfant ne démarrerait jamais et la sonde, qui vit dans l'enfant, ne tournerait pas. « Pas de worker » devient une erreur claire, comme « pas de machine » *[rev2 B]*.
- L'enfant garde un ID préfixé par la session (dérivé de celui du parent, `<session>:…`) pour `query_workflow` et `SessionOf` *[rev2 I]*. Le `CallContext` passe tel quel à l'enfant : avis d'attente, session et `runQueue` ne changent pas.

Les conteneurs Claude Code ne publient plus `analyze_repo` ni `implement_feature` : ils servent seulement leurs workflows sur leur queue. Les descriptions de ces outils, composées aujourd'hui dans le conteneur (`auth.CostNote()`, attente d'un worker) et publiées seulement là où la CLI est installée, sont récrites pour le worker principal (« sur ta machine, avec ton abonnement ; sinon le repli de l'installation ») et publiées sans condition sur la CLI *[rev2 G]*. « Queue = capacité » reste vrai pour nos workers ; les machines sont une couche de routage au-dessus, dans `CodingRunWorkflow`, et `runQueue` reste le choix d'une queue de workers à l'intérieur du repli.

## 5. Inscription et connexion

**Inscription par code** (flux « par appareil », RFC 8628, celui de `gh auth login` et des télés connectées) : aucun navigateur sur la machine.
1. `agent connect --join <url> [--name "serveur maison"]` (nom par défaut : le nom d'hôte) se déclare au serveur avec son nom, son système et ses capacités. Le serveur crée une demande en attente (10 min) et rend deux codes : un **code utilisateur** court (`KX4-92M`), fait pour être tapé, et un **secret de demande** long, que la machine garde pour elle.
2. La machine affiche : « Ouvre `<url>/machines/activer` et saisis le code KX4-92M », puis interroge le serveur toutes les 5 s avec le secret de demande.
3. L'utilisateur, connecté au front (portable, téléphone), saisit le code dans « Mes machines › Ajouter une machine ». La page montre la demande désignée : nom, système, capacités, adresse IP, heure, et l'avertissement « n'approuve qu'un code que tu viens de voir sur ta propre machine ». Il approuve.
4. À l'interrogation suivante, le serveur rend le **jeton machine**, une seule fois ; le secret de demande est consommé.

Le code va toujours **de la machine vers l'utilisateur** : c'est sa saisie dans **sa** session qui rattache la demande à son compte. « Mes machines » n'affiche jamais de demandes en attente : au moment de la demande, personne ne sait à qui elle appartient. Le code utilisateur ne permet qu'approuver ; seul le détenteur du secret de demande récupère le jeton. Deux secrets plutôt qu'un : ce qui circule pendant l'attente ne vaut plus rien après, et l'identifiant durable n'est créé qu'après l'approbation d'un humain. Risque connu du flux, l'hameçonnage (« approuve ce code » envoyé par quelqu'un d'autre) : page d'approbation explicite, 10 min, essais de saisie limités, notification de chaque machine inscrite. Pour un script, un **jeton d'inscription** créé dans « Mes machines » (15 min, usage unique) reste possible, lu sur l'entrée standard, jamais en argument *[rev. 13]*.

**Le jeton machine.** Long, révocable, propre à cette machine ; seul son hash est en base (`machines` : id, utilisateur, nom, capacités, plafond, priorité, en pause, version, créée, vue pour la dernière fois, réplique qui tient la connexion, révoquée). Sur la machine, dans le trousseau du système quand il existe, sinon un fichier 0600 du dossier de configuration (comme une clé SSH, ou le login de la CLI Claude) : `agent connect` doit se reconnecter seul. Ce qui limite un vol :
- **sa portée** : les directives de son propriétaire, rien d'autre ; il n'ouvre **jamais** de session web (voir §13) ;
- **une rotation en deux temps** à chaque connexion : le serveur remet J2, J1 reste valide jusqu'à ce que la machine confirme avoir écrit J2, puis J1 est invalidé (une machine qui plante entre les deux se reconnecte avec J1) ;
- **la détection de réutilisation** : un jeton remplacé qui se représente révèle une copie ; le serveur révoque la machine et prévient son propriétaire, qui la ré-inscrit ;
- **une seule connexion par jeton** : deux connexions simultanées coupent les deux, avec la même alerte ;
- **une notification** à chaque connexion depuis une nouvelle adresse, pour réduire la fenêtre où un voleur se connecte pendant que la vraie machine est éteinte.
Plus tard, avant la phase 3 (où des conversations passent par la machine) : une **paire de clés** plutôt qu'un jeton, la clé privée générée à l'inscription et jamais transmise, dans une puce sécurisée (Secure Enclave, TPM) quand le trousseau le permet ; la connexion signe un défi du serveur.

**Connexion.** `agent connect` ouvre une WebSocket TLS sur `/machines/connect` (certificat du serveur vérifié, aucune option pour s'en passer hors dev), puis envoie `hello` (version du protocole, capacités, plafond, directives en cours et finies non acquittées). « Mes machines » liste les machines, en ligne ou non, leurs capacités, leurs directives ; on peut en mettre une en pause, changer sa priorité, la révoquer (connexion coupée, jeton refusé, directives ouvertes terminées en erreur).

**Transport : WebSocket**, pas un flux gRPC. gRPC apporterait un schéma typé et le contrôle de flux, et ses dépendances sont déjà là (SDK Temporal). Mais il exige HTTP/2 de bout en bout, que des box, des proxys d'entreprise et des CDN cassent, alors que les machines se connectent depuis des réseaux qu'on ne maîtrise pas ; une WebSocket est du HTTP/1.1 sur le port 443, comme le front, servie par le même serveur, le même routeur, le même TLS. Les messages sont peu nombreux et petits (les gros contenus passent par l'API HTTP), le contrôle de flux n'apporterait guère. Messages JSON versionnés (`protocol` du `hello`) ; du protobuf dans la WebSocket reste possible si un schéma typé devient utile.

Ce que la machine n'a pas : `DATABASE_URL`, `TEMPORAL_HOST`, `INTERNAL_API_KEY`, la clé LLM du serveur, ni aucun jeton de tâche Temporal (§6).

**Versions.** Les machines tournent avec le binaire que leur propriétaire a installé : c'est le seul endroit du projet où une incompatibilité entre versions est réelle même sans prod. Le `hello` négocie une version de protocole ; trop vieille, la machine est refusée avec « mets à jour agent ».

## 6. Une directive, de bout en bout

1. `CodingRunWorkflow` a choisi une machine (`PickMachine`, §9). Il planifie `RunOnMachine` sur sa propre queue, avec la directive (petite : dépôt, ref, tâche, options ; jamais un gros contenu).
2. `RunOnMachine` (n'importe quel worker) complète la directive que `PickMachine` a créée (§9) avec le **jeton de tâche** de l'activity, puis la remet à la passerelle par l'API interne (`POST /internal/machines/directives`, `Authorization: Bearer $INTERNAL_API_KEY`, comme `/internal/notify`), et rend `activity.ErrResultPending` : **complétion asynchrone**, aucun emplacement de worker tenu pendant un run *[rev. 5]*.
3. La passerelle envoie la directive à la machine (sur la réplique qui tient sa connexion, §12). Le jeton de tâche **ne part jamais** vers la machine : elle ne connaît que l'ID de directive. Qui détient ce jeton et un client du namespace peut terminer l'activity avec n'importe quel résultat *[rev. 11]*.
4. **Heartbeats portés par la passerelle** *[rev. 6]* : toutes les 30 s pour chaque directive en cours, tant que la connexion de la machine répond au ping (`client.RecordActivityHeartbeat` avec le jeton : un appel client pur, que le serveur fait avec son client Temporal ; hors d'un contexte d'activity la requête part sans namespace, le serveur Temporal le lit dans le jeton, à couvrir par le test de la phase 0). Les `progress` de la machine sont regroupés (le dernier seulement, 1 Kio au plus, ils finissent dans l'historique) et joints au heartbeat suivant. Ils alimentent aussi la note du tour (« Analyse sur la machine de Victor — 34 outils ») : la passerelle est le serveur, elle appelle directement `session.Service.Observe`, donc **web seulement** ; une session Telegram n'a pas cet avis *[rev2 E]*.
5. **Arrêt** : la réponse à un heartbeat rend `CanceledError` quand le workflow a annulé l'activity (vérifié dans le SDK : `recordActivityHeartbeat`), ou `NotFound` quand le workflow n'existe plus (session supprimée) : dans les deux cas, `cancel` à la machine. `activity_paused` est ignoré. Comme les heartbeats partent toutes les 30 s, un « Arrêter » arrive en 30 s au plus, même quand la CLI se tait.
6. **Résultat** : la passerelle termine l'activity (`client.CompleteActivity`), avec le rapport et la progression. Un résultat pour une directive que la base ne tient plus pour ouverte est jeté.

**Options de `RunOnMachine`** *[rev. 2]* : `MaximumAttempts: 1` (un run n'est jamais rejoué : il se paie, et un second `implement_feature` produirait d'autres commits) ; `StartToCloseTimeout` = la durée d'un run (45 min pour une analyse) ; `HeartbeatTimeout` = **5 min**, au-delà d'un redémarrage du serveur ; `WaitForCancellation: true`, pour que le workflow attende la réponse finale de la machine (son rapport partiel, l'assurance que la CLI est arrêtée) au lieu de rendre `CanceledError` aussitôt. L'annulation elle-même devient `cancel_requested` dans tous les cas.

**Un choix assumé** *[rev2 F]* : 5 min de silence = machine perdue. Un portable qu'on ferme 10 min perd son run, même si la CLI aurait repris au réveil (la passerelle lui envoie alors `cancel`). C'est le prix d'une détection rapide d'une machine vraiment morte ; `StartToCloseTimeout` borne de toute façon le run.

**Garanties** *[rev. 7]* : une directive de run est exécutée **au plus une fois** ; perdue, c'est un échec clair au modèle (« la machine de Victor a décroché pendant le run ; rien n'a été poussé ; relancer repart de zéro et se paie à nouveau »), jamais une relance automatique. Seul ce qui est idempotent est « au moins une fois » : upload d'un fichier, `CompleteActivity`.

**Redémarrage de la passerelle (déploiement)** *[rev. 2]* : les connexions tombent, les directives restent en base avec leur jeton. La machine se reconnecte et son `hello` liste ses directives **en cours** et ses directives **finies non acquittées** ; la passerelle rattache celles que la base attribue à cette machine et tient pour ouvertes (jamais une autre : le `hello` ne ressuscite rien *[rev. 13]*), reprend leurs heartbeats, et accepte les résultats en attente. Une directive listée que la base a close reçoit `cancel`. Sans reconnexion dans les 5 min, l'activity expire : échec clair, comme ci-dessus.

**Un résultat n'est jamais perdu par une déconnexion** *[rev2 C]* : la machine garde chaque résultat (sur disque) jusqu'à l'`ack` de la passerelle, qui ne l'envoie qu'après `CompleteActivity`. Un run fini pendant les trois minutes d'un déploiement rend donc son rapport à la reconnexion, au lieu de 45 min payées pour rien.

**Directive orpheline** *[rev. 11]* : créée en base mais dont l'activity a échoué avant `ErrResultPending`, ou réservée par `PickMachine` dans un workflow annulé avant `RunOnMachine`. La passerelle le découvre au premier heartbeat (`NotFound`) et la ferme ; un balayage ferme celles sans jeton dont l'échéance de remise est passée, et toutes celles dont l'échéance est passée.

**Révocation** *[rev2 H]* : la passerelle ferme la connexion, envoie `cancel`, et termine elle-même l'activity de chaque directive ouverte (`CompleteActivity` avec l'erreur « machine révoquée »), plutôt que de laisser le tour attendre 5 min.

**Arrêt d'`agent connect`** : comme `RunStop` aujourd'hui, la machine tue ses runs et répond `machine_stopping` à chaque directive ; l'activity échoue avec ce type, lu comme une machine perdue.

## 7. Fichiers

Le contenu d'un fichier ne passe jamais par Temporal. Une machine publie par l'API : `PUT /machines/files`, avec l'ID de directive (qui donne session, tour, appel et utilisateur, vérifiés en base) et le nom ; même idempotence que `SaveFile` (`(session, tour, appel, nom)` : même sha256 = l'existant, autre contenu = refus, dit au modèle dans le résultat) *[rev. 15]*. Le serveur écrit (`store.FileStore`, S3 plus tard avec une URL présignée) et rend la référence, seule chose qui remonte au workflow, dans le résultat de la directive.

Un run qui produit un fichier (un rapport, un patch) le publie de sa machine. Un résultat jeté (directive close entre-temps) laisse ses fichiers déjà publiés rattachés au tour : ils apparaissent dans le fil, ce qui est acceptable.

## 8. Ce qui tourne sur la machine *[rev. 8]*

`agent connect` est un **exécutable natif** sur l'hôte, sans conteneur : le binaire `agent` compilé pour le système, qui trouve `claude` et `git` dans le PATH et tourne sous le compte de l'utilisateur, sans root. Sans `claude`, il n'annonce pas la capacité `claude-code`.

Phase 1 : **Linux**. Un utilisateur Windows passe par WSL2 (c'est la version Linux, sur son PC) ; un utilisateur Mac, en attendant, par un serveur maison ou un VPS. macOS ensuite : `Setpgid` et `kill(-pgid)`, sur lesquels repose `subproc`, y existent ; le coût est la distribution (signature et notarisation Apple, ou Homebrew). Windows natif plus tard, si la demande est là : `subproc` ne compile pas pour Windows (il faudrait des Job Objects et `CTRL_BREAK`), et Claude Code y dépend de Git Bash.

- **La CLI tourne sous l'utilisateur**, avec **son** login : c'est ce qui rend l'abonnement utilisable, et c'est comme s'il la lançait lui-même. Pas de `RUN_AS_UID`, pas de `KillStrays`, pas de verrou de racine.
- **Le login existant suffit** : celui de `claude` en interactif (`~/.claude/.credentials.json`, ou le trousseau sous macOS), que la CLI renouvelle elle-même. Pas besoin de `claude setup-token` ni de `CLAUDE_CODE_OAUTH_TOKEN`, sauf sur une machine sans navigateur où c'est le plus simple (`/login` en SSH marche aussi : la CLI donne une URL à ouvrir ailleurs). Donc **aucune copie de configuration** (`claudecode.OwnConfig`) : la CLI écrit son jeton renouvelé dans le `~/.claude` de l'utilisateur, partagé avec ses terminaux ; et le filtrage d'environnement garde ce dont elle a besoin pour le trouver (`HOME`, `XDG_*`). Le login reste sur la machine, jamais envoyé au serveur.
- **Qui paie, sans surprise** : la logique actuelle s'applique chez lui. `CLAUDE_CODE_AUTH=subscription` retire `ANTHROPIC_API_KEY` de l'environnement de la CLI (`Auth.Filter`) : sinon une clé présente dans son shell ferait facturer ses runs à l'API sans qu'il le sache ; les deux présents sans mode = `agent connect` refuse de démarrer ; il affiche au démarrage qui paie.
- **Login vérifié avant d'annoncer la capacité** : un login dans `~/.claude` ou le trousseau, `CLAUDE_CODE_OAUTH_TOKEN`, ou `ANTHROPIC_API_KEY` avec son mode, constatés sans appel payant. Sans aucun, pas de capacité `claude-code`, et un message clair (« lance `claude` puis /login, ou `claude setup-token` »), repris dans « Mes machines » (« Claude Code : pas connecté »). Un run qui échoue sur une erreur d'authentification retire la capacité, le signale, et elle revient avec le login.
- **Garde-fous**, réglés chez lui et jamais par le serveur : les modes de permission de la CLI (une analyse reste en lecture seule, mêmes options qu'aujourd'hui), une liste locale de dépôts autorisés (vide = tout refusé, comme `CLAUDE_CODE_REPOS`), un plafond de dépense local, un dossier de travail sous son cache.
- **Réutilisé tel quel** : `claudecode.Runner` (lecture du flux, minuterie de heartbeat qui devient les `progress`, détection de blocage), `subproc.KillGroup`, le filtrage d'environnement, la logique de clone, d'inspection et de push des activities de code (sans `Reclaim`, `Identity.Apply`, `SeedConfigDir`).
- **Un run = une seule directive** : clone avec ses identifiants git, run, inspection, push si demandé, nettoyage, rapport. Plus d'affinité entre étapes à garantir.
- **Ce que ça implique** : une analyse reste en lecture seule ; mais `implement_feature` laisse la CLI exécuter des commandes sur son poste avec ses droits, et l'utilisateur isolé de nos workers n'existe pas ici. Ce qui le protège : sa liste de dépôts, son plafond, les permissions de la CLI, et la règle qui ne lui envoie que les runs de **ses** tours. Reste le cas d'un serveur compromis qui ferait travailler sa machine : le client de bureau proposera « me demander avant chaque run ».
- **Isolation en option, plus tard** : bubblewrap sous Linux (sans root sur la plupart des distributions : seul le clone en écriture, le reste du dossier personnel masqué sauf `~/.claude`), un mode `--docker` pour qui a Docker (en y faisant entrer le login de la CLI), `sandbox-exec` sous macOS (déprécié, fragile).

## 9. Qui exécute quoi

- **Règle** : un tour tourne pour l'humain qui l'a demandé, sur **sa** machine. `PickMachine` ne regarde que les machines de l'auteur du message du tour.
- **Plusieurs machines par utilisateur** : chacune fait tourner son `agent connect`, inscrite sur le même compte (portable et serveur maison, par exemple), avec ses propres réglages locaux (login Claude, identifiants git, dépôts autorisés, plafond de dépense). Elles partagent l'abonnement de l'utilisateur, donc sa limite d'utilisation : deux runs en parallèle consomment deux fois plus vite.
- **Choix** : parmi ses machines en ligne, pas en pause, qui ont la capacité : la plus haute **priorité** (réglée dans « Mes machines » : « mon serveur d'abord, le portable s'il est éteint »), puis la moins occupée, sous le plafond qu'elle a annoncé *[rev. 14]*. Une machine **en pause** reste connectée sans rien recevoir.
- **Pas de compteur** *[rev2 D]* : la charge d'une machine est le nombre de ses lignes ouvertes dans `machine_directives`. `PickMachine` **crée la directive** dans la transaction qui choisit (verrou sur la ligne de la machine, `SELECT … FOR UPDATE`, compte des directives ouvertes, insertion), idempotente sur `(run du workflow, appel)` : une `PickMachine` rejouée retrouve sa ligne au lieu d'en réserver une seconde. Réservation et directive sont la même ligne ; toute fin (résultat, échec, `cancel` acquitté, `NotFound`, révocation, orpheline, échéance) est un seul `UPDATE` de son état. Rien à décrémenter, rien qui dérive.
- **Aucune machine** : repli sur la queue de l'outil (`CLAUDE_CODE_ANALYZE_QUEUE`, `CLAUDE_CODE_IMPLEMENT_QUEUE`) si l'installation en a une et le permet, sinon « ta machine n'est pas connectée ». Qui paie le repli (la clé de l'installation) est une politique de l'installation, à afficher dans « Mes machines ».
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
- **Temporal** : tous les workflows et activities sur chaque queue (`workerWorkflows`), `query_workflow`, client des tâches planifiées, relais, sessions de run (`openRun`, `ProbeRunWorker`, `CLAUDE_CODE_QUEUE_WAIT`, remplacés côté machine par le routage), avis par `CallContext.NotifyQueue` (remplacés par les `progress`, web seulement).
- **Processus** : `subproc.Runs`, `claimRunsRoot` et son verrou, `CLAUDE_CONFIG_DIR`/`SeedConfigDir`, `cliRuns`, `RunStop` (équivalent : `machine_stopping`).
- **Configuration** : `CLAUDE_CODE_AUTH`/`ResolveAuth` reste utile sur la machine (abonnement ou clé de l'utilisateur), lu chez elle.

Nouveau :
- côté serveur : la passerelle (`/machines/connect`, `/machines/files`, `/internal/machines/directives`), les tables `machines` et `machine_directives`, la page « Mes machines » ;
- côté workers : `CodingRunWorkflow`, les activities `PickMachine` et `RunOnMachine`, les queues de repli par outil (`CLAUDE_CODE_ANALYZE_QUEUE`, `CLAUDE_CODE_IMPLEMENT_QUEUE`), les descriptions récrites d'`analyze_repo` et `implement_feature` ;
- côté machine : le mode `agent connect`, sans base ni Temporal.

**Plusieurs répliques du serveur** *[rev. 5]* : la machine est connectée à une réplique (`machines.connected_to`) ; `/internal/machines/directives` arrive sur n'importe laquelle, qui transmet à la bonne (adresse interne de la réplique, ou LISTEN/NOTIFY). Heartbeats et `CompleteActivity` partent de la réplique qui tient la connexion, avec le jeton lu en base. Phases 0 à 2 : une seule réplique.

## 13. Le client de bureau

Une application (Wails : Go, webview du système) qui embarque `agent connect` et affiche le front de l'installation dans une fenêtre. Une seule chose à lancer.
- **Inscription sans code** : au premier lancement, la fenêtre affiche le login du front ; une fois connecté (le cookie est gardé par le webview, comme dans un navigateur), c'est cette session qui approuve directement l'inscription de sa machine. Le flux par code (§5) ne sert qu'à `agent connect` en ligne de commande. Le jeton machine va dans le trousseau du système.
- **Le login donne le jeton machine, jamais l'inverse** : un point d'entrée qui échangerait le jeton machine contre une session web ferait d'un identifiant qui ne reçoit que des directives la clé de tout le compte (sessions, mémoire, fichiers, invitations). La session web garde sa propre durée de vie.
- **Barre système** : en ligne ou non, runs en cours, CLI détectées (Claude Code, Codex) et ce qui manque ; démarrage automatique.
- **Mise à jour automatique** : obligatoire, puisque le serveur refuse les machines trop vieilles (§5). Wails ne la fournit pas.
- **Pas un verrou** : le web reste utilisable sans l'application (téléphone compris). Ce qui a besoin de la machine dit « ta machine n'est pas connectée » ; lire, écrire, forker marchent partout. Le serveur ne peut de toute façon pas distinguer l'application d'un navigateur.
- **Coût** : builds par système avec cgo (WebKitGTK, WebView2, WKWebView), signature (notarisation Apple, SmartScreen), et l'exécution hors Linux (§8 est Linux seulement en phase 1).

C'est une couche au-dessus d'`agent connect` : aucun protocole nouveau, sauf le flux de connexion qui inscrit la machine.

## 14. Phases *[rev. 9]*

| Phase | Contenu | Résultat |
|---|---|---|
| **0** (fait, §16) | Passerelle (une réplique) en WebSocket, inscription par code (et par jeton pour un script), rotation du jeton machine, tables, `PickMachine` et `RunOnMachine` avec complétion asynchrone, une directive triviale (`echo`), un test `RealServer` sur la base jetable du `CLAUDE.md` *[rev2 J]* : heartbeat **et** complétion par client depuis le serveur, `cancel`, redémarrage de la passerelle avec rattachement et résultat rendu après reconnexion (`ack`), machine perdue, révocation, réservations concurrentes au plafond | Le mécanisme est prouvé contre le vrai serveur |
| **1** | `CodingRunWorkflow`, repli par outil avec sonde, `analyze_repo` sur la machine (Linux), « Mes machines » | Alice lance une analyse avec son abonnement, depuis chez elle, sans VPN |
| **1 bis** | Client de bureau (Linux d'abord) | Une seule chose à lancer |
| **2** | Upload de fichiers depuis la machine ; `implement_feature` sur la machine (push avec ses identifiants) | Les runs rendent des fichiers ; le code part de sa machine |
| **3** | `CallLLM` sur la machine, avec sa conception et la politique par agent (§10) | Un utilisateur sans clé sur le serveur a un agent |
| **4** | Conception puis code de `CLIAgentWorkflow`, `agent mcp-bridge`, Update `call_tool` | La CLI fait tout le tour avec les outils de la plateforme |
| **5** | Plusieurs répliques, publication d'un agent au groupe, macOS et Windows | Montée en charge, partage consenti |

## 15. Questions ouvertes

1. Repli sur les queues de l'installation quand la machine de l'auteur est hors ligne : permis par défaut ou non ?
2. Une machine pour plusieurs utilisateurs (serveur d'équipe) : exclu en phase 1.
3. Liste locale des dépôts : vide par défaut (tout refusé, comme aujourd'hui), ou proposée à l'inscription ?

## 16. Phase 0 : ce qui est fait, et les écarts

Fait (6 octobre 2026), tel que décrit plus haut : tables (`machines`, `machine_directives`, `machine_enrollments`, plus `machine_retired_tokens`), `PickMachine` dans une transaction qui tient les machines de l'utilisateur, `RunOnMachine` en complétion asynchrone (1 essai, `HeartbeatTimeout` 5 min, `WaitForCancellation`), remise par `POST /internal/machines/directives` (en mémoire en `agent dev`), passerelle WebSocket (`gateway/`, `github.com/coder/websocket`) qui heartbeate et complète en **client** Temporal, inscription par code et par jeton d'inscription, rotation du jeton, révocation, balayage, `agent connect` (`machine/connect`, sans base ni Temporal, vérifié par un test sur ses dépendances), une page « Mes machines » minimale, la directive `echo`, `MachineEchoWorkflow` et la commande `agent machine-echo` pour essayer à la main. Le test `RealServer` de `gateway/` couvre la liste de la phase 0, et le point laissé ouvert au §6 : `RecordActivityHeartbeat` et `CompleteActivity` hors d'un contexte d'activity marchent sur notre serveur 1.25, le namespace est lu dans le jeton.

Précisions et écarts :

1. **« En ligne »** : la base ne sait pas seule qui tient une connexion. `machines.connected_to` (l'instance de passerelle) plus `seen_at`, rafraîchi à chaque ping réussi (30 s) ; `PickMachine` ne prend qu'une machine vue depuis 2 min (une passerelle morte sans un mot ne laisse pas de machine « en ligne »). Une passerelle qui démarre remet toutes les connexions à zéro : c'est le choix d'une seule réplique (phase 5 : chaque réplique les siennes).
2. **Jetons remplacés** : gardés dans `machine_retired_tokens` (tous, pas seulement le précédent), pour reconnaître la réutilisation de n'importe lequel ; la révocation y met aussi les jetons courants. Cas de la rotation précisés : la machine présente le jeton en attente (elle n'a pas écrit le nouveau) → le nouveau, que personne ne devrait avoir, est retiré ; elle présente le courant alors qu'un précédent attend encore (sa confirmation s'est perdue) → le précédent est retiré.
3. **Une seule connexion** : une connexion plus ancienne qui ne répond plus au ping (réseau tombé sans un mot, la passerelle ne l'a pas encore vu) cède sans alerte ; seules deux connexions vivantes sont coupées toutes les deux, avec alerte (une par machine et par heure : deux copies se coupent à chaque reconnexion), **sans révocation** : si une copie du jeton circule, la rotation la révèle à la connexion suivante (jeton retiré présenté) et révoque alors.
4. **Remise au plus une fois** : une directive n'est envoyée qu'une fois (`sent_conn`, posé avant l'envoi par la seule connexion qui le gagne). Au `hello`, une directive ouverte en base pour cette machine et non listée : jamais envoyée → envoyée ; envoyée par une connexion précédente → **perdue** (`DirectiveLost`, échec clair, jamais renvoyée). Une directive listée que la base a close reçoit `cancel` si elle tourne, `ack` si elle est finie (son résultat est jeté : il n'y a plus rien à arrêter). La machine marque sur disque une directive avant de la lancer ; après un plantage d'`agent connect`, elle la rend en échec (« agent connect s'est arrêté pendant la directive »).
5. **Résultat** : écrit dans la ligne (`result`) avant `CompleteActivity`, essayée 5 fois (10 s au plus chacune, 1+2+4+8 s d'écart : ~80 s si Temporal ne répond pas, ~15 s s'il refuse aussitôt), avec le contexte de la passerelle : une machine qui envoie son résultat puis part aussitôt ne le perd pas. Si la complétion échoue (Temporal injoignable), la directive reste ouverte, sans `ack`, et la **connexion reste** : la fermer ferait revenir la machine, donc tourner son jeton, à chaque essai pendant toute la panne. C'est le **balayage** qui reprend, depuis la base, toute directive `running` dont le résultat est écrit : un essai par directive et par balayage, chacun dans sa goroutine, 8 au plus à la fois, sans que le balayage les attende (en séquence, une panne de Temporal le bloquerait jusqu'à une minute par directive) ; puis `ack` si la machine est connectée, sinon à son prochain `hello` (directive close). Une directive dont la machine ne revient pas est complétée quand même, avant que l'activity n'expire faute de heartbeat. Une erreur de la base pendant la réception ferme la connexion (1011) : le résultat n'est peut-être pas écrit, la machine le renvoie. Une seule complétion à la fois par directive (connexion ou balayage), sans heartbeat pendant qu'elle se fait (un `NotFound` la fermerait comme `gone`). Un `canceled` que Temporal refuse (annulation non demandée, `InvalidArgument`) est rendu comme un échec.
6. **Alertes au propriétaire** (inscription, révocation, jeton réutilisé, deux connexions, nouvelle adresse) : journalisées et postées dans ses notifications (la pseudo-session des résultats de tâches planifiées, annoncées en SSE). « Nouvelle adresse » seulement quand une adresse était connue avant.
7. **Abus** : 256 Kio par message, `4 × max_directives + 10` messages/s par connexion (rafale ×4), calibré sur le rythme d'`agent connect` (un `progress` par directive et par 250 ms au plus, le dernier gagne), `progress` coupé à 1 Kio ; 500 demandes d'inscription en attente au plus, 20 par adresse et 10 min seulement si l'adresse du client est connue (`TRUSTED_PROXIES`, comme les connexions) ; 10 codes faux par utilisateur et 15 min.
8. **La machine pingue aussi** la passerelle (30 s), pour ne pas attendre toujours sur un réseau tombé sans un mot ; elle se reconnecte avec un backoff (1 s doublé jusqu'à 1 min, avec gigue), et s'arrête sur un refus définitif (401, 4001, protocole trop ancien 4003). La passerelle ne ferme en 4001 que pour un jeton refusé ou une machine révoquée ; une erreur de la base pendant l'admission ferme en 1011, et la machine revient (un redémarrage de PostgreSQL n'arrête aucune machine). Un seul `agent connect` par dossier de machine (`flock`, `LockFileEx` sous Windows : `machine/...` compile déjà pour Windows et macOS, le binaire non). La machine envoie dès son `hello` ; la passerelle ne remet de directive à une connexion qu'après son `welcome`, et `reconcile` envoie ce qui est arrivé avant.
9. **Échéance envoyée à la machine** : absolue (horloge du serveur) ; une machine à l'horloge très décalée couperait trop tôt ou trop tard, la borne réelle reste le `StartToCloseTimeout`.
10. **Déclencheur manuel** : `MachineEchoWorkflow` est enregistré sur tous les workers, et `PickMachine`/`RunOnMachine` dans leurs activities, pour `agent machine-echo` : sans lui, rien ne confie de directive en phase 0. Aucun workflow existant n'est modifié.
11. **Pas encore** (phases suivantes) : pause et priorité dans « Mes machines » (colonnes prêtes, pas d'interface), `progress` vers la note du tour (`session.Service.Observe`), trousseau du système, `/machines/files`, plusieurs répliques (`connected_to` est déjà là), notification par email.
