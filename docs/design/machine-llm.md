# Conception : le modèle sur la machine de l'utilisateur (phase 3)

Statut : **version 2.1** ; **phase 3.0 faite** le 7 octobre 2026 (§14 : ce qui est fait, et les écarts), 3.1 à faire. Version 1 le 7 octobre 2026, révisée le même jour après deux relectures ; les points de la seconde sont marqués *[rev2 1…9]*. Relecture contre le code (`activity/llm.go`, `workflow/agent.go`, `activity/machine.go`, `gateway/`, `machine/connect/`, `store/machine.go`) et les modules (`coder/websocket` 1.8.15, SDK Temporal 1.33). Les points issus de la relecture sont marqués *[rev. 1…16]*. Suite de `docs/design/machines.md` (§10 l'esquissait ; phases 0 à 2 et leurs écarts, §16 et suivants).

## 1. Objet

Faire tourner l'appel au modèle d'un tour (`CallLLM`) sur la machine de l'auteur du message, avec **sa** clé d'API et **son** modèle, par la passerelle des machines. Deux usages :
- **une installation sans clé** : un groupe où chacun apporte son modèle, le serveur n'en paie aucun ;
- **une installation avec clé** où un membre préfère payer ses tours lui-même, ou utiliser son propre modèle.

Hors périmètre : la CLI Claude Code comme moteur (un abonnement seul) ; c'est la phase 4 (`CLIAgentWorkflow`, pont MCP), seule à donner des outils à un agent dans ce cas. Une CLI « sans outils » comme moteur de `CallLLM` n'est pas prévue : un agent sans `web_search`, `ask_user` ni sous-agents perd l'essentiel.

## 2. Ce qui existe

`CallLLM` (`activity/llm.go`) est déjà coupé en deux :
1. `buildRequest` : charge la conversation, l'ordonne et la convertit, reconstruit les définitions d'outils depuis le catalogue, construit le prompt système (identité, comportements, skills, mémoire de l'utilisateur relue, `PartNote`), et dit ce que le prompt tient de la mémoire (`PromptMemory` : version, illisible). Il produit une `provider.ChatRequest` **neutre**, jusqu'à `LLM_MAX_CONTEXT_BYTES` (2 Mo par défaut), avec ses points de cache.
2. `Provider.Chat` : l'appel au fournisseur, avec la traduction des erreurs (`ContextTooLong`, `PermanentAPIError`, `RetryAfterError` → `NextRetryDelay`).

L'activity a `StartToCloseTimeout` 180 s et 6 essais (`workflow/agent.go`), et peut être mise sur une queue dédiée (`queueMap["CallLLM"]`). Elle sert les tours de session, les sous-agents (`agent_<id>`) et les tâches planifiées. Le résumé d'un fork et le rapport au parent appellent aussi le modèle (`ForkActivities.LLM`).

La phase 3 déplace la **seconde moitié** sur la machine. La première reste sur nos workers : elle lit la base et le catalogue.

## 3. Qui décide : une option par agent

`agents.llm_on_machine`, réglée dans `/admin` :
- **`never`** (défaut) : la clé du serveur, comme aujourd'hui ;
- **`prefer`** : la machine de l'auteur du tour si elle a la capacité `llm`, sinon la clé du serveur ;
- **`require`** : la machine de l'auteur, sinon le tour s'arrête avec un message clair (« connecte ta machine pour parler à cet agent »).

L'option est par agent parce que c'est l'agent qui fixe ce qui part sur la machine (son prompt, ses skills, ses outils, §9) : l'admin décide en connaissance de cause, agent par agent.

**Le modèle.** Celui de la machine prime : c'est elle qui paie. La machine applique la requête telle quelle (`MaxTokens`, système, outils, points de cache), **sauf** `Model`, qu'elle remplace par le sien ; le fil dit lequel a répondu *[rev. 10]*. Dans une installation d'entreprise, ce n'est pas forcément souhaitable (modèle imposé, liste permise) : une politique d'installation ou par agent viendra plus tard, sans changer le mécanisme.

**Serveur sans clé** : c'est la phase 3.1 (§12) *[rev. 14]*. En 3.0, `LLM_API_KEY` reste obligatoire (`provider.New` au démarrage, résumés de fork et rapports).

## 4. Le chemin d'un appel

**Au début d'un tour** dont l'agent n'est pas `never`, une activity **`ChooseMachine`** (capacité `llm`, auteur du tour `LLMTurnRequest.UserID`) choisit une machine, **sans créer de directive** : lecture des machines en ligne, pas en pause, avec la capacité et de la place dans leur plafond `llm`, plus haute priorité puis moins chargée *[rev. 15]*. Le tour garde l'ID de la machine : toutes ses étapes vont sur la même machine, avec le même modèle et le même cache de prompt. Rien trouvé : clé du serveur (`prefer`) ou arrêt du tour (`require`). C'est un résultat d'activity, dans l'historique : déterministe au rejeu ; un `AgentWorkflow` ne fait pas de continue-as-new (seul le participant en fait, entre les tours) *[rev. 16]*.

**À chaque étape** de la boucle ReAct, à la place de `CallLLM`, l'activity **`CallLLMOnMachine`** (sur nos workers ; même entrée de `queueMap` que `CallLLM`, et le worker de cette queue doit pouvoir remettre une directive : `MACHINES_ENABLED`, `NOTIFY_URL`, `INTERNAL_API_KEY` *[rev. 10]*) :
1. construit la requête avec `buildRequest` et applique la garde `LLM_MAX_CONTEXT_BYTES` du worker qui construit (un modèle plus petit sur la machine refusera par son API : traduit en `ContextTooLong` typé par la machine, comme aujourd'hui) *[rev. 11]* ;
2. crée la directive `llm` sur la machine du tour, sous verrou et sous le plafond `llm` de la machine, avec la clé **`llm:<étape>:<tentative>`** (la tentative est tenue par le workflow, §7) *[rev. 2]*, et range dans son entrée, en base, ce qui doit revenir au workflow : `PromptMemory` (version, illisible) *[rev. 3]*. La requête elle-même ne va jamais en base ;
3. la remet à la passerelle **dans le corps** de `POST /internal/machines/directives`, avec le jeton de tâche (corps jusqu'à 4 Mio, délai 60 s, pas de nouvel essai : un échec de remise est un échec de l'étape, §7 ; en `agent dev`, remise en mémoire, sans copie) *[rev. 10]* ;
4. rend `activity.ErrResultPending`.

La passerelle envoie la directive **avec la requête** sur la WebSocket de la machine (§5). La machine appelle son fournisseur et rend la réponse (texte, appels d'outils, raison de fin, usage, modèle) dans `result`. La passerelle **valide** ce résultat (§9), puis termine l'activity (`CompleteActivity`) avec une `LLMTurnResponse` : la `ChatResponse` et la `PromptMemory` relue dans la ligne de la directive, ce qui vaut aussi quand c'est le balayage qui complète depuis la base *[rev. 3]*. Ou l'erreur typée de la machine (§7). Le tour continue comme aujourd'hui : il dispatche les appels d'outils sur nos workers, et **n'exécute jamais un outil hors de l'allowlist de l'agent, quelle que soit la source de la réponse** *[rev. 8]*.

La requête ne passe **jamais** par Temporal : du worker à la passerelle par l'API interne, puis à la machine par la WebSocket. Seule la réponse (bornée par `max_tokens`) entre dans l'historique, comme la sortie de `CallLLM` aujourd'hui.

**Arrêter** : les options de `CallLLMOnMachine` ont `WaitForCancellation: true`, comme `RunOnMachine` ; un « Arrêter » annule l'activity, la réponse au heartbeat le dit, la passerelle envoie `cancel` et la machine abandonne l'appel *[rev. 10]*. Coût assumé : la passerelle n'apprend l'annulation qu'à son heartbeat suivant, donc le tour finit (et le fil cesse de dire « travaille… ») jusqu'à ~30 s plus tard qu'avec la clé du serveur. Dans la boucle de relance, une annulation, pendant l'appel comme pendant un `workflow.Sleep` d'attente, est un **arrêt** du tour (`cancelledOutput`, écriture protégée), jamais une erreur passagère à retenter *[rev2 3]*.

**Heartbeat** : `HeartbeatTimeout` (2 min) court dès le début de l'activity, pendant `buildRequest` et la remise ; le premier heartbeat de la passerelle part au plus 30 s après la remise : la marge suffit *[rev2 7]*.

**Note du tour** : pas de note par étape. Une note de tour, « modèle sur la machine « X » (sonnet) », posée au choix de la machine et effacée à la fin ; une directive `llm` n'envoie pas de `progress` *[rev. 10]*.

## 5. Transport : la requête dans la WebSocket

**La connexion porte les directives, requête LLM comprise, et leurs résultats ; l'API HTTP ne sert qu'à ce que la machine envoie de volumineux (les fichiers).** Pas de table ni de `GET` pour la requête : un aller-retour de moins à chaque étape (un tour fait souvent 5 à 15 appels au modèle), rien à stocker ni à nettoyer.

- **Remise immédiate ou échec** *[rev. 4]* : une directive `llm` est remise à une connexion prête au moment où la passerelle la reçoit, ou elle est close en échec typé (`MachineUnreachable`, §7). La passerelle ne garde la requête que le temps de l'écriture. `reconcile` ne renvoie **jamais** une `llm` (il n'a pas sa requête) : une `llm` qu'une machine liste à sa reconnexion est déclarée perdue.
- **Limites de lecture** *[rev. 5]* : `SetReadLimit` vaut par connexion, pas par type de message. Côté machine, une **constante du protocole**, 4 Mio pour tous les messages (le serveur est de confiance ; la machine ne connaît pas `LLM_MAX_CONTEXT_BYTES`). Côté passerelle, 4 Mio aussi (le `result` d'une étape porte les entrées des appels d'outils, `publish_file` jusqu'à 1 Mio), puis un plafond **par type** après lecture : tout message autre qu'un `result` de `llm` 256 Kio comme aujourd'hui ; le `result` d'une `llm` est borné par §9 (1,5 Mio au total), une limite que fixe **Temporal**, pas la WebSocket : la réponse devient le payload de `CompleteActivity` puis l'entrée de la réécriture du tour, et un blob dépasse 2 Mo = refus du serveur *[rev2 1]*.
- **Compression** *[rev. 12]* : `permessage-deflate` (`CompressionMode` de `coder/websocket`) des deux côtés, en **`CompressionNoContextTakeover`** : un compresseur pris dans un pool par message, plutôt que 1,2 Mo fixes par connexion ; le contexte entre messages n'apporte rien à une requête qui est un seul gros message. Le JSON d'une conversation se compresse d'un facteur 5 à 10.
- **Envoi en morceaux et délais** *[rev. 7]* : `Write` tient la trame entière, un ping attend derrière. La passerelle écrit une directive `llm` par un `Writer`, par morceaux de 64 Kio d'entrée ; les trames compressées ont la taille que produit flate, et un ping s'intercale entre deux trames (`writeFrame` verrouille par trame ; vérifié dans `coder/websocket` 1.8.15, compression comprise) *[rev2 4]*. Les délais d'écriture et de ping passent à 60 s pour toute connexion ; les pongs de la machine gardent leurs 5 s. Test `RealServer` avec une liaison bridée.
- **Redémarrage de la passerelle avant ou pendant l'envoi** : la directive est close en échec, l'étape échoue de façon retentable, le workflow la refait (§7) : la requête est reconstruite et relit l'état présent. Un appel au modèle n'a pas d'effet de bord.

## 6. Sur la machine

`agent connect` annonce la capacité **`llm`** quand il a un fournisseur configuré :
- `--llm-provider` (le registre `provider.New` du binaire : `anthropic` aujourd'hui), `--llm-model`, la clé dans **`AGENT_CONNECT_LLM_API_KEY`** : jamais `ANTHROPIC_API_KEY`, que le filtre de Claude Code retire ou garde selon `CLAUDE_CODE_AUTH` ;
- le fournisseur et le modèle sont affichés au démarrage et dans « Mes machines » ;
- **un plafond par famille** *[rev. 6]* : `--llm-max-concurrent` (défaut 4), annoncé dans `hello`, séparé du plafond des runs (`max_directives`). Les deux bouts comptent par famille (`coding` : `analyze_repo`, `implement_feature` ; `llm`) : `ChooseMachine` et la création de directive dans `store`, `Client.start` sur la machine. Une machine à un run à la fois sert donc encore le modèle pendant une analyse de 45 min ;
- l'appel passe par le même paquet `provider` que nos workers : mêmes erreurs typées, même cache de prompt. La machine ne réessaie pas elle-même : une erreur passagère remonte typée, et c'est le workflow qui décide (§7) ;
- **l'usage** *[rev. 10]* : `provider.ChatResponse` gagne un champ `Usage` (tokens d'entrée, de sortie, de cache), décodé par le fournisseur `anthropic`, sur nos workers comme sur la machine ; il est écrit sur le message assistant de l'étape, avec le modèle. La visibilité des coûts le lira.

## 7. Échecs et relances : dans le workflow

*[rev. 1]* Les essais d'une activity se font hors du workflow, sur la même entrée : avec 6 essais, une machine perdue serait retentée 5 fois **sur la même machine**, 2 min chacune, avant que le workflow puisse basculer. Donc :
- **`CallLLMOnMachine` a un seul essai** (`MaximumAttempts: 1`), `StartToCloseTimeout` 180 s, `HeartbeatTimeout` 2 min (la passerelle bat toutes les 30 s pendant l'appel) ;
- **la relance est dans `AgentWorkflow`**, qui tient le compte des tentatives de l'étape (6, comme aujourd'hui) et donne chaque fois une nouvelle clé de directive (§4.2) :
  - `PermanentAPIError`, `ContextTooLong` : arrêt, comme aujourd'hui (le message « forke-la » pour le second) ; une clé refusée ou un crédit épuisé retire aussi la capacité `llm` de la machine jusqu'à ce que sa configuration change ;
  - `RetryAfter` du fournisseur de la machine : `workflow.Sleep` du délai (plafonné à 2 min, valeur absurde ignorée, comme `NextRetryDelay` aujourd'hui), puis nouvelle tentative sur la même machine ;
  - **machine perdue ou injoignable** (`HeartbeatTimeout`, `MachineUnreachable`, remise échouée) ou **refus** (`DirectiveRefused` : plafond, capacité retirée) : en `prefer`, l'étape et **la suite du tour** passent sur `CallLLM` (clé du serveur), et le tour le note ; en `require`, un nouveau `ChooseMachine` cherche une **autre** machine de l'auteur, sinon le tour s'arrête avec un message clair. Le workflow tient la liste des machines écartées pendant le tour et la passe à `ChooseMachine` ; côté serveur, une machine qui a perdu une directive est écartée jusqu'à sa reconnexion : sinon, encore « vue depuis moins de 2 min », elle serait rechoisie *[rev2 2]* ;
  - toute autre erreur passagère : nouvelle tentative, avec l'attente croissante de la politique actuelle (5 s × 3, plafonnée à 2 min).
- **Durée** : par étape, 6 tentatives de 180 s au plus, avec des attentes de 2 min au plus : environ 28 min au pire, comme aujourd'hui avec la politique de l'activity ; le tour n'a pas d'autre borne *[rev2 6]*.
- **Directives réservées jamais démarrées** (worker mort entre la création et la remise) : le balayage les ferme (`orphaned`) après 10 min ; elles comptent dans le plafond `llm` jusque-là *[rev. 2]*.

Changer de modèle au milieu d'un tour est sans danger tant que le registre des fournisseurs n'a qu'`anthropic` : la conversation est neutre, et un appel d'outil d'un modèle se relit par un autre *[rev. 13]*. La bascule perd le préfixe du cache de prompt : un coût, pas une erreur *[rev. 10]*.

## 8. Sous-agents, tâches planifiées, résumés

- **Sous-agent** (`agent_<id>`) : il suit la route de son parent (la même machine, héritée par son entrée comme le canal, `buildChildInput`), sauf si sa propre option est `never` : il prend alors la clé du serveur. C'est voulu : l'admin a choisi `never` pour cet agent-là, quelle que soit la machine de l'appelant. `machines.md` §10 est corrigé dans ce sens *[rev. 9]*.
- **Tâche planifiée** : elle tourne pour son utilisateur, selon l'option de son agent, sur la machine de cet utilisateur. Machine éteinte : `prefer` passe sur la clé du serveur, `require` échoue (`task_logs`, et l'utilisateur est prévenu). Phase 3.1.
- **Résumé de fork et rapport au parent** : phase 3.1, quand le serveur n'a pas de clé ; ils passent par la machine de celui qui forke ou qui rapporte, avec une directive `llm` portant la `ChatRequest` du résumé.

## 9. Ce qui part sur la machine, ce qu'elle peut faire, ce qui est vérifié

La machine qui fait tourner le modèle **a l'autorité de l'agent** :
- **elle reçoit** le prompt de l'agent et ses skills, les définitions de ses outils, la conversation de la session (ce que l'auteur, membre, voit déjà ; les entrées et résultats `PrivateInput` des autres membres restent masqués par `conversation.Convert`), la mémoire de l'auteur seulement ;
- **elle décide** des appels d'outils que le tour exécute, dans l'allowlist de l'agent (`send_email`, `exec` sur nos workers, sous-agents…), sans injection de prompt nécessaire.

Dans un groupe qui se fait confiance, c'est acceptable ; c'est la raison de l'option par agent, `never` par défaut.

**Ce qui est vérifié à l'arrivée** *[rev. 8]*, par la passerelle avant `SaveDirectiveResult`, sur le `result` d'une `llm` (refus = échec typé de l'étape, comme une erreur de fournisseur) : **1,5 Mio au total** (texte, entrées des appels, usage : la limite de payload de Temporal, §5) *[rev2 1]*, 64 appels d'outils au plus, des IDs d'appel uniques et non vides, l'entrée de chaque appel en JSON objet valide, `StopReason` dans l'ensemble connu, le nom du modèle en chaîne courte (128 caractères). Un résultat refusé est quand même écrit, avec un statut d'erreur synthétisé, puis complété comme les autres : si Temporal est injoignable à cet instant, le balayage le complète, au lieu que l'étape attende son `HeartbeatTimeout` *[rev2 9]*. Le nom d'un outil hors de l'allowlist n'est pas une erreur de forme : le tour le traite comme aujourd'hui (« Not in this agent's allowlist, or unknown tool »).

**Traçabilité** : chaque message assistant d'une étape passée par une machine porte `machine_id`, le modèle et l'usage ; le fil l'affiche (« via la machine de Victor · sonnet »).

## 10. Ce qui change

- **Store** : `agents.llm_on_machine` ; `machine_id`, `model` et usage sur les messages assistant ; capacité `llm`, fournisseur, modèle et plafond `llm` dans l'état de la machine ; compte des directives ouvertes par famille.
- **`provider`** : `ChatResponse.Usage`, décodé par `anthropic`.
- **Workers** : `ChooseMachine`, `CallLLMOnMachine` ; dans `AgentWorkflow`, la boucle de relance et de repli par étape, la machine du tour et sa propagation aux sous-agents. `CallLLM` est inchangé pour la clé du serveur. Les commandes de l'`AgentWorkflow` changent pour les tours d'agents `prefer`/`require` (le chemin `never` reste celui d'aujourd'hui) : on termine les runs ouverts au déploiement (pas de prod, pas de `GetVersion`) *[rev2 8]*.
- **Passerelle** : directives `llm` avec leur requête (remise immédiate ou échec, envoi en morceaux, compression, limites par type), validation du résultat, `PromptMemory` relue en base, plafond par famille, `/internal/machines/directives` jusqu'à 4 Mio et 60 s.
- **`agent connect`** : la capacité `llm`, sa configuration, l'appel au fournisseur, le plafond `llm`.
- **Serveur** : l'option dans `/admin` ; l'affichage dans le fil, le panneau Agents et « Mes machines ».
- **Protocole** : une version de plus (sans compatibilité) ; délais de 60 s ; limite de lecture de 4 Mio.

## 11. Tests

Contre le vrai serveur (`RealServer` des machines, fournisseur factice sur la machine) : un tour routé sur la machine, toutes ses étapes sur la même machine, `PromptMemory` revenue (et après une complétion par le balayage) ; `prefer` sans machine ; `require` sans machine ; machine perdue en plein tour (`prefer` bascule pour la suite, `require` en cherche une autre) ; `RetryAfter`, `ContextTooLong`, `PermanentAPIError` venus de la machine ; machine non connectée à la remise (`MachineUnreachable`, puis repli) ; une requête de 1,5 Mo compressée, sur une liaison bridée, sans déconnexion ; un `result` mal formé refusé ; « Arrêter » pendant un appel ; un sous-agent qui suit son parent et un sous-agent `never` ; plafonds par famille (un appel au modèle servi pendant un run). Unitaires : routage, relance et repli, clés de directive, validation, limites.

## 12. Phases

| Phase | Contenu |
|---|---|
| **3.0** (fait, §14) | Option par agent, `ChooseMachine`, `CallLLMOnMachine` à un essai et relance dans le workflow, directive `llm` dans la WebSocket (remise immédiate, morceaux, compression, limites), validation du résultat, plafond par famille, `PromptMemory` en base, usage, sous-agents, traçabilité et affichage |
| **3.1** | Serveur sans clé, résumés de fork et rapports par la machine, tâches planifiées |

## 13. Questions ouvertes

1. Politique de modèle d'entreprise (modèle imposé ou liste permise) : quand et à quel niveau (installation, agent) ?
2. Un membre peut-il refuser que **ses** tours passent par sa machine pour un agent `prefer` (préférence utilisateur), ou est-ce le choix de l'admin seul ?

## 14. 3.0 : ce qui est fait, et les écarts

Fait (7 octobre 2026), tel que décrit plus haut : `agents.llm_on_machine` (`never` par défaut, `prefer`, `require` ; `/admin`, le seed `agents.yaml`, le catalogue des workers, lu par `LoadSkillsForAgent` au début du tour) ; `ChooseMachine`, `CallLLMOnMachine` (une méthode de `LLMActivities`, qui partage `buildRequest`) et `SetMachineAside` ; la relance et le repli dans `AgentWorkflow` (`workflow/agent_llm.go`) ; la directive `llm` dans la WebSocket (remise immédiate ou `MachineUnreachable`, morceaux de 64 Kio, `CompressionNoContextTakeover` des deux côtés, 4 Mio par message, validation du `result`) ; un plafond par famille (`coding`, `llm`) dans `store` et sur la machine ; `PromptMemory` dans l'entrée de la directive ; `ChatResponse.Usage` et `Model` ; la capacité `llm` d'`agent connect` ; l'affichage. Protocole 4, sans compatibilité avec le 3. Testé contre le vrai Temporal et la base jetable avec un fournisseur factice sur la machine (`TestMachinesLLM_RealServer` de `gateway/`, les scénarios du §11), sans appel payant ; la boucle en unitaire (`workflow/agent_llm_test.go`).

Précisions et écarts :

1. **Directive créée `running`** : `CallLLMOnMachine` a déjà son jeton de tâche quand il crée la directive ; elle naît donc `running` (pas `reserved` puis démarrée), dans la transaction qui tient la ligne de la machine (`store.CreateLLMDirective` : en ligne, pas en pause, capacité `llm`, pas écartée, sous `max_llm`, clé `(run, llm:<étape>:<tentative>)` jamais réutilisée). Un worker mort entre la création et la remise laisse une directive jamais envoyée : elle expire à l'échéance de l'activité (3 min), close `expired` par le balayage, au lieu d'`orphaned` après 10 min ; elle compte dans le plafond `llm` jusque-là (§7). `ChooseMachine` lit sans verrou : seule la création revérifie.
2. **Délai de la remise** : 60 s d'écriture côté passerelle (`machine.LLMWriteTimeout`), et 75 s côté worker (`machine.LLMHandoffTimeout`) pour que la réponse de la passerelle arrive avant que le worker abandonne ; une seule tentative. Le corps de `/internal/machines/directives` est lu jusqu'à 4 Mio + 4 Kio ; 424 = machine injoignable (la passerelle a clos la directive). La requête est bornée à `min(LLM_MAX_CONTEXT_BYTES, 4 Mio − 64 Kio)` (`machine.MaxLLMRequestBytes`) : au-delà, `ContextTooLong` comme la garde.
3. **Écartement côté serveur** : `machines.llm_aside_at`, posé par `SetMachineAside` (que le workflow appelle sur une machine perdue ou injoignable : timeout de heartbeat, `MachineUnreachable`, `MachineStopping`, `MachineRevoked` ; pas sur un refus, pas sur un `DirectiveLost`, constaté à la reconnexion : la machine est déjà revenue), levé par la connexion suivante (`connected_at` postérieur, `Machine.AsideForLLM`). « Mes machines » le dit.
4. **Classement des échecs** (`machineFailure`) : `PermanentAPIError` et `ContextTooLong` arrêtent ; perdue ou injoignable (ci-dessus) et `DirectiveRefused` font passer à la clé du serveur (`prefer`) ou à une autre machine (`require`, `ChooseMachine` avec les exclues) ; tout le reste (`DirectiveFailed`, `MachineBadResult`, timeout `StartToClose`) est retenté sur la même machine, avec l'attente de la politique (5 s × 3, 2 min au plus) ou celle du fournisseur (`RetryAfterError` : le délai est dans les détails de l'erreur, `machine.LLMFailure`, plafonné à 2 min, ignoré au-delà d'une heure : la machine n'est pas de confiance). Les 6 tentatives comptent toutes celles de l'étape, changements de machine compris ; en `prefer`, la bascule appelle aussitôt `CallLLM`, qui a sa propre politique. Un résultat refusé (`MachineBadResult`) est traité comme une erreur de fournisseur, donc retenté sur la même machine.
5. **Clé refusée** : `provider.PermanentAPIError.Credentials` (401, 402, 403, `authentication_error`, `permission_error`, `billing_error`, « credit balance ») ; la machine rend `LLMFailure{credentials}`, retire `llm` (message `capabilities`, état `refused`) jusqu'au redémarrage d'`agent connect`, et la passerelle prévient le propriétaire. Le tour s'arrête (`PermanentAPIError`), comme prévu, même en `prefer`.
6. **Sous-agents** (§8) : un sous-agent reçoit la machine de son parent (`AgentWorkflowInput.LLMMachine`, celle du moment : `nil` après une bascule) et l'utilise sauf si son agent est `never`. Précision : sans machine du parent (parent `never`, ou sur la clé du serveur), c'est l'option du sous-agent qui décide (il choisit une machine s'il est `prefer` ou `require`) ; « suivre la route du parent » aurait fait tourner un sous-agent `require` sur la clé du serveur. Une tâche planifiée (ni tour, ni chaîne d'agents) reste sur la clé du serveur (3.1).
7. **Limites de lecture** : un `result` est lu jusqu'à 4 Mio quel que soit son type, puis borné une fois sa directive lue en base (256 Kio hors appel au modèle, `conn.result`) : une machine renvoie ses résultats au `welcome`, avant que la passerelle ne porte leurs directives, et un résultat d'appel au modèle de plus de 256 Kio aurait coupé la connexion. Tout autre message : 256 Kio. Délais de ping : 60 s des deux côtés (la machine aussi, son pong peut attendre derrière une requête) ; le ping qui départage deux connexions d'une machine garde 10 s.
8. **Validation** (`machine.CheckLLMOutput`) : en plus du §9, un nom d'outil non vide ; les échecs de la machine sont lus comme `LLMFailure` (256 Kio au plus), un échec illisible est refusé comme une réponse mal formée.
9. **Traçabilité** : chaque message assistant porte `model` et `usage` (clé du serveur comprise : `anthropic` décode `model` et `usage`), et, venu d'une machine, `machine_id` **et** `machine` (son nom à l'écriture). Le fil signe « via la machine de Victor · modèle » (le propriétaire nommé par ses messages dans le fil, à défaut le nom de la machine), et « … puis le modèle de l'installation » après une bascule. Le panneau Agents n'a rien de plus : la note du tour y est déjà.
10. **Note du tour** : envoyée par `AgentWorkflow` (événement `notice`, web seul, une tentative de 3 s comme les événements de tour), seulement pour un tour de session : « modèle sur la machine « X » (modèle) » au choix, « modèle de l'installation : la machine « X » ne répond plus » à la bascule, la nouvelle machine en `require`, effacée à la fin du tour, arrêt compris (contexte déconnecté). La passerelle ne note rien pour une directive `llm`.
11. **`agent connect`** : `--llm-model` est obligatoire avec `--llm-provider` (le modèle de la machine remplace celui de la requête : il faut en avoir un) ; `--llm-max-concurrent` de 1 à 16 ; la clé seulement dans `AGENT_CONNECT_LLM_API_KEY` (absente de la liste blanche des sous-processus : la CLI ne la voit pas). Un appel n'envoie pas de `progress`. Une réponse au-delà de 1,5 Mio est refusée par la machine elle-même (échec passager).
12. **Tests** : la machine « perdue » du `RealServer` est un `agent connect` arrêté en plein appel (`MachineStopping`) ; une perte silencieuse attendrait le `HeartbeatTimeout` réel (2 min), couverte en unitaire par son classement. La liaison bridée est un proxy TCP à 150 Kio/s vers la machine : une requête de 1,5 Mo (hexadécimal, compressible de moitié) passe en ~6 s sans reconnexion, la passerelle avec le délai de ping de production.
13. **Déploiement** : les commandes d'`AgentWorkflow` ne changent que pour un agent `prefer`/`require` ; le protocole 4 refuse les `agent connect` antérieurs (4003, « mets à jour agent ») ; terminer les runs ouverts (participants et tours) au déploiement, comme d'habitude.
14. **Non vérifié** : un vrai appel au fournisseur depuis la machine (aucun appel payant), le `HeartbeatTimeout` réel d'une machine muette, plusieurs répliques (phase 5).
