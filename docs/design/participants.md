# Conception : des participants à la place du workflow de session

Statut : **proposition, version 3**, prête pour la phase 1. Version 1 le 4 octobre 2026, révisée deux fois le même jour après deux relectures contre le code et le SDK Temporal (v1.33, API v1.44.1), la seconde avec un prototype de la règle de lecture et de l'ordre. Les points issus de la première relecture sont marqués *[rev. B1…]*, ceux de la seconde *[rev2 N1…]*.

## 1. Pourquoi

Aujourd'hui, chaque session a un `SessionWorkflow` (`session-<id>`) qui traite ses messages **un par un** : il reçoit un signal, fait répondre le ou les agents (un `AgentWorkflow` enfant par agent, l'un après l'autre), attend la fin, puis passe au message suivant. Il se met en veille après 30 min sans message et se remet à neuf (*continue-as-new*) quand son historique grossit.

Ce modèle pose quatre problèmes :

1. **Un agent occupé bloque toute la session.** Pendant qu'un tour tourne, y compris un run Claude Code de 20 min qui est un enfant du tour, aucun autre message n'est traité, même adressé à un autre agent.
2. **On ne voit pas qui travaille.** L'état « l'agent travaille » est celui de la session, pas d'un agent.
3. **La conversation est modélisée comme un processus.** Or c'est un ensemble de données (en base) sur lesquelles s'exécutent des traitements (les tours). Toute la mécanique de vie d'un workflow long (veille, reprise, `SignalWithStart` sur un identifiant fixe, remise à neuf) n'existe qu'à cause de ce choix.
4. **Les tâches longues sont synchrones.** Un agent qui lance une analyse est indisponible jusqu'à sa fin.

## 2. Objectifs et non-objectifs

**Objectifs**
- Chaque agent d'une session est un **participant** : il traite ses messages **dans l'ordre, un à la fois**. C'est le comportement d'un chatbot, porté par l'agent et non plus par la plateforme.
- Des participants différents travaillent **en parallèle**.
- L'ordre d'un participant est garanti **par Temporal**, sans file en base et sans workflow qui vit plus longtemps que son travail.
- On sait à tout moment, par participant, s'il est disponible, s'il répond, combien de messages l'attendent, et s'il a une tâche de fond.
- Ce modèle prépare la voie express `/btw`, les tâches longues asynchrones, les triggers et les agents nés de la discussion.

**Non-objectifs (pour cette refonte)**
- Les triggers et les fichiers ont leurs propres chantiers. Ce document fixe seulement leur point d'entrée : poster un message.
- Le pool d'utilisateurs système (plusieurs runs par worker) reste un chantier à part.
- Aucune compatibilité avec les workflows ouverts : il n'y a pas de prod. Au déploiement, on termine les `SessionWorkflow` ouverts.
- Les tâches planifiées restent inchangées jusqu'à la phase 4 : elles n'ont aucun lien avec les sessions (`workflow/scheduled_agent.go`, `activity/delivery.go`).

## 3. Le modèle

| Notion | Ce que c'est | Où ça vit |
|---|---|---|
| **Session** | Messages, membres, forks, agent par défaut (`sessions.agent_id`) | Postgres uniquement, plus aucun workflow |
| **Participant** | Un agent dans une session : un agent *défini* (Jarvis) ou une *instance* née d'une tâche (§9) | Un workflow `ParticipantWorkflow`, **seulement pendant qu'il a du travail** |
| **Tour** | La réponse d'un participant à un message : boucle LLM + outils | `AgentWorkflow` enfant, court, comme aujourd'hui |
| **Message** | Ce qu'écrit un membre, ou ce que poste un trigger ou une tâche | Postgres, puis un signal au participant concerné |

### Qui répond à un message

Les règles actuelles ne changent pas (`session.Service.Deliver`) : mentions résolues côté serveur (au plus 3), `agent_mode` (`auto`, `always`, `mention`) quand il n'y a pas de mention. Sans mention, c'est l'agent par défaut de la session (`sessions.agent_id`, via `agentOrDefault`). Ces règles donnent une **liste ordonnée** de participants à faire répondre.

## 4. Les workflows

### 4.1 Nommage

Les identifiants de workflow aujourd'hui commencent par `<session>-` (`<session>-turn-N`, `<session>-tool-…`, questions `ask_user`). Un identifiant `<session>-<agent>` pourrait entrer en collision avec un agent nommé `turn` ou `tool`. On réserve donc le séparateur `:`, que les identifiants d'agents ne peuvent pas contenir :

| Workflow | Identifiant |
|---|---|
| Participant (agent défini) | `<session>:p:<agent>` |
| Participant (instance, §9) | `<session>:i:<instance>` |
| Tour | `<participant>:m<message id>` |
| Aparté (`/btw`) | `<participant>:btw` (et son tour : `<participant>:btw:m<message id>`) |
| Outils lancés par un tour, questions `ask_user` | préfixés par l'identifiant du tour |

**Règle unique :** la session d'un identifiant de workflow, c'est **ce qui précède le premier `:`**. Toutes les requêtes « dans cette session » deviennent `WorkflowId STARTS_WITH '<session>:'`. On valide l'`agent_id` (et l'ID d'instance) avant de l'interpoler dans une requête de visibilité, comme on valide déjà l'UUID de session.

Les workflows `fork-<session>` et `report-<fork>-<from>` ne commencent pas par la session et gardent leur nom : ils sont trouvés par leur identifiant exact, jamais par préfixe.

**Unicité.** Le nommage repose sur des identifiants uniques et stables :
- **Agent défini** : son `agent_id`, clé primaire de la table `agents`, immuable, de forme `[a-z0-9-]`, donc jamais de `:`, `.` ni `@`. On utilise l'identifiant, **pas la mention** : la mention (`@jarvis`) peut changer dans `/admin` sans toucher aux identifiants de workflow. Elle est déjà unique sans tenir compte de la casse, et ne peut pas égaler l'identifiant d'un autre agent.
- **Instance née d'une tâche** (§9) : un espace de noms à part (`:i:`), pour qu'une instance ne puisse jamais prendre l'identifiant d'un agent défini créé plus tard. L'identifiant est généré, unique par construction (l'ID de sa ligne en base), jamais dérivé du nom du dépôt. Sa mention lisible (`@analyste-temporal-agent`) est unique dans la session et ne peut pas entrer en collision avec celle d'un agent défini ; en cas de doublon, on ajoute un suffixe `-2`.
- **Agent supprimé puis recréé avec le même identifiant** : Temporal permet de réutiliser un identifiant une fois le workflow précédent terminé (politique par défaut `ALLOW_DUPLICATE`). Les messages encore en file d'un agent supprimé échouent proprement (§4.6).

**Ce qui doit changer** *[rev. I6]*, liste à tenir à jour pendant l'implémentation :
- `session/status.go` : `uuidPrefix` (exige aujourd'hui `id[36] == '-'`), et la requête des statuts ;
- `session/temporal.go` : requêtes de session, questions en attente, `activeWorkflowID`, `State`, `Cancel` ;
- `session/ops.go` : `Open`, `Delete`, `Leave` ;
- `workflow/claude_code_session.go` : `toolCallSession`, qui retrouve la session en coupant à `-tool-` ;
- `workflow/ask_user.go` et `workflow/agent.go` (`childWorkflowID`) ;
- `tool/query_workflow.go` et sa requête par défaut `session-state` ;
- `cmd/agent/api.go` (état, flux), `cmd/agent/ui.go`, `web/chat/render.go` (états, `Active`) ;
- le commentaire de `maxAgentsPerMessage` (`session/messages.go`), qui décrit le traitement séquentiel *[rev2 N14]* ;
- la documentation : `docs/session-flow.md`, `docs/architecture.md`, `README.md`, `CLAUDE.md`.

### 4.2 `ParticipantWorkflow`

**Démarrage et livraison.** Le serveur ne démarre jamais un participant directement. Pour chaque message, il fait un `SignalWithStart` sur l'identifiant du participant, avec le signal `message`. La politique de conflit est `USE_EXISTING`, qui est d'ailleurs le défaut pour un `SignalWithStart` (`FAIL` y est invalide) :
- si le participant ne tourne pas, Temporal le démarre et lui livre le message ;
- s'il tourne, le message rejoint sa boîte de réception (le canal de signaux, durable dans son historique).

Le serveur reste sans état : un seul appel, toujours le même. L'opération est atomique côté serveur.

**Entrée de démarrage** : la session, l'agent, le canal et `ChannelID` de la session. Comme l'entrée est figée au démarrage (`USE_EXISTING`), **tout ce qui varie d'un message à l'autre voyage dans le message** (§4.7).

**Le signal ne porte pas le texte** *[rev2 N5]* : tout message est déjà en base avant d'être livré. Le signal porte son identifiant et ce qui l'accompagne (§4.7) ; le tour charge le texte avec la conversation. Ainsi, ni un signal, ni la boîte emportée par une remise à neuf ne peuvent approcher la limite de 2 Mo par donnée, même avec des relais (exemptés du plafond) ou des résultats de tâches en continu (phase 4). Le repli actuel sur `LastMessageID` pour un message non enregistré disparaît : il n'y en a plus.

**Boucle.**

```
démarrage(entrée)
boucle :
    si la boîte est vide (ReceiveAsync) : terminer
    msg := message suivant
    si msg.ID a déjà son turn_end pour ce participant : ignorer (relivraison, §8)
    vider un stop-turn périmé, juste avant turn_started : un stop ne vise
    qu'un tour annoncé (pas celui dont on vérifie encore le message)
    tour := AgentWorkflow enfant <participant>:m<msg.ID>
    attendre la fin du tour, ou stop-turn / clear
    responsabilités de fin de tour (§4.6)
    si msg.Next n'est pas vide et le tour a réussi : relayer (§4.4)
    si l'historique devient gros : continue-as-new en passant la boîte en entrée
```

**Fin sans perte** *[rev. I3]*. Quand la boîte est vide, le workflow se termine. Si un signal arrive pendant cette décision, **le serveur Temporal** refuse la fin : la tâche échoue avec `WORKFLOW_TASK_FAILED_CAUSE_UNHANDLED_COMMAND` (« some new command (like a signal) was processed into workflow history. The outstanding task will be failed »), et une nouvelle tâche apporte le signal. C'est une garantie du serveur, pas du SDK, et l'environnement de test du SDK ne la simule pas : elle sera vérifiée par un test contre un vrai serveur (§11).

**Signaux.**

| Signal | Effet |
|---|---|
| `message` | Ajoute un message à la boîte |
| `stop-turn` | Annule le tour en cours, pas les messages en attente |
| `clear` | Annule le tour en cours et vide la boîte (bouton « tout arrêter ») ; chaque message jeté reçoit un `turn_end` « annulé », pour qu'une relivraison tardive ne le rejoue pas *[rev2 N8]* |

**Requête `state`** (remplace `session-state`) : le message en cours (ID, auteur, depuis quand), le nombre de messages en attente, l'éventuelle tâche de fond. C'est la source du panneau « Agents ».

**Remise à neuf** *[rev. I10]*. Un participant ne vit que le temps de vider sa boîte, donc son historique reste court en général. Mais une file jamais vide (la phase 4 peut l'alimenter en continu) ne se remettrait jamais à neuf si on attendait une boîte vide. On applique donc la garde de taille après chaque tour, et le `continue-as-new` **emporte les messages encore dans la boîte** dans son entrée : la boîte est vidée par `ReceiveAsync`, puis `NewContinueAsNewError` dans la même tâche. Un signal arrivé pendant la décision est protégé par le même mécanisme que la fin (le refus `UNHANDLED_COMMAND` vaut pour toute commande de clôture). Une assertion `GetUnhandledSignalNames(ctx)` vide avant de retourner coûte une ligne. Les messages ne portant pas de texte, l'entrée reste petite.

### 4.3 `AgentWorkflow` (le tour)

Il reste presque tel quel. Ce qui change :
- **Sa clé de tour nomme le participant** *[rev. B1]* : `m<id du message>.<participant>` (`store.TurnKey`), où `<participant>` est l'`agent_id`, `i=<instance>` pour une instance, `<agent>~btw` pour un aparté. L'ancre est l'ID qui suit `m` (`store.TurnAnchor`) ; `store.TurnParticipant` lit ce qui suit le dernier `.`. *Simplifié après la v3 :* plus de groupe ni de rang ; les tours d'un même message (relais compris) se reconnaissent à leur ancre, et l'ordre du relais est porté par `EarlierTurns`. Un participant ne répond qu'une fois à un message : sa clé de tour est unique dans la session. `TurnOf` ne reconnaît que cette forme, suivie d'un index ou de `end` : il renvoie `false` pour le résumé de fork (`fork-summary:0`) et pour toute autre clé. **Toute clé de tour porte une ancre** : il n'existe plus de groupe sans ancre *[rev2 N13]*.
- **Le `turn_end` a sa propre clé**, `<clé du tour>:end` (`store.TurnEndKey`) *[rev2 N1]*, écrite par un `store.AppendTurnEnd` après la réécriture du tour. Il ne peut pas prendre un index de message du tour : `AppendMessages` ignore une clé déjà prise (`ON CONFLICT DO NOTHING`), et un `turn_end` écrit à l'index 0 après un échec, alors que le tour avait déjà écrit son message 0, serait silencieusement absorbé. `TurnOf` attribue `<clé>:end` au tour.
- **Ses identifiants dérivés** (outils, `ask_user`) partent de son propre identifiant de tour.
- **`AgentWorkflowInput.Model` reste** : il sert aux sous-agents (`agent.go`), même si l'API de session ne le propose plus *[rev2 N10]*.
- Il continue d'écrire ses messages au fil de l'eau. Ce qu'il faisait faire à la session passe au **participant**, pas au tour (§4.6) *[rev. I4]* : le participant sait quand l'enfant meurt, le tour lui-même pas toujours (un tour qui échoue avant d'écrire, ou terminé de l'extérieur, n'écrit rien).

### 4.4 Un message adressé à plusieurs participants : le relais

« @jarvis résume, @smith à partir de là… » : la dépendance est écrite dans le message, donc Smith doit voir la réponse de Jarvis à **ce** message.
- Le serveur ne sonne que chez le **premier** participant, avec `Next = [smith]` et `EarlierTurns = []`.
- Quand le tour de Jarvis se termine **sans échec ni arrêt**, le participant de Jarvis relaie le message à Smith avec `Next = []` et `EarlierTurns = [clé du tour de Jarvis]`.
- **Le relais est une activity**, pas un appel du workflow *[rev. I1]* : un workflow ne peut pas faire de `SignalWithStart` (`SignalExternalWorkflow` ne démarre rien, et un workflow enfant n'a pas de politique de conflit). L'activity utilise le client Temporal, comme `tool/schedule.go`. Ses relances génèrent un nouveau `RequestId` : c'est la vérification « déjà traité » chez Smith (§8) qui empêche le doublon.
- **Contrat d'échec** *[rev2 N6]* : l'activity a une politique de relance bornée (par exemple 5 essais, 1 min au plus). Après le dernier échec, le relayeur écrit un **`turn_end` d'erreur sous la clé du tour du destinataire** (« le relais vers @smith a échoué »), puis notifie le canal. La déduplication ignorera ensuite toute relivraison tardive de ce message chez Smith, ce qui est voulu. L'échec du relais ne fait **jamais** échouer le participant qui relaie : il passe à son message suivant.
- **Plusieurs relais** : le destinataire reçoit `Next` = le reste de la liste, et `EarlierTurns` = les clés des tours précédents, cumulées.
- Si le tour de Jarvis échoue ou est arrêté, le relais s'arrête, comme aujourd'hui.
- Pendant ce temps, Smith peut traiter d'autres messages : le relais n'est qu'un message de plus dans sa boîte.

C'est un trigger implicite (« quand Jarvis aura fini ce message, à Smith »).

### 4.5 La voie express `/btw`

- Un message `/btw …`, ou le bouton « en aparté » à côté de « Jarvis travaille… », est livré par `SignalWithStart` sur `<participant>:btw`. Un seul aparté à la fois par participant, garanti par l'identifiant.
- C'est le même `ParticipantWorkflow`, avec une option `aside` : le tour est lancé **sans outils**, ou avec des outils en lecture seule, pour ne jamais entrer en conflit avec les effets du tour principal.
- Sa clé de tour nomme un participant distinct (`<agent>~btw`), pour que la règle de lecture (§5) le traite comme un autre participant : l'aparté ne lit pas le tour principal en plein vol, et le tour principal ne le lit pas *[rev. m6]*.
- L'échange est enregistré dans le fil avec `kind = aside`, affiché « en aparté », et lu par les tours suivants.

### 4.6 Ce que le participant fait à chaque tour *[rev. I2, I4]*

Le participant reprend toutes les responsabilités de fin de tour de l'actuel `SessionWorkflow` (`workflow/session.go`), plus une vérification :
1. **Avant le tour : l'agent existe-t-il ?** Une activity interroge **la base** (`store.GetAgent`), pas le catalogue en mémoire, rafraîchi toutes les 30 s, qui refuserait un agent tout juste créé *[rev2 N7]*. Aujourd'hui, `LoadSkillsForAgent` (`activity/skill.go`) ne renvoie aucune erreur pour un agent inconnu : le tour répondrait avec un prompt générique sans outils. Agent inconnu = erreur non retentée, `turn_end` d'erreur, message suivant.
2. **`turn_started`** au démarrage du tour, **`turn_done`** quand il est fini, quoi qu'il arrive.
3. **Réécriture idempotente** des messages du tour à la fin, comme aujourd'hui : c'est le dernier filet si la dernière écriture du tour a échoué.
4. **`turn_end`** : un dernier message de fin de tour, sous la clé `<tour>:end` (§4.3), dans tous les cas (succès, erreur, arrêt, échec avant toute écriture), jamais montré au modèle. Il **unifie l'actuel `turn_error`** : `turn_end` porte l'éventuelle erreur, affichée dans le fil comme aujourd'hui. Il couvre au passage un cas que le code actuel rate : un échec dur du tour n'écrit aujourd'hui rien. Il sert à la règle de lecture (§5) et à la déduplication (§8). Un `turn_end` **sans** erreur n'est affiché nulle part : `conversation.Convert`, la vue du fil (`web/chat/views.go`) et l'historique de l'API (`cmd/agent/api.go`) doivent l'ignorer *[rev2 N9]*. Si son écriture échoue définitivement, le participant le journalise et passe au message suivant : le tour reste invisible aux autres participants, et serait rejoué par une relivraison (§8).
5. **Notification d'erreur** sur le canal, comme aujourd'hui (`session.go`, « Error processing message »).

Ce que le participant **abandonne** de la session : `GoalAchieved` (jamais utilisé), et le compteur de tours (remplacé par l'ID du message).

### 4.7 Contrats *[rev2]*

**Signal `message`** (aucun texte) :

| Champ | Rôle |
|---|---|
| `message_id` | Le message, déjà en base ; c'est aussi l'ancre (`upTo`) |
| `user_id`, `user_name` | L'auteur, dont la mémoire est chargée et pour qui les outils agissent |
| `next` | Les participants suivants du relais |
| `earlier_turns` | Les clés des tours précédents du même message |
| `sign_reply` | Signer la réponse sur le canal |
| `channel`, `channel_id` | Le canal de réponse de ce message |
| `aside` | Aparté (§4.5) ; **phase 3**, pas encore dans le contrat |

La citation du message dans la note multi-agents (`partNote`) est chargée par le tour avec la conversation, ou clippée dans le signal.

**Requête `state`** : `{current: {message_id, user_name, since} | null, queued: N, background: [...]}`.

**Politiques de relance** :

| Étape | Relance |
|---|---|
| Vérification du tour (`CheckTurn` : session, `turn_end`, `store.GetAgent`) | Un nombre d'essais, pas une durée : 12 essais, intervalle 1 s doublé jusqu'à 15 s (tentatives à 0, 1, 3, 7, 15, 30… 120 s). Une base absente environ 2 min (redémarrage, bascule) ne perd aucun message. Pas de `ScheduleToCloseTimeout` : il compterait aussi l'attente d'un worker, et un worker absent quelques minutes (un redéploiement) ferait échouer l'étape ; avec des essais, l'étape attend le worker comme le workflow. Seules erreurs définitives : agent absent (`AgentNotFound`) et session supprimée (`SessionGone`), non retentables |
| Relais | Bornée, environ 5 essais sur 1 min ; échec final selon §4.4 |
| `turn_end` | Comme la vérification : 12 essais ; échec final selon §4.6 |
| `turn_started` / `turn_done` | Les options courtes actuelles (une tentative de 3 s) |

## 5. La règle de lecture de l'historique

C'est le point le plus délicat. `store.TurnReads` dit aujourd'hui qu'un tour lit :
1. la session jusqu'au message auquel il répond ;
2. ce qu'a écrit son propre groupe (les tours du même message) ;
3. **ce qu'ont écrit les tours des messages précédents, même après lui**, au motif que « they ran before it, as a session answers its messages one at a time ».

Avec des participants en parallèle, la règle 3 ne tient plus, et la règle 1 non plus *[rev. B2]* : le tour de Smith (message 12) lirait, par la règle 1, l'appel d'outil de Jarvis (ID 11) écrit pour le message 10, sans son résultat (ID 13). L'API refuse une telle requête, et c'est exactement « lire l'autre en plein vol ».

**Nouvelle règle.** Un tour du participant P, pour le message M (`upTo = id(M)`), lit :
1. tout message **qui n'est pas d'un tour** et dont l'ID est ≤ `upTo` (messages humains, résumé de fork, rapports, résultats planifiés) ;
2. **sa propre clé** et les tours de `EarlierTurns` (le relais), **entiers** *[rev2 N2]* : un tour relit ses propres appels et résultats déjà en base ;
3. les tours **de P** dont l'**ancre est ≤ `upTo`**, **entiers**, même écrits après `upTo` *[rev2 N3]*. On raisonne par l'ancre, pas par l'ordre de traitement : avec le relais, Smith peut traiter le message 12 puis recevoir le message 10, et son tour sur le 10 ne doit pas lire son tour sur le 12 ;
4. les tours d'un **autre** participant Q, **entiers ou pas du tout** : lus si et seulement si leur `turn_end` a un ID ≤ `upTo`, c'est-à-dire s'ils étaient **terminés avant M**.

Le prédicat est monotone et figé pour toute la durée du tour : le début de la conversation ne change pas d'un appel LLM à l'autre, sans rien ajouter à l'entrée de l'activity. `upTo` et le participant se lisent dans la clé du tour (`store.ScopeOf`), et le chargement lit la session en une seule requête, donc un seul instantané : il en tire l'ID des `turn_end`, puis filtre les messages en mémoire (`LoadConversation`). Un `turn_end` n'est jamais lu par un tour.

**Un participant mort sans `turn_end`** (participant terminé de l'extérieur, base en panne au moment de l'écrire ; un crash de worker, lui, est rejoué par Temporal) : son tour reste **invisible aux autres participants pour toujours**, mais lu par ses propres tours suivants. C'est acceptable : il n'a jamais été « terminé » pour les autres.

**L'ordre de présentation au modèle change aussi** *[rev. B3]*. `conversation.Order` repousse aujourd'hui tout message hors tour qui tombe dans l'étendue d'un groupe, en supposant les tours contigus dans le temps. En parallèle, Jarvis (message 10, IDs 11 à 40) et Smith (message 12, IDs 13 à 20) se chevauchent : le message 12 serait relâché après 40, et la réponse de Smith resterait à 13-20, avant sa question. Nouvel ordre, **par ancre** :
- chaque tour est placé juste après son ancre, c'est-à-dire **après le dernier message hors tour d'ID ≤ son ancre** (l'ID qui suit `m` dans sa clé, `store.TurnAnchor`) *[rev2 N13]* ;
- les tours d'une même ancre sont rangés par leur premier ID (le relais garde son ordre) ;
- les messages hors tour sont rangés par ID ;
- le tour courant vient en dernier, puis la queue non encore écrite (`Tail`).

L'appariement appel/résultat d'outil est conservé (un tour reste un bloc), une question précède toujours sa réponse, et la conversation ne finit jamais sur un message assistant d'un autre participant : le tour courant est dernier par construction (son ancre est le plus grand ID hors tour qu'il lit, et un tour d'un autre participant ancré au même message aurait un `turn_end` après cette ancre, donc ne serait pas lu). En traitement séquentiel, cet ordre est **identique** à l'ordre actuel : vérifié par un prototype sur 3000 historiques aléatoires, sans aucun écart. Seul appelant : `activity/llm.go`.

La course documentée sur `TurnReads` (IDs pas dans l'ordre des commits) subsiste, un peu plus fréquente avec du parallélisme, avec la même conséquence bénigne *[rev. m9]*.

## 6. Côté serveur

**`Deliver`**, dans cet ordre *[rev2 N4]* :
1. résout les participants (inchangé, `agentOrDefault` pour l'agent par défaut) : la résolution ne dépend que du texte ;
2. **contrôle la file** : si le premier participant a déjà 5 messages en attente, refus (429, et sur Telegram une réponse en texte, envoyée dans la réponse du webhook), **sans rien enregistrer**. Le compte vient de l'état en mémoire du serveur (§6) : les messages qu'il a livrés à ce participant et dont aucun événement de tour n'a encore dit le début ou la fin ; pas d'une requête Temporal sur le chemin de la requête ; en cas de doute (serveur redémarré), on laisse passer, et deux envois simultanés peuvent dépasser le plafond d'un message. Le relais en est exempté ;
3. enregistre le message (inchangé) ;
4. fait un `SignalWithStart` chez le premier (§4.7).

`signalSession` et le démarrage de session à l'ouverture (`Open`) disparaissent *[rev. m1]*.

**Événement `user_message`** : enrichi, pour chaque participant appelé, d'un `queued_behind` (le message en cours, s'il y en a un), au lieu d'un nouvel événement de workflow.

**Arrêter** *[rev. I5]* :
- **Phase 1** : le bouton actuel « Arrêter » (`Cancel(sessionID)`, appelé par `cmd/agent/ui.go` et `cmd/agent/api.go`) envoie `stop-turn` à **tous** les participants en cours de la session.
- **Phase 2** : un bouton par participant, autorisé à l'auteur du message en cours ou au créateur de la session.

**Supprimer ou quitter une session** *[rev. I7]* : `Delete` et le `Leave` du dernier membre **terminent tous les participants** de la session et leurs tours (aujourd'hui, ils ne terminent que le workflow de session). Sinon, un tour écrirait dans une session supprimée, et une question `ask_user` attendrait 72 h. La liste vient de la visibilité, qui a un léger retard, complétée par les participants que les événements de tour disent au travail : un participant démarré à l'instant peut encore survivre. *Corrigé à l'implémentation :* `messages` n'a pas de clé étrangère vers `sessions`, un tour en cours peut donc y laisser des lignes orphelines (sans lecteur : la session n'existe plus) ; au tour suivant, la vérification (`CheckTurn`) trouve la session absente et le participant s'arrête sans tour *[rev2 N15]*.

**Qui travaille** *[rev. I8]* :
- `WorkflowId STARTS_WITH '<session>:p:'` (et `:i:`) + `ExecutionStatus = 'Running'` donne les participants actifs. La requête `state` de chacun donne le détail.
- L'état des tours en mémoire (`session/turns.go`, alimenté par `turn_started` et `turn_done`) devient **indexé par participant**, et l'état d'une session est l'**agrégat** de ses participants : sinon, le `turn_done` d'un participant masquerait le travail d'un autre. Les points de l'arbre et la ligne « … travaille… » en dérivent ; elle peut nommer plusieurs agents. Un événement de moins de 30 s l'emporte sur la visibilité. Au-delà, un participant que la visibilité liste en cours ne travaille que s'il est sur un tour (`turn_started` sans `turn_done`) ou a un message livré par ce serveur et pas encore commencé : un participant dont le dernier événement est son `turn_done` ne travaille pas, quel que soit son âge (il tourne encore entre deux tours : `EndTurn` relancé, relais, vérification d'un message qu'il ne traitera pas). Limite connue de la phase 1 : un `turn_started` perdu sur un message relayé (que ce serveur n'a pas livré) laisse ce tour invisible jusqu'à sa fin.
- `StatusActive` (l'état « session ouverte » du workflow de session) disparaît, et `Active` est retiré de la liste des sessions (`GET /me/sessions`).
- `GET /sessions/{id}/state` renvoie l'**agrégat des états des participants** (la liste des requêtes `state`), et l'outil `query_workflow` prend `state` comme requête par défaut *[rev2 N11]*.
- Questions en attente (`ask_user`) : la même requête qu'aujourd'hui, avec le nouveau préfixe. Avec plusieurs questions ouvertes, une réponse Telegram va à **la plus ancienne** (aujourd'hui `PageSize: 1`, au hasard) *[rev. m5]*.

**Le fil** (`web/chat`, `BuildThread`) est **chronologique, avec citation** *(révisé après essai : par ancre, la revue d'un relecteur finie à 16 h 30 s'affichait au-dessus de la question de 16 h 27 posée à Jarvis)*. Le modèle, lui, lit toujours par ancre (`conversation.Order`, §5) ; seul l'affichage change.
- Messages des membres, résumé de fork, rapports et résultats planifiés : par ID. Deux résultats planifiés consécutifs sont deux éléments.
- Un tour = un bloc (texte, outils, signature), placé à l'ID de son `turn_end` : là où il a fini. Un `turn_end` en erreur s'affiche au même endroit (l'encadré d'erreur, à l'heure de la fin), sous la réponse du tour s'il y en a une.
- **Heure d'un bloc** : son début, ou « début → fin » (« 16:27 → 16:37 »). Une seule heure quand il a fini dans la minute d'horloge où il a commencé (même minute, pas « moins de 60 s » : jamais « 16:27 → 16:27 », et un tour de 50 s à cheval sur deux minutes affiche les deux). Une session séquentielle garde donc l'heure de début de ses réponses rapides, comme avant. En cours : « 16:27 → en cours » ; l'heure ne saute jamais, elle se complète. Un tour qui ne finira jamais : « 16:27 → interrompu ».
- Un tour sans `turn_end` est **en cours** : en bas du fil, après tout le reste (par premier ID s'il y en a plusieurs), « → en cours » ; la ligne « … travaille… » et les questions `ask_user` viennent dessous. Un tour sans fin que son participant a suivi d'un autre ne finira jamais (participant arrêté de l'extérieur) : il s'affiche à son dernier message, « → interrompu ». Ne compte comme tour suivant qu'un tour qui a écrit autre chose que sa fin : un bloc fait de sa seule fin (relais échoué, file vidée, refus de `CheckTurn`) peut être écrit par un autre que le participant et ne prouve pas qu'il a avancé. Compromis : un tour mort suivi seulement de telles fins reste « en cours ».
- **Citation** : le premier élément d'un tour affiche « ↩ en réponse à Victor : <début de la question> » (une ligne, 120 caractères, échappé) seulement si autre chose s'affiche entre sa question (l'ancre) et lui. Le lien va à `#m<ancre>` (défilement et surbrillance) ; c'est un pas d'historique (`pushState`) : « retour » ramène où on lisait. Au relais, chaque tour non adjacent cite la même question. Ancre sans auteur : « en réponse au brief », « au rapport du fork « … » », ou à l'agent d'un résultat planifié.
- Quand chaque message est écrit après la fin des réponses au précédent, le fil est celui d'avant (ordre par ancre), sans citation (test aléatoire `TestBuildThread_OneAtATimeAsBefore`). Un relais séquentiel cite au second tour.
- **Fork** : un fork depuis un bloc fini part de son `turn_end` (`ThreadItem.ForkID`, accepté par `forkable` : assistant, sans résultat d'outil) : il reprend tout ce que le fil affiche au-dessus du bloc, et le début des tours encore en cours affichés dessous. Depuis un tour en cours ou interrompu, un message ou un rapport : son ID (pour un tour, son dernier texte). « Forker un fil » part du plus grand de ces points. Le lien « forké depuis » (`#m<turn_end>`) retrouve le bloc par `data-fork`. Ce que l'utilisateur veut au fond reste le préfixe affiché du fil, à revoir en phase 2 avec le panneau « Agents ». Les résumés (fork, rapport) lisent leur transcript dans l'ordre par ancre.
- La marque du dernier rapport d'un fork se place après le dernier élément affiché d'ID ≤ son dernier message (pour un tour, l'ID de son dernier texte) : un tour couvert en partie et fini après le rapport la tire sous des messages non rapportés, un tour en cours couvert la met en bas.
- Morph : un bloc garde l'id `m<ID de son dernier texte>`. Il ne change qu'à l'arrivée d'un texte (en pratique juste avant la fin : le bloc est alors recréé) ; un tour qui finit sans nouveau texte remonte à sa place sans être dupliqué (idiomorph déplace par id).

**Panneau « Agents »** (phase 2) dans le panneau de détails, une ligne par participant : disponible, répond (à qui, depuis quand), N messages en file, tâche de fond, attend une réponse, attend un worker. Avec « Arrêter » et « tout arrêter » selon les droits.

**Prompt et modèle de session** *[rev. m2]* : `SystemPrompt` et `Model` (`POST /sessions`) ne sont pas enregistrés en base et sont déjà perdus après 30 min de veille. On les **retire** de l'API, faute d'usage : le prompt vient de l'agent.

**Inchangé** : forks (`ForkSessionWorkflow`), rapports au parent, mentions, Telegram (même chemin `Deliver`), mémoire, interface fluide (le SSE reste la sonnette de la session).

## 7. Tâches longues asynchrones (phase ultérieure, point d'entrée fixé ici)

- Un outil long (`analyze_repo`, `implement_feature`) peut être lancé **sans être attendu** : le tour reçoit « tâche lancée », répond, et se termine. Le participant redevient disponible.
- À la fin de la tâche, son résultat est **posté comme un message** (`kind = task_result`, au nom de l'utilisateur dont le tour l'a lancé) et livré par `SignalWithStart` au participant qui l'a lancé. Il l'exploite dans un nouveau tour.
- C'est le même mécanisme que les triggers : « quand ce workflow se termine, poster ce message ».
- La requête `state` du participant liste ses tâches de fond.

## 8. Fiabilité

- **Relivraison** *[rev. I9]*. Un `SignalWithStart` relancé par le serveur, ou par l'activity de relais, peut livrer deux fois le même message, y compris **après** la fin du participant (une nouvelle exécution démarre alors). Une déduplication en mémoire ne suffirait pas : le tour serait rejoué, le LLM payé deux fois et la réponse Telegram envoyée en double. Avant chaque tour, le participant vérifie donc **en base** que le message n'a pas déjà son `turn_end` pour ce participant. La garantie est **« au moins une fois »** : un tour dont le `turn_end` n'a jamais été écrit (§4.6) serait rejoué par une relivraison *[rev2 N12]*.
- **Agent supprimé** avec des messages en file : refusé avant le tour (§4.6).
- **Membre retiré** : ses messages déjà en file sont traités. Ils sont en base et visibles de tous.
- **Ordre entre participants** : il n'est **pas** garanti, et c'est voulu. Une dépendance passe par le relais (même message) ou par un trigger (messages différents).

## 9. Agents nés de la discussion (phase ultérieure)

- Une tâche (une analyse de dépôt) crée une **instance** : un participant `<session>:i:<instance>` (§4.1), avec son état (clone au commit analysé, session Claude Code reprise par `--resume`) sur un worker.
- Elle apparaît dans le panneau « Agents » avec une mention générée (`@analyste-temporal-agent`) et son origine.
- Chaque question est un court run qui reprend sa session sur son worker. Il faut une file propre au worker, puisqu'une session Temporal ne peut pas rester ouverte des jours.
- Durée de vie : « Congédier », expiration après inactivité, renaissance (reclone) si le worker disparaît.
- Les règles d'accès (qui peut l'interroger, selon la clé ou l'abonnement de son créateur) sont celles du cas entre particuliers.

## 9 bis. Idée pour plus tard : cloner un participant

Proposée le 4 octobre 2026. Quand Jarvis est occupé (tâche longue, tour long), un membre peut demander « un autre Jarvis ».
- **Ce que c'est** : une instance (§4.1, `<session>:i:<id>`) dont l'agent de base est Jarvis. Même persona et mêmes outils, mais **sa propre file** ; mention `@jarvis-2`, réponses signées « Jarvis (2) ».
- **Contexte** : il lit la même conversation jusqu'au message auquel il répond, et ignore le tour de Jarvis en cours (§5).
- **Différence avec `/btw`** : c'est un participant complet (outils, file, plusieurs demandes), pas un aparté sans outils.
- **Interface** : quand un message part dans la file d'un participant occupé, le fil propose « [Attendre] [Demander à un autre Jarvis] ». Le second bouton crée le clone et y déplace le message. On peut aussi créer un clone depuis le panneau « Agents ».
- **Points à surveiller** :
  - effets en parallèle : la mémoire est protégée (verrou optimiste), mais l'espace de travail des outils fichiers et `exec` est partagé sur le worker. Il faut au minimum le signaler, et à terme un espace de travail par participant ;
  - durée de vie éphémère : congédié à la main, ou retiré après inactivité ;
  - coût : plus de tours en parallèle, payés par l'auteur de chaque message.
- Le besoin diminue avec la phase 4 (tâches longues asynchrones), mais reste utile pour les tours longs et pour mener deux sujets de front.

## 10. Découpage en phases

| Phase | Contenu | Résultat |
|---|---|---|
| **1** | `ParticipantWorkflow`, nommage et liste des changements (§4.1), clé de tour nommant le participant, relais en activity, nouvelle règle de lecture et ordre par ancre (§5), responsabilités de fin de tour et `turn_end` (§4.6), `Deliver` sans session avec plafond de file, arrêt de tous les participants, terminaison à la suppression, états agrégés par session, déduplication en base ; suppression du `SessionWorkflow` | Un agent occupé ne bloque plus les autres ; mêmes fonctionnalités qu'aujourd'hui |
| **2** | Panneau « Agents », arrêt par participant avec droits | La visibilité demandée |
| **3** | `/btw` | Questions en aparté |
| **4** | Tâches longues asynchrones + triggers (fin de workflow, puis heure : les tâches planifiées deviennent des triggers) | Plus aucun tour bloqué par un run |
| **5** | Agents nés de la discussion, clones | Agents résidents |

Chaque phase est livrable seule. **Le risque de la phase 1 est dans l'ordre des messages (§5) et dans l'état côté serveur (§6), pas dans le nommage** *[rev.]* : ces deux parties auront leurs propres tests purs avant tout branchement.

## 11. Tests (phase 1)

**Purs (`store`, `conversation`)**
- `TurnReads` et `TurnParticipant` sur les nouvelles clés : un autre participant lu entier si terminé avant `upTo`, pas du tout sinon ; ses propres tours lus entiers ; le relais.
- `Order` par ancre : un test aléatoire « ordre par ancre ≡ `Order` actuel » sur des historiques séquentiels (reprendre le prototype de la seconde relecture) ; participants qui se chevauchent (le cas Jarvis 10 / Smith 12), relais, résumé de fork, rapport, résultat planifié, `Tail` ; jamais de réponse avant sa question ; appariement d'outil intact ; jamais de fin sur un assistant d'un autre participant ; identique à l'ordre actuel en séquentiel.

**Workflows (environnement de test)**
- Un participant traite deux messages dans l'ordre, et le second tour voit la réponse au premier.
- Deux participants traitent en parallèle des messages différents, et aucun ne lit l'autre en plein vol.
- Relais : Smith voit la réponse de Jarvis au même message ; le relais s'arrête sur un échec ou un arrêt ; un relais livré deux fois n'est traité qu'une fois.
- `stop-turn` arrête le tour sans vider la boîte ; `clear` vide la boîte.
- Agent supprimé : refus avant le tour, `turn_end` d'erreur, message suivant traité.
- Continue-as-new avec une boîte de 20 messages : aucun message perdu, `GetUnhandledSignalNames` vide.
- `turn_end` sous `:end` après un flush partiel et après un échec dur ; jamais rendu au modèle, au fil ni dans un résumé de fork.
- Relais vers un participant qui a déjà répondu à un message plus récent : son tour sur l'ancien message ne lit pas son tour sur le récent.
- Relais en échec final : `turn_end` d'erreur chez le destinataire, notification, le relayeur continue.
- Agent créé il y a moins de 30 s : accepté (lecture en base).
- `clear` puis relivraison tardive d'un message jeté : pas de tour.
- `turn_end` écrit dans tous les cas (succès, erreur, arrêt, échec avant la première écriture).

**Serveur**
- États agrégés : le `turn_done` d'un participant ne masque pas un autre participant au travail.
- `Delete` et `Leave` terminent tous les participants.
- Plafond de file : 429 au-delà de 5, **sans message enregistré**, relais exempté.
- Relivraison après la fin d'un participant : pas de second tour.

**Contre un vrai serveur Temporal**
- **Fin sans perte** : un signal arrivé pendant la fin du participant est traité par une nouvelle tâche du même workflow (la garantie `UNHANDLED_COMMAND`, que l'environnement de test ne simule pas).
- Démarrage réel du worker (enregistrement des workflows et activités), comme pour les sessions Claude Code.

Les deux sont dans le dépôt : `TestParticipant_NoSignalLostOnExit_RealServer` (`workflow/participant_server_test.go`, sauté sans `TEMPORAL_SMOKE_HOST`) et `scripts/smoke.sh` ; commandes dans `CLAUDE.md`.

## 12. Questions tranchées

Les questions ouvertes de la version 1, avec les réponses retenues :
1. **Sans mention, seul dans la session** : l'agent par défaut de la session (`sessions.agent_id`, via `agentOrDefault`). Pas de nouveau concept.
2. **Messages en file d'un membre retiré** : traités. Ils sont déjà en base et visibles ; seule exception, une session sans plus aucun membre est supprimée, et ses participants terminés (§6).
3. **Plafond de file** : oui, 5 messages en attente par participant, contrôlé par le serveur à la réception (429). Le relais en est exempté.
4. **Tâches planifiées** : inchangées jusqu'à la phase 4.
5. **Événement « en file »** : pas de nouvel événement de workflow. `user_message` est enrichi d'un `queued_behind` par participant ; `turn_started` reste émis par le participant.
