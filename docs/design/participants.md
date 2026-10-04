# Conception : des participants à la place du workflow de session

Statut : **proposition**, à relire avant tout code. Rédigé le 4 octobre 2026.

## 1. Pourquoi

Aujourd'hui, chaque session a un `SessionWorkflow` (`session-<id>`) qui traite ses messages **un par un** : il reçoit un signal, fait répondre le ou les agents (un `AgentWorkflow` enfant par agent, l'un après l'autre), attend la fin, puis passe au message suivant. Il se met en veille après 30 min sans message et se remet à neuf (*continue-as-new*) quand son historique grossit.

Ce modèle pose quatre problèmes :

1. **Un agent occupé bloque toute la session.** Pendant qu'un tour tourne, y compris un run Claude Code de 20 min qui est un enfant du tour, aucun autre message n'est traité, même adressé à un autre agent.
2. **On ne voit pas qui travaille.** L'état « l'agent travaille » est celui de la session, pas d'un agent.
3. **La conversation est modélisée comme un processus.** Or c'est un ensemble de données (en base) sur lesquelles s'exécutent des traitements (les tours). Toute la mécanique de vie d'un workflow long (veille, reprise, `SignalWithStart` sur un identifiant fixe, remise à neuf, compatibilité au rejeu) n'existe qu'à cause de ce choix.
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

## 3. Le modèle

| Notion | Ce que c'est | Où ça vit |
|---|---|---|
| **Session** | Messages, membres, forks, mode de l'agent par défaut | Postgres uniquement, plus aucun workflow |
| **Participant** | Un agent dans une session : un agent *défini* (Jarvis) ou une *instance* née d'une tâche (§9) | Un workflow `ParticipantWorkflow`, **seulement pendant qu'il a du travail** |
| **Tour** | La réponse d'un participant à un message : boucle LLM + outils | `AgentWorkflow` enfant, court, comme aujourd'hui |
| **Message** | Ce qu'écrit un membre, ou ce que poste un trigger ou une tâche | Postgres, puis un signal à chaque participant concerné |

### Qui répond à un message

Les règles actuelles ne changent pas (`session.Service.Deliver`) : mentions résolues côté serveur (au plus 3), `agent_mode` (`auto`, `always`, `mention`) quand il n'y a pas de mention. Elles donnent une **liste ordonnée** de participants à faire répondre.

## 4. Les workflows

### 4.1 Nommage

Les identifiants de workflow aujourd'hui commencent par `<session>-` (`<session>-turn-N`, `<session>-tool-…`, `<session>-<…ask_user…>`). Un identifiant `<session>-<agent>` pourrait entrer en collision avec un agent nommé `turn` ou `tool`. On réserve donc un séparateur que les identifiants d'agents ne peuvent pas contenir (ils suivent `[a-z0-9-]`) :

| Workflow | Identifiant |
|---|---|
| Participant | `<session>:p:<agent>` |
| Tour d'un participant | `<session>:p:<agent>:m<message id>` |
| Aparté (`/btw`) | `<session>:p:<agent>:btw` |
| Outils lancés par un tour, questions `ask_user` | préfixés par l'identifiant du tour, au lieu de la session |

Toutes les requêtes « dans cette session » deviennent `WorkflowId STARTS_WITH '<session>:'`.

**Unicité.** Le nommage repose sur des identifiants uniques et stables :
- **Agent défini** : son `agent_id`, clé primaire de la table `agents`, immuable, de forme `[a-z0-9-]`, donc jamais de `:`. On utilise l'identifiant, **pas la mention** : la mention (`@jarvis`) peut changer dans `/admin` sans toucher aux identifiants de workflow. Elle est déjà unique sans tenir compte de la casse, et ne peut pas égaler l'identifiant d'un autre agent.
- **Instance née d'une tâche** (§9) : un espace de noms à part, `<session>:i:<instance>`, pour qu'une instance ne puisse jamais prendre l'identifiant d'un agent défini créé plus tard. L'identifiant est généré, unique par construction (l'ID de sa ligne en base), jamais dérivé du nom du dépôt. Sa mention lisible (`@analyste-temporal-agent`) est unique dans la session et ne peut pas entrer en collision avec celle d'un agent défini ; en cas de doublon, on ajoute un suffixe `-2`.
- **Agent supprimé puis recréé avec le même identifiant** : Temporal permet de réutiliser un identifiant une fois le workflow précédent terminé. Les messages encore en file d'un agent supprimé échouent proprement (§8).

### 4.2 `ParticipantWorkflow`

**Démarrage et livraison.** Le serveur ne démarre jamais un participant directement. Pour chaque message, il fait un `SignalWithStart` sur `<session>:p:<agent>` (politique de conflit `USE_EXISTING`), avec le signal `message` :
- si le participant ne tourne pas, Temporal le démarre et lui livre le message ;
- s'il tourne, le message rejoint sa boîte de réception (le canal de signaux, durable dans son historique).

Le serveur reste sans état : un seul appel, toujours le même.

**Boucle.**

```
démarrage(entrée : session, agent, canal…)
boucle :
    si la boîte de réception est vide (ReceiveAsync) : terminer
    msg := message suivant
    si msg.ID déjà traité : ignorer          (relivraison, §8)
    tour := AgentWorkflow enfant <session>:p:<agent>:m<msg.ID>
    attendre la fin du tour, ou un signal stop-turn qui l'annule
    enregistrer le tour (déjà fait par le tour, voir 4.3)
    si msg.Next n'est pas vide : relayer (4.4)
    si l'historique devient gros et la boîte est vide : continue-as-new
```

**Fin sans perte.** Quand la boîte est vide, le workflow se termine. Si un signal arrive pendant cette décision, Temporal refuse la fin et renvoie une tâche avec ce signal. Ce comportement est déjà vérifié dans le SDK v1.33 pour la session actuelle : il suffit de tester la boîte juste avant de retourner.

**Signaux.**

| Signal | Effet |
|---|---|
| `message` | Ajoute un message à la boîte |
| `stop-turn` | Annule le tour en cours, pas les messages en attente |
| `clear` | Annule le tour en cours et vide la boîte (bouton « tout arrêter ») |

**Requête `state`.** Le message en cours (ID, auteur, depuis quand), le nombre de messages en attente, l'éventuelle tâche de fond. C'est la source du panneau « Agents ».

**Remise à neuf.** Un participant ne vit que le temps de vider sa boîte, donc son historique reste court. La garde de taille actuelle est conservée, mais elle n'est appliquée que boîte vide, pour ne perdre aucun signal.

### 4.3 `AgentWorkflow` (le tour)

Il reste presque tel quel. Ce qui change :
- **Il émet lui-même** `turn_started` et `turn_done` (aujourd'hui c'est la session), avec les mêmes options courtes.
- **Il écrit lui-même son `turn_error`** après son transcript, en plus des écritures au fil de l'eau qu'il fait déjà. La réécriture de fin de tour par la session disparaît.
- **Ses identifiants dérivés** (outils, `ask_user`) partent de son propre identifiant de tour.
- **Sa clé de tour** (`TurnKey`) est dérivée du message, pas d'un compteur de la session : `m<id>@<id>.<rang>`, où `<rang>` est la position du participant dans la liste du message. Elle est donc stable d'un participant à l'autre pendant un relais.

### 4.4 Un message adressé à plusieurs participants : le relais

« @jarvis résume, @smith à partir de là… » : la dépendance est écrite dans le message, donc Smith doit voir la réponse de Jarvis à **ce** message.
- Le serveur ne sonne que chez le **premier** participant, avec `Next = [smith]` et `EarlierTurns = []`.
- Quand le tour de Jarvis se termine **sans échec ni arrêt**, Jarvis fait un `SignalWithStart` chez `<session>:p:smith` avec le même message, `Next = []`, et `EarlierTurns = [clé du tour de Jarvis]`.
- Si le tour de Jarvis échoue ou est arrêté, le relais s'arrête, comme aujourd'hui.
- Pendant ce temps, Smith peut traiter d'autres messages : le relais n'est qu'un message de plus dans sa boîte.

C'est un trigger implicite (« quand Jarvis aura fini ce message, à Smith »). Il ne demande pas d'attendre un workflow qui n'est pas un enfant.

### 4.5 La voie express `/btw`

- Un message `/btw …`, ou le bouton « en aparté » à côté de « Jarvis travaille… », est livré par `SignalWithStart` sur `<session>:p:<agent>:btw`. Un seul aparté à la fois par participant, garanti par l'identifiant.
- C'est le même `ParticipantWorkflow`, avec une option `aside` : le tour est lancé **sans outils**, ou avec des outils en lecture seule, pour ne jamais entrer en conflit avec les effets du tour principal.
- L'échange est enregistré dans le fil avec `kind = aside`, affiché « en aparté ». Le tour principal ne le voit pas pendant qu'il tourne ; les tours suivants, si.

## 5. La règle de lecture de l'historique

C'est le point le plus délicat. `store.TurnReads` dit aujourd'hui qu'un tour lit :
1. la session jusqu'au message auquel il répond ;
2. ce qu'a écrit son propre groupe (les tours du même message) ;
3. **ce qu'ont écrit les tours des messages précédents, même après lui**, au motif que « they ran before it, as a session answers its messages one at a time ».

La règle 3 suppose le traitement séquentiel, qui disparaît. Avec des participants en parallèle, le tour de Smith (message 12) lirait en plein vol le tour de Jarvis (message 10) qui tourne encore : le début de sa conversation changerait d'un appel LLM à l'autre (le cache serait manqué, et des messages apparaîtraient avant ses propres appels d'outils).

**Nouvelle règle.** Un tour du participant P, pour le message M (`upTo = id(M)`), lit :
1. tout message d'ID ≤ `upTo` ;
2. son propre groupe (le relais : `EarlierTurns` + lui-même) ;
3. les tours **de P** pour les messages précédents, même écrits après `upTo` : P les a traités avant M, c'est son ordre à lui ;
4. et **pas** les tours des **autres** participants écrits après `upTo` : ils seront lus par les tours suivants.

La règle 3 de P reste vraie par construction : un participant traite ses messages un par un. Il faut que la clé de tour permette de retrouver son participant (`TurnOf` → participant), ce que le nouveau format (§4.3) doit inclure.

L'ordre d'affichage pour le modèle (`conversation.Order`, qui garde chaque tour groupé) ne change pas.

## 6. Côté serveur

**`Deliver`** :
1. enregistre le message (inchangé) ;
2. résout les participants (inchangé) ;
3. fait un `SignalWithStart` chez le premier, avec la liste des suivants (§4.4).

La méthode `signalSession`, le repli sur les anciens identifiants et le démarrage de session à l'ouverture disparaissent.

**Arrêter.** Le bouton « Arrêter » d'un participant envoie `stop-turn` à `<session>:p:<agent>`. Qui peut le faire : l'auteur du message en cours, ou le créateur de la session (décision déjà prise en discussion).

**Qui travaille.**
- `WorkflowId STARTS_WITH '<session>:p:'` + `ExecutionStatus = 'Running'` donne les participants actifs. La requête `state` de chacun donne le détail.
- L'état des tours en mémoire (`session/turns.go`, alimenté par `turn_started` / `turn_done`) reste le chemin rapide pour l'interface. Il est indexé par participant au lieu de par session.
- Les questions en attente (`ask_user`) : la même requête qu'aujourd'hui, avec le nouveau préfixe.

**Panneau « Agents »** dans le panneau de détails, une ligne par participant : disponible, répond (à qui, depuis quand), N messages en file, tâche de fond, attend une réponse, attend un worker. Avec « Arrêter » et « tout arrêter » selon les droits.

**Inchangé** : forks (`ForkSessionWorkflow`), rapports au parent, mentions, Telegram (même chemin `Deliver`), mémoire, interface fluide (le SSE reste la sonnette de la session).

## 7. Tâches longues asynchrones (phase ultérieure, point d'entrée fixé ici)

- Un outil long (`analyze_repo`, `implement_feature`) peut être lancé **sans être attendu** : le tour reçoit « tâche lancée », répond, et se termine. Le participant redevient disponible.
- À la fin de la tâche, son résultat est **posté comme un message** (`kind = task_result`, au nom de l'utilisateur dont le tour l'a lancé) et livré par `SignalWithStart` au participant qui l'a lancé. Il l'exploite dans un nouveau tour.
- C'est le même mécanisme que les triggers : « quand ce workflow se termine, poster ce message ».
- La requête `state` du participant liste ses tâches de fond.

## 8. Fiabilité

- **Relivraison.** Si le serveur relance un `SignalWithStart` après une erreur réseau, le même message peut arriver deux fois. Le participant garde les ID de messages déjà traités dans son état. Après une remise à neuf, il garde les derniers (une centaine) ; l'enregistrement en base est déjà idempotent par clé de tour.
- **Agent supprimé** avec des messages en file : le tour échoue avec une erreur claire (`turn_error`), et le participant passe au message suivant.
- **Membre retiré** : ses messages déjà en file sont traités, comme un message déjà envoyé. À trancher (question ouverte 2).
- **Ordre entre participants** : il n'est **pas** garanti, et c'est voulu. Une dépendance passe par le relais (même message) ou par un trigger (messages différents).

## 9. Agents nés de la discussion (phase ultérieure)

- Une tâche (une analyse de dépôt) crée une **instance** : un participant `<session>:i:<instance>` (§4.1), avec son état (clone au commit analysé, session Claude Code reprise par `--resume`) sur un worker.
- Elle apparaît dans le panneau « Agents » avec une mention générée (`@analyste-temporal-agent`) et son origine.
- Chaque question est un court run qui reprend sa session sur son worker. Il faut une file propre au worker, puisqu'une session Temporal ne peut pas rester ouverte des jours.
- Durée de vie : « Congédier », expiration après inactivité, renaissance (reclone) si le worker disparaît.
- Les règles d'accès (qui peut l'interroger, selon la clé ou l'abonnement de son créateur) sont celles du cas entre particuliers.

## 10. Découpage en phases

| Phase | Contenu | Résultat |
|---|---|---|
| **1** | `ParticipantWorkflow`, nouveau nommage, relais, nouvelle règle de lecture, `Deliver` sans session, tours qui émettent leurs événements et s'enregistrent ; suppression du `SessionWorkflow` | Un agent occupé ne bloque plus les autres ; mêmes fonctionnalités qu'aujourd'hui |
| **2** | Panneau « Agents », arrêt par participant avec droits | La visibilité demandée |
| **3** | `/btw` | Questions en aparté |
| **4** | Tâches longues asynchrones + triggers (fin de workflow, puis heure : les tâches planifiées deviennent des triggers) | Plus aucun tour bloqué par un run |
| **5** | Agents nés de la discussion | Agents résidents |

Chaque phase est livrable seule. La phase 1 est la plus risquée (règle de lecture, nommage) : elle passera par la boucle habituelle de revue.

## 11. Tests (phase 1)

- Un participant traite deux messages dans l'ordre, et le second tour voit la réponse au premier.
- Deux participants traitent en parallèle des messages différents, et aucun ne lit l'autre en plein vol (nouvelle règle de lecture).
- Relais : Smith voit la réponse de Jarvis au même message ; le relais s'arrête sur un échec ou un arrêt.
- Fin sans perte : un signal arrivé pendant la fin est traité par le même workflow.
- Relivraison : un message livré deux fois n'est traité qu'une fois.
- `stop-turn` arrête le tour sans vider la boîte ; `clear` vide la boîte.
- Règle de lecture : tests purs de `TurnReads` et `Order` sur les nouveaux cas, avec des clés réelles.
- Rejeu non requis (pas de prod), mais un test de démarrage réel du worker (enregistrement des workflows et activités), comme pour les sessions Claude Code.

## 12. Questions ouvertes

1. **`agent_mode = auto` à plusieurs** : sans mention, rien ne répond (inchangé). Mais un message sans mention dans une session à un seul membre va à l'agent par défaut. Faut-il un « agent par défaut » par session, ou garder celui de la session ?
2. **Messages en file d'un membre retiré** : les traiter, ou les abandonner ?
3. **Limite de file par participant** : faut-il plafonner le nombre de messages en attente (coût) ?
4. **Les tâches planifiées actuelles** : les garder telles quelles jusqu'à la phase 4, ou les faire passer tout de suite par un participant ?
5. **`turn_started` émis par le tour** : on perd la notification « en file » (message reçu mais pas encore pris). Faut-il un événement `queued` émis à la livraison ?
