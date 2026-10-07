# Conception : les tâches de fond d'un agent

Statut : **version 2.2, faite** (§12, avec ses écarts), sauf les tâches planifiées qui postent dans leur session (§11, chantier suivant). Version 1 le 7 octobre 2026, révisée le même jour après deux relectures contre le code ; les points de la première sont marqués *[rev. 1…13]*, ceux de la seconde *[rev2 1…10]*. Phase 4 de `docs/design/participants.md` (« tâches longues asynchrones + triggers »), resserrée sur ce qui manque à l'usage : un agent qui lance un travail long **reste disponible**, et **sa fin le réveille**.

## 1. Pourquoi

Aujourd'hui, un outil long (un sous-agent `agent_<id>`, `analyze_repo`, `implement_feature`) est **synchrone** : le tour attend, le participant est occupé, les messages suivants attendent en file. C'est voulu et utile : deux messages au même agent s'enchaînent, le second lit le premier (`participants.md` §4, §5).

Ce qui manque, c'est l'autre mode, celui d'un orchestrateur : lancer un travail de 20 minutes, rester joignable pour autre chose, et **être prévenu à la fin** pour en rendre compte ou enchaîner. L'option `FireAndForget` d'un outil (`workflow/agent.go`) en était l'ébauche : aucun outil ne s'en sert, et elle ne prévient personne de la fin (le modèle doit penser à `query_workflow`). Depuis le correctif `027fd90`, au moins l'enfant survit-il au tour (`ParentClosePolicy` `ABANDON`).

## 2. Le principe

1. **Le synchrone reste le défaut.** Rien ne change pour un appel d'outil ordinaire.
2. **L'arrière-plan est un choix explicite** du modèle, pour un appel donné, sur un outil qui le permet (§3).
3. La tâche tourne **hors du tour** ; le tour reçoit « tâche lancée » et continue ; l'agent redevient disponible à la fin de son tour.
4. **À la fin de la tâche, un message est posté dans la session** (le résultat, ou l'échec), adressé à l'agent qui l'a lancée, puis **`SignalWithStart`** sur son participant : exactement ce que font `Deliver` pour un humain et `Relay` pour un relais. Ce message le réveille comme n'importe quel message ; s'il est occupé, il attend en file.
5. Le tour réveillé **lit toute la conversation**, y compris ce qu'on lui a demandé entre-temps (« quand tu auras fini, implémente… ») : c'est l'historique qui porte l'intention, la notification qui donne le moment.

## 3. Quels outils, et qui décide

- **Éligibles** : les outils workflow (les sous-agents, `analyze_repo`, `implement_feature`), **sauf** un outil `PrivateInput` (son résultat irait dans un message que tous les membres lisent) *[rev. 5]*. Un outil activity est court (borné à quelques minutes) : jamais en arrière-plan.
- **Seulement depuis un tour de session** (`TurnKey` non vide) : un sous-agent, une tâche planifiée ou une tâche de fond ne lancent pas de tâche de fond (le champ n'est pas offert, et l'appel est refusé s'il arrive quand même). Pas de tâches de tâches, hors de tout plafond *[rev. 6]*.
- **Le modèle décide** par un champ `background` (booléen) que le catalogue **ajoute au schéma** des outils éligibles, avec sa description : « lance la tâche sans l'attendre ; tu recevras un message à sa fin ; utilise-le pour un travail long quand l'utilisateur peut avoir autre chose à te demander, ou quand il le demande ». Le dispatch le retire de l'entrée avant l'appel de **tout** outil éligible (pas seulement ceux qui ont `NeedsCallContext`, et pour un `agent_<id>` dont le schéma est une constante, `AgentToolSchema`), mais il reste dans le `tool_use` persisté : le modèle relit son choix *[rev. 8]*.
- **Le prompt** (section des comportements) dit quand l'utiliser, et que par défaut on attend. **L'utilisateur l'oriente par ses mots** (« fais-le en arrière-plan », « préviens-moi quand c'est fini ») ; l'admin peut l'écrire dans le prompt d'un agent (« lance toujours les analyses en arrière-plan »). Décidé : le modèle décide, il n'y a pas de commande.
- `FireAndForget` (le réglage par outil) disparaît : il est remplacé par ce choix par appel.

## 4. Le lancement

Le tour, au lieu d'exécuter l'outil en enfant attendu, lance en enfant **`BackgroundTaskWorkflow`** (`ParentClosePolicy` `ABANDON`, démarrage attendu, comme le correctif actuel) avec :
- l'outil et son entrée, la résolution du catalogue (queue, workflow), le `CallContext` de l'appel (auteur, **canal du tour d'origine**, tour de session pour publier des fichiers, machine du tour et machines exclues pour un sous-agent : à la fin, la machine est peut-être partie, `prefer` bascule et `require` en cherche une autre, comme en phase 3) *[rev. 6, 7]* ;
- la session, le participant, la clé du tour et l'ID de l'appel qui l'ont lancé ;
- l'ID **`<tour>:bg:<appel>`** (préfixe de session : `query_workflow`, `SessionOf`, le panneau Agents et la suppression de session le trouvent).

Avant de démarrer, le tour enregistre la tâche en base (activity `RegisterTask`, table **`background_tasks`** : ID du workflow, session, participant, agent, utilisateur qui l'a demandée, outil, résumé de l'entrée, tour et appel d'origine, démarrée à, état `running`/`done`/`failed`/`cancelled`, ID du message de fin, consignes de suite (§7)). La base est la source du panneau, du prompt et de la suppression ; Temporal reste celle de l'exécution.

Le résultat de l'outil, pour le tour qui lance : « Tâche lancée en arrière-plan (id …) : tu recevras un message à sa fin. » Un plafond de **3 tâches en cours par participant**, compté sur la base **et** recoupé par le balayage (§5.4) : au-delà, l'appel est refusé (erreur d'outil : « attends la fin d'une tâche, ou fais-le sans arrière-plan »).

**Ce que la tâche publie pendant qu'elle tourne** *[rev. 3]* : un `notice` (la note d'un run sur une machine, l'attente d'un worker) et une question `ask_user` portent l'**ID de la tâche** ; le serveur les range sous la tâche dans le panneau Agents, jamais sous la ligne du participant (`setNote` ne l'applique qu'à un tour en cours, ou au mauvais). `Statuses` distingue l'attente d'un tour et celle d'une tâche : une question d'une tâche ne met pas le participant « attend une réponse ».

## 5. La fin

`BackgroundTaskWorkflow` exécute l'outil comme un enfant ordinaire (il l'attend, lui), puis :
1. **`PostTaskResult`** (activity) : écrit dans la session un message **`kind = task_result`**, clé **`task:<ID du workflow>`** (au moins une fois sans doublon ; ni `TurnOf` ni `IsScheduledResult` ne la reconnaissent), avec `user_id` de l'utilisateur qui l'a demandée (le tour réveillé lui répond) mais **sans** `author` d'humain : ce n'est pas une parole de membre. Contenu : l'outil, la durée, le résultat, ou l'échec. **Le résultat** : le message en garde **16 Kio au plus** (le début) ; au-delà, le résultat complet est **publié en fichier** (`resultat-<outil>.md`). La borne est plus basse que celle d'un résultat d'outil (96 Kio) parce que ce message est hors tour : chaque appel de chaque participant de la session le relit **entier**, pour toujours, là où le résultat d'un outil dans un tour n'est lu entier que par son agent (les autres le lisent coupé à 1,5 Kio, `conversation.otherResult`) ; `Convert` le coupe de même pour un agent qui n'est pas son destinataire *[rev2 3]*. **Ordre** : le fichier d'abord, puis le message, qui le cite par son nom ; la publication passe par le `tool.Publisher` du worker (tout worker qui sert les workflows en a un), sous `(session, tour d'origine, appel, nom)`, donc idempotente : un second essai de l'activity retrouve le fichier et écrit le même message ; `file_published` part sur la `NotifyQueue` du tour d'origine *[rev2 5]* ; il marque la tâche finie en base.
2. **Puis, dans la même activity**, relecture de la session **juste avant** de signaler (absente : le message déjà écrit reste une ligne orpheline, comme celles d'un tour en cours à la suppression, `participants.md` §6 ; pas de signal, fin) *[rev2 6]* et **`SignalWithStart`** sur `<session>:p:<agent>` par le code de `Relay` (5 essais sur 1 min ; échec final = `turn_end` d'erreur sous la clé `m<task_result>.<agent>`, pour qu'une relivraison ne le rejoue pas) *[rev. 2, 7]*. `ParticipantMessage` : l'ID du message, l'utilisateur de la tâche, **le canal du tour d'origine**, et `SignReply` selon la règle de `Deliver` (signé si l'agent n'est pas celui de la session) *[rev. 1, 7]*. `CheckTurn` déduplique. Un `task_result` **n'est pas soumis** à `MaxQueued`.
3. **Annulée** : le message est posté (« tâche annulée par X ») mais **ne réveille pas** l'agent (comme `fork_reported`) : un tour payé pour accuser réception d'une annulation n'apporte rien ; le prochain tour le lira *[rev. 11]*. `PostTaskResult` tourne alors sur un contexte **déconnecté** (comme `cancelSafeFlush`) : sur le contexte annulé du workflow, l'activity ne serait jamais planifiée *[rev2 8]*.
4. **Balayage** *[rev. 4]* : chaque minute, une requête de visibilité (`WorkflowType = 'BackgroundTaskWorkflow' AND ExecutionStatus != 'Running'`) croisée avec les lignes `running` de plus de 5 min *[rev2 10]* ; une tâche close sans être passée par `PostTaskResult` (terminée par un admin, borne d'exécution, suppression ratée) est marquée `failed` et son message de fin est posté par le balayage (« la tâche s'est arrêtée sans résultat »), avec le réveil. Sans lui, une ligne `running` compterait pour toujours dans le plafond et dans le prompt.
5. **Concurrence entre l'activity et le balayage** *[rev2 4]* : terminer un `BackgroundTaskWorkflow` ne tue pas un `PostTaskResult` déjà en cours. Règle : le message s'insère par `INSERT … ON CONFLICT DO NOTHING` sur sa clé, et **seul celui qui l'a inséré** (l'activity ou le balayage) signale le participant ; l'état ne change que par `UPDATE … WHERE state = 'running'`. Le premier qui écrit gagne, le second ne fait rien : ni doublon de message, ni de tour, ni d'état contradictoire.

Le tour réveillé répond **à l'utilisateur qui avait demandé la tâche** (sa mémoire, ses outils), sur le canal de la session : il rend compte, puis applique ce qu'on lui a demandé en attendant (§7).

**Le contrat du message** *[rev. 1]* : un `task_result` a son cas partout, jamais celui d'un message de membre :
- `conversation.Convert` : encadré, rôle user, « [Task result: analyze_repo (cinesense), started for Victor at 10:02, 14 min] … », jamais `[Victor] …` ;
- le fil (`BuildThread`) : une carte « Tâche terminée », ni message de membre ni tour, avec le lien vers le tour qui l'a lancée et **les fichiers de l'appel** (`files` par session, tour d'origine et appel), que `AttachFiles` montre aujourd'hui sous la réponse du tour d'origine, fini depuis longtemps *[rev. 5]* ;
- les résumés de fork et les rapports (`Reportable`, `Transcribed`) : étiqueté, comme un `fork_report` ;
- les membres (`people`, avatars) : il n'est compté pour personne.

**Ancre et lecture** *[rev. 9]* : hors tour, son ancre est son propre ID. Un tour du même participant en cours quand il est écrit ne le lit pas (ancre plus ancienne) ; le réveil qu'il provoque, si.

## 6. Ce que l'agent sait pendant qu'elle tourne

- **Le prompt** de chaque tour du participant liste ses tâches en cours (lues dans `background_tasks`) : outil, résumé, depuis quand, pour qui, avec la consigne : « ne refais pas une tâche en cours ; une demande qui dépend d'elle attend sa fin : dis-le, et enregistre la suite (§7) ».
- **Le panneau Agents** affiche les tâches sous la ligne de l'agent (outil, pour qui, depuis quand, note et question de la tâche, §4) ; c'est le serveur (`session.Participants`) qui les lit dans `background_tasks`, pas la requête `state` du participant, qui ne lit pas la base *[rev. 10]*.

## 7. « Quand tu auras fini… »

Deux voies, complémentaires :
- **par l'historique** : le message de fin réveille l'agent, qui relit la demande et agit (§2.5) ;
- **par une consigne enregistrée**, plus sûre que la mémoire du modèle : l'outil **`when_task_done(task_id, instruction)`** ajoute la consigne à la tâche (`background_tasks.follow_ups`, une liste, 1 000 caractères chacune, signée de l'utilisateur du tour) ; le message de fin les reprend en tête (« Consignes à appliquer maintenant : … »). Le prompt dit de s'en servir quand l'utilisateur dit « quand tu auras fini… » pendant qu'une tâche tourne.

Ce n'est pas un trigger général (fin d'un workflow quelconque, heure) : c'est la suite d'**une tâche de cet agent**. Les triggers généraux restent un chantier à part.

## 8. Arrêter

- Dans le panneau Agents, « Arrêter » sur une tâche : permis à l'utilisateur qui l'a demandée et au créateur de la session (comme `MayStop`) ; annule `BackgroundTaskWorkflow`, qui annule l'outil, puis poste « tâche annulée par X » sans réveiller le participant (§5.3) *[rev2 1]*.
- « Arrêter » du fil et « Tout arrêter » d'un agent n'arrêtent **pas** ses tâches de fond (elles ont été lancées pour durer) ; « Tout arrêter » le dit, et pour tout couper, le créateur arrête chaque tâche *[rev. 11]*.
- **Supprimer la session, ou le départ du dernier membre**, termine ses tâches de fond **avant** de supprimer la session, comme ses participants : `BackgroundTaskWorkflow` listés par la visibilité (type et préfixe de session) et par la base, terminés avec leurs enfants (un `CodingRunWorkflow` libère sa machine) *[rev. 2]*. Reste une course, la même que pour un participant à peine démarré, acceptée dans `participants.md` §6 : un `PostTaskResult` en vol qui relit la session encore présente et signale juste avant la suppression démarre un participant, dont le tour suivant trouve la session absente *[rev2 7]*.

## 9. Ce qui change

- **Store** : `background_tasks` ; `kind = task_result`, et son cas dans `Convert`, `BuildThread`, `Reportable`/`Transcribed`, le compte des membres ; `MaxQueued` qui l'exempte.
- **Catalogue** : le champ `background` des outils éligibles, retiré de l'entrée avant l'appel ; `FireAndForget` supprimé.
- **Workflows** : `AgentWorkflow` (lancement en arrière-plan, plafond, résultat « lancée ») ; `BackgroundTaskWorkflow` (exécution, fin, annulation) ; activities `RegisterTask`, `PostTaskResult`, `when_task_done`.
- **Serveur** : `session.Participants` lit `background_tasks` pour le panneau (la requête `state` du participant ne lit pas la base) *[rev2 2]*.
- **Conversation** : rendu d'un `task_result` ; prompt : la liste des tâches en cours et les consignes.
- **Serveur** : panneau Agents (tâches, leurs notes et questions), « Arrêter » une tâche, suppression de session et départ du dernier membre, balayage des tâches, rendu du `task_result` dans le fil.
- **Telegram** *[rev. 12]* : un `task_result` n'est pas envoyé (comme un `fork_report`) ; la réponse du tour réveillé part sur le canal du tour d'origine.

## 10. Tests

Unitaires : schéma (`background` ajouté et retiré), plafond, rendu du `task_result`, prompt. Workflow (testsuite) : un tour lance un sous-agent en arrière-plan et finit ; la tâche finit ; message posté et participant signalé ; un second message arrivé entre-temps est traité avant ou après selon l'ordre ; consigne `when_task_done` reprise ; annulation ; session supprimée pendant la tâche. `RealServer` (participant) : la fin d'une tâche réveille un participant terminé (`SignalWithStart`), sans perte ni double tour.

## 11. Questions ouvertes

Tranchées le 7 octobre 2026 : le modèle décide, l'utilisateur l'oriente par ses mots (§3) ; un résultat au-delà de 96 Kio est publié en fichier (§5.1) ; 3 tâches en cours par agent (par participant, §4).

**Les tâches planifiées** *[rev. 13]* : décidé, une tâche planifiée **poste son résultat dans la session qui l'a créée** (un `task_result`, même contrat), au lieu de ne l'envoyer qu'au canal : la session garde la trace de ce qui a tourné pour elle. Elle **ne réveille pas** l'agent (le travail est fait). Il faut pour cela que la tâche planifiée connaisse sa session d'origine (`task_logs`, posée à la création par `schedule_task` depuis le `CallContext`) ; une session supprimée entre-temps, **ou que son utilisateur a quittée**, : résultat envoyé au canal seulement, comme aujourd'hui (elle ne poste pas pour les autres membres d'une session qui n'est plus la sienne). La clé est celle qui existe déjà, `sched:<id>:<run>` (`store.ScheduledResultKey`, que le fil sait citer), avec `kind = task_result` *[rev2 9]*. Ce chantier suit les tâches de fond (même message, même rendu).

Reste ouvert : faut-il un plafond par utilisateur en plus de celui par agent, quand plusieurs agents d'une session lancent des tâches pour la même personne ?

## 12. Ce qui est fait, et les écarts

Fait le 7 octobre 2026, branche `feat/background-tasks`. Tout ce que décrivent les §2 à §9, sauf les tâches planifiées (§11).

**Où vit quoi.**
- Store (`store/background_task.go`) : `background_tasks` (clé étrangère vers `sessions`, `ON DELETE CASCADE` : la session supprimée emporte ses lignes), `kind = task_result` porté par `Message.Task` (`TaskRef` : outil, résumé, tour et appel d'origine, état, demandeur, début et fin, annulée par, consignes, fichier), clé `task:<ID>`. `RegisterTask` compte et insère sous un verrou consultatif par participant ; `EndTask` écrit la fin en une transaction qui tient la ligne de la tâche (`FOR UPDATE`).
- Catalogue (`activity/background.go`) : `Backgroundable` (outil workflow, pas `PrivateInput`, et dont le schéma n'a pas déjà un champ `background`), `WithBackgroundField` (ajouté par `ToolDefinitions(noms, background)`, `AgentToolSchema` compris, octets stables), `TakeBackground` (retiré au dispatch de tout outil éligible, gardé dans le `tool_use`). `ToolResolution.Background` dit l'éligibilité au workflow. `FireAndForget` supprimé (champ, colonne, test).
- Workflows (`workflow/background.go`) : le tour enregistre (`RegisterTask`), lance `BackgroundTaskWorkflow` en enfant `ABANDON`, démarrage attendu (échec : ligne supprimée par `DropTask`), envoie `task_started` ; la tâche exécute l'outil (`<tâche>:tool:<outil>:<appel>`, le `CallContext` du tour, machine et exclusions héritées par un sous-agent), poste (`PostTaskResult`), publie `task_result` (et `file_published`) sur le web, puis réveille.
- Serveur (`session/tasks.go`) : panneau, `StopTask`, `terminateTasks`, `SweepTasks` ; interface : carte « Tâche terminée / échouée / annulée par X » (`ItemTask`), lignes de tâches sous l'agent avec « Arrêter la tâche » ; JSON `GET /sessions/{id}/tasks`, `POST /sessions/{id}/tasks/stop` (`{"task"}` ; 202, 403, 404, 409).
- `when_task_done` (`tool/tasks.go`) : outil activity publié comme les autres (donc soumis à l'allowlist), consignes de 1 000 caractères au plus, 10 par tâche, signées du nom de l'utilisateur du tour ; refusé à un sous-agent et hors d'un tour de session.

**Écarts et précisions.**
1. *§5.1-5.2, une activity ou deux.* La fin est en deux activities : `PostTaskResult` (fichier, message, état ; politique du store, 12 essais) puis le réveil par l'activity `Relay`, à laquelle `RelayInput.SessionID` fait relire la session juste avant le `SignalWithStart` (politique du relais, 5 essais sur 1 min). La conception mettait les deux dans une seule activity ; deux, chacune garde la politique de relance de son étape (une base absente 2 min ne doit pas compter comme un relais échoué), et la relecture de la session reste immédiatement avant le signal. Échec final du réveil : `turn_end` d'erreur sous `m<task_result>.<agent>` et notification d'erreur sur le canal d'origine, comme un relais.
2. *§5.5, « seul celui qui l'a inséré signale ».* L'insertion du message et le changement d'état sont dans la même transaction, qui tient la ligne : le premier écrivain gagne, le second ne fait rien. Mais un essai rejoué de l'activity (la base a commité, la réponse s'est perdue) trouverait le message déjà inséré et ne réveillerait personne ; la ligne garde donc qui l'a finie (`ended_by` : `task` ou `sweep`), et un nouvel essai du même écrivain réveille à nouveau (`CheckTurn` déduplique). Une tâche disparue (session supprimée) n'écrit rien du tout, ni message orphelin.
3. *§5.4, le balayage.* Dans le serveur (une réplique, `RunTaskSweep`, chaque minute) : les lignes `running` de plus de 5 min, croisées avec les `BackgroundTaskWorkflow` que la visibilité voit **en cours** (requête bornée, au lieu de lister tous les clos), et pour celles qu'elle ne voit pas, un `Describe` qui confirme (clos ou inconnu de Temporal ; s'il ne répond pas, rien). Son message : « The task stopped without a result… », état `failed`, avec le réveil. Limites : un serveur qui s'arrête entre l'écriture et le signal perd ce réveil ; un réveil du balayage qui échoue écrit le `turn_end` d'erreur mais ne prévient pas le canal (le serveur n'a pas de notifier Telegram).
4. *Taille du résultat.* La tâche passe à `PostTaskResult` au plus 1 Mio (début et fin gardés, comme un résultat d'outil) : c'est le fichier publié au-delà de 16 Kio. Le message garde le début (16 Kio, sur une frontière de rune).
5. *Fichiers d'un sous-agent.* Les fichiers sont rangés par (tour, appel) ; un sous-agent publiait sous ses propres appels, introuvables depuis l'appel qui l'a lancé. Ses appels portent maintenant l'appel parent en préfixe (`AgentWorkflowInput.CallPrefix`, `<appel parent>/<appel>`), pour tout sous-agent, en arrière-plan ou non : la carte d'une tâche montre les fichiers de son appel et de ses sous-agents, que le fil ne montre plus sous le tour d'origine.
6. *Le prompt.* La section « Background tasks » (après la mémoire, avant la note multi-agents) dit quand lancer une tâche si un outil l'offre, et liste les tâches en cours du participant, **sauf celles du tour même** : leur résultat le dit au modèle, et le prompt, préfixe en cache, ne change pas sous le tour.
7. *§4, questions et notes d'une tâche.* `Statuses` garde la session « attend une réponse » pour une question d'une tâche (quelqu'un doit répondre) ; c'est le participant qui ne l'est pas (`visible.taskAsking`, à part de `asking`), et le panneau la range sous la tâche. Conséquence connue : la note du rapport partiel d'un fork (« l'agent est en plein tour ») s'affiche aussi pour la seule question d'une tâche. Les notes d'une tâche (`notice` avec `task`, depuis `sendRunNotice` et la passerelle, `store.TaskOfWorkflow` sur l'ID du run) sont gardées à part (`turns.taskNotes`), effacées à sa fin.
8. *La requête `state`* ne porte plus `background` : elle ne lit pas la base, et les tâches survivent au participant.
9. *Éligibilité.* `ask_user` est un outil workflow sans `PrivateInput` : il reçoit le champ, comme le dit le §3 ; le modèle n'a pas de raison de s'en servir.
10. *Événement de plus.* `task_started` (panneau Agents) ; `task_result` est un événement d'état (arbres, fil, panneau). Ni l'un ni l'autre ne part sur Telegram.

**Tests.** Unitaires : schéma ajouté et retiré, éligibilité, `TakeBackground`, plafond (store, concurrent), rendu du `task_result` (le sien entier avec consignes et fichier, coupé pour un autre agent), transcript, prompt, premier écrivant gagne (store et activity), `when_task_done`, carte du fil et fichiers, panneau, routes. Testsuite : un tour lance un sous-agent en arrière-plan et finit ; plafond ; appel attendu sans le champ ; refus hors tour de session ; la tâche finit, poste et réveille ; échec ; résultat publié en fichier ; annulation sans réveil ; session supprimée pendant la tâche ; réveil en échec ; un participant répond à un `task_result` avant ou après un message arrivé entre-temps. Serveur : arrêt (droits), suppression et départ du dernier membre (tâches terminées avant la suppression), panneau, balayage. Contre un vrai serveur : `TestBackgroundTask_WakesTheParticipant_RealServer` (la fin d'une tâche réveille un participant terminé, ou occupé par le message suivant, sans perte ni double tour).

