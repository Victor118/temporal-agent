# Conception : les tâches de fond d'un agent

Statut : **version 1**, proposition, rien n'est fait. Le 7 octobre 2026. Phase 4 de `docs/design/participants.md` (« tâches longues asynchrones + triggers »), resserrée sur ce qui manque à l'usage : un agent qui lance un travail long **reste disponible**, et **sa fin le réveille**.

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

- **Éligibles** : les outils workflow (les sous-agents, `analyze_repo`, `implement_feature`). Un outil activity est court (borné à quelques minutes) : jamais en arrière-plan.
- **Le modèle décide** par un champ `background` (booléen) que le catalogue **ajoute au schéma** des outils éligibles, avec sa description : « lance la tâche sans l'attendre ; tu recevras un message à sa fin ; utilise-le pour un travail long quand l'utilisateur peut avoir autre chose à te demander, ou quand il le demande ». Le champ est retiré de l'entrée avant l'appel de l'outil (comme les clés de contexte réservées).
- **Le prompt** (section des comportements) dit quand l'utiliser, et que par défaut on attend.
- `FireAndForget` (le réglage par outil) disparaît : il est remplacé par ce choix par appel.

## 4. Le lancement

Le tour, au lieu d'exécuter l'outil en enfant attendu, lance en enfant **`BackgroundTaskWorkflow`** (`ParentClosePolicy` `ABANDON`, démarrage attendu, comme le correctif actuel) avec :
- l'outil et son entrée, la résolution du catalogue (queue, workflow), le `CallContext` de l'appel (auteur, canal, tour de session pour publier des fichiers, machine du tour pour un sous-agent) ;
- la session, le participant, la clé du tour et l'ID de l'appel qui l'ont lancé ;
- l'ID **`<tour>:bg:<appel>`** (préfixe de session : `query_workflow`, `SessionOf`, le panneau Agents et la suppression de session le trouvent).

Avant de démarrer, le tour enregistre la tâche en base (activity `RegisterTask`, table **`background_tasks`** : ID du workflow, session, participant, agent, utilisateur qui l'a demandée, outil, résumé de l'entrée, tour et appel d'origine, démarrée à, état `running`/`done`/`failed`/`cancelled`, ID du message de fin, consignes de suite (§7)). La base est la source du panneau, du prompt et de la suppression ; Temporal reste celle de l'exécution.

Le résultat de l'outil, pour le tour qui lance : « Tâche lancée en arrière-plan (id …) : tu recevras un message à sa fin. » Un plafond de **3 tâches en cours par participant** : au-delà, l'appel est refusé (erreur d'outil : « attends la fin d'une tâche, ou fais-le sans arrière-plan »).

## 5. La fin

`BackgroundTaskWorkflow` exécute l'outil comme un enfant ordinaire (il l'attend, lui), puis :
1. **`PostTaskResult`** (activity) : écrit dans la session un message **`kind = task_result`**, clé **`task:<ID du workflow>`** (au moins une fois sans doublon), au nom de l'utilisateur qui l'a demandée, `user_id` posé, adressé à l'agent : le nom de l'outil, la durée, le résultat (texte de l'outil tel qu'un tour l'aurait reçu, tronqué comme un résultat d'outil ; les fichiers publiés sont déjà rattachés au tour d'origine et cités), ou l'échec, ou « annulée » ; il marque la tâche finie en base ; si la session n'existe plus, rien n'est écrit et le workflow s'arrête.
2. **`SignalWithStart`** sur `<session>:p:<agent>` (`ParticipantMessage` : l'ID du message, l'auteur = l'utilisateur de la tâche, le canal de la session) ; `CheckTurn` déduplique comme pour un relais. Un `task_result` **n'est pas soumis** à `MaxQueued` : la fin d'une tâche n'est jamais refusée.

Le tour réveillé répond **à l'utilisateur qui avait demandé la tâche** (sa mémoire, ses outils), sur le canal de la session : il rend compte, puis applique ce qu'on lui a demandé en attendant (§7).

**Ce que lit le modèle** (`conversation.Convert`) : un `task_result` est rendu comme un message encadré, rôle user, sans être confondu avec un humain : « [Tâche terminée : analyze_repo (cinesense), lancée par toi à 10:02, 14 min] … ». Les résumés de fork l'étiquettent de même. Son ancre est son propre ID : un tour qui le lit voit tout ce qui précède.

## 6. Ce que l'agent sait pendant qu'elle tourne

- **Le prompt** de chaque tour du participant liste ses tâches en cours (lues dans `background_tasks`) : outil, résumé, depuis quand, pour qui, avec la consigne : « ne refais pas une tâche en cours ; une demande qui dépend d'elle attend sa fin : dis-le, et enregistre la suite (§7) ».
- **La requête `state`** du participant remplit enfin `Background` (aujourd'hui toujours vide) depuis la base ; le **panneau Agents** affiche les tâches sous la ligne de l'agent (outil, pour qui, depuis quand, avancement s'il y en a : note d'un run sur une machine, attente d'un worker).

## 7. « Quand tu auras fini… »

Deux voies, complémentaires :
- **par l'historique** : le message de fin réveille l'agent, qui relit la demande et agit (§2.5) ;
- **par une consigne enregistrée**, plus sûre que la mémoire du modèle : l'outil **`when_task_done(task_id, instruction)`** ajoute la consigne à la tâche (`background_tasks.follow_ups`, une liste, 1 000 caractères chacune, signée de l'utilisateur du tour) ; le message de fin les reprend en tête (« Consignes à appliquer maintenant : … »). Le prompt dit de s'en servir quand l'utilisateur dit « quand tu auras fini… » pendant qu'une tâche tourne.

Ce n'est pas un trigger général (fin d'un workflow quelconque, heure) : c'est la suite d'**une tâche de cet agent**. Les triggers généraux restent un chantier à part.

## 8. Arrêter

- Dans le panneau Agents, « Arrêter » sur une tâche : permis à l'utilisateur qui l'a demandée et au créateur de la session (comme `MayStop`) ; annule `BackgroundTaskWorkflow`, qui annule l'outil, puis poste « tâche annulée par X » (le participant est réveillé : il peut en tenir compte).
- « Arrêter » du fil et « Tout arrêter » d'un agent n'arrêtent **pas** ses tâches de fond (elles ont été lancées pour durer) ; « Tout arrêter » le dit.
- Supprimer la session (ou le départ du dernier membre) termine ses tâches de fond (liste en base), comme ses participants.

## 9. Ce qui change

- **Store** : `background_tasks` ; `kind = task_result` ; `MaxQueued` qui l'exempte.
- **Catalogue** : le champ `background` des outils éligibles, retiré de l'entrée avant l'appel ; `FireAndForget` supprimé.
- **Workflows** : `AgentWorkflow` (lancement en arrière-plan, plafond, résultat « lancée ») ; `BackgroundTaskWorkflow` (exécution, fin, annulation) ; activities `RegisterTask`, `PostTaskResult`, `when_task_done`.
- **Participant** : `state.Background` depuis la base.
- **Conversation** : rendu d'un `task_result` ; prompt : la liste des tâches en cours et les consignes.
- **Serveur** : panneau Agents, « Arrêter » une tâche, suppression de session, rendu du `task_result` dans le fil (une carte « Tâche terminée », avec le lien vers le tour qui l'a lancée).

## 10. Tests

Unitaires : schéma (`background` ajouté et retiré), plafond, rendu du `task_result`, prompt. Workflow (testsuite) : un tour lance un sous-agent en arrière-plan et finit ; la tâche finit ; message posté et participant signalé ; un second message arrivé entre-temps est traité avant ou après selon l'ordre ; consigne `when_task_done` reprise ; annulation ; session supprimée pendant la tâche. `RealServer` (participant) : la fin d'une tâche réveille un participant terminé (`SignalWithStart`), sans perte ni double tour.

## 11. Questions ouvertes

1. Le modèle décide seul (champ `background`), ou aussi l'utilisateur par une commande (« en arrière-plan » / `/bg`) ?
2. Le résultat complet d'une tâche longue (un rapport de 50 Kio) : tronqué dans le message comme un résultat d'outil, ou publié en fichier et cité ?
3. Sur Telegram : le message de fin est-il envoyé tel quel (il réveille l'agent, qui répondra de toute façon) ?
4. Plafond de tâches en cours : 3 par participant, ou par utilisateur ?
