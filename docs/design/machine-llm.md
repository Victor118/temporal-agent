# Conception : le modèle sur la machine de l'utilisateur (phase 3)

Statut : **version 1**, proposition, rien n'est fait. Le 7 octobre 2026. Suite de `docs/design/machines.md` (§10 l'esquissait) : les phases 0 à 2 y sont décrites, avec leurs écarts (§16 et suivants).

## 1. Objet

Faire tourner l'appel au modèle d'un tour (`CallLLM`) sur la machine de l'auteur du message, avec **sa** clé d'API et **son** modèle, par la passerelle des machines. Deux usages :
- **une installation sans clé** : un groupe où chacun apporte son modèle, le serveur n'en paie aucun ;
- **une installation avec clé** où un membre préfère payer ses tours lui-même, ou utiliser son propre modèle.

Hors périmètre : la CLI Claude Code comme moteur (un abonnement seul) ; c'est la phase 4 (`CLIAgentWorkflow`, pont MCP), seule à donner des outils à un agent dans ce cas. Une CLI « sans outils » comme moteur de `CallLLM` n'est pas prévue : un agent sans `web_search`, `ask_user` ni sous-agents perd l'essentiel.

## 2. Ce qui existe

`CallLLM` (`activity/llm.go`) est déjà coupé en deux :
1. `buildRequest` : charge la conversation (`LoadConversation`), l'ordonne et la convertit (`conversation.Order`, `Convert`), reconstruit les définitions d'outils depuis le catalogue, construit le prompt système (identité, comportements, skills, mémoire de l'utilisateur relue, `PartNote`). Il produit une `provider.ChatRequest` **neutre** (messages, outils, système), jusqu'à `LLM_MAX_CONTEXT_BYTES` (2 Mo par défaut).
2. `Provider.Chat` : l'appel au fournisseur, avec la traduction des erreurs (`ContextTooLong`, `PermanentAPIError`, `RetryAfterError` → `NextRetryDelay`).

L'activity a `StartToCloseTimeout` 180 s et 6 essais (`workflow/agent.go`). Elle sert les tours de session, les sous-agents (`agent_<id>`, mode inline) et les tâches planifiées. Deux autres usages du modèle passent par le `provider` du worker : le résumé d'un fork (`SummarizeConversation`) et le rapport au parent (`SummarizeForkReport`).

La phase 3 déplace la **seconde moitié** sur la machine. La première reste sur nos workers : elle lit la base et le catalogue.

## 3. Qui décide : une option par agent

`agents.llm_on_machine`, réglée dans `/admin` :
- **`never`** (défaut) : la clé du serveur, comme aujourd'hui ;
- **`prefer`** : la machine de l'auteur du tour si elle a la capacité `llm`, sinon la clé du serveur ;
- **`require`** : la machine de l'auteur, sinon le tour s'arrête avec un message clair (« connecte ta machine pour parler à cet agent »).

Un serveur **sans clé** (`LLM_API_KEY` vide, permis seulement si `MACHINES_ENABLED`) traite `never` et le repli de `prefer` comme « aucun modèle disponible » : erreur claire, jamais un appel sans clé.

L'option est par agent parce que c'est l'agent qui fixe ce qui part sur la machine (son prompt, ses skills, ses outils, §9) : l'admin décide en connaissance de cause, agent par agent.

**Le modèle.** Celui de la machine prime : c'est elle qui paie. Le `Model` de la requête est ignoré par la machine, qui met le sien ; le fil dit lequel a répondu. Dans une installation d'entreprise, ce n'est pas forcément souhaitable (un modèle imposé, une liste permise) : une politique d'installation ou par agent viendra plus tard, sans changer le mécanisme.

## 4. Le chemin d'un appel

Au début d'un tour dont l'agent n'est pas `never` :
1. **Choix de la machine**, une fois par tour : `PickMachine` (capacité `llm`, auteur du tour `LLMTurnRequest.UserID`), sans directive créée (la réservation se fait appel par appel, §6). Le tour garde l'ID de la machine choisie : toutes ses étapes vont sur la même machine (un même modèle d'un bout à l'autre du tour). Rien trouvé : clé du serveur (`prefer`) ou erreur (`require`).
2. **Chaque étape** de la boucle ReAct appelle, à la place de `CallLLM`, l'activity **`CallLLMOnMachine`** (sur nos workers, n'importe lequel) :
   - crée la directive `llm` pour cette machine (sous verrou, plafond de la machine, comme `PickMachine`) ;
   - construit la requête avec `buildRequest`, le code de `CallLLM`, et applique la garde `LLM_MAX_CONTEXT_BYTES` ;
   - la remet à la passerelle **dans le corps** de `POST /internal/machines/directives`, avec le jeton de tâche ;
   - rend `activity.ErrResultPending`.
3. La passerelle envoie la directive **avec la requête** sur la WebSocket de la machine (§5). La machine appelle son fournisseur et rend la réponse (texte, appels d'outils, raison de fin, tokens, modèle) dans `result`.
4. La passerelle termine l'activity (`CompleteActivity`) avec une `LLMTurnResponse` (la `ChatResponse` et la mémoire du prompt, que `buildRequest` a déterminée), ou l'erreur typée de la machine (§7). Le tour continue comme aujourd'hui : il dispatche les appels d'outils sur nos workers, avec l'allowlist de l'agent.

La requête ne passe **jamais** par Temporal : elle va du worker à la passerelle par l'API interne, puis à la machine par la WebSocket. Seule la réponse (quelques dizaines de Ko au plus, bornée par `max_tokens`) entre dans l'historique, comme la sortie de `CallLLM` aujourd'hui.

## 5. Transport : la requête dans la WebSocket

La règle des machines devient : **la connexion porte les directives, requête LLM comprise, et leurs résultats ; l'API HTTP ne sert qu'à ce que la machine envoie de volumineux (les fichiers)**. Pas de table ni de `GET` pour la requête : un aller-retour de moins à chaque étape (un tour fait souvent 5 à 15 appels au modèle), rien à stocker ni à nettoyer.
- **Taille** : la limite de lecture côté machine passe à `LLM_MAX_CONTEXT_BYTES` plus une marge pour un message `directive` de nature `llm` ; les autres messages gardent leurs limites.
- **Compression** : `permessage-deflate` (`coder/websocket`) des deux côtés ; le JSON d'une conversation se compresse d'un facteur 5 à 10.
- **Pings** : un gros message retarde brièvement les autres sur la connexion ; le délai d'un ping doit couvrir l'envoi d'une requête maximale sur une liaison lente.
- **Redémarrage de la passerelle avant la remise** : la requête, en mémoire seulement, est perdue ; la directive est close en échec et l'activity échoue de façon **retentable** (§7) : l'essai suivant reconstruit la requête et crée une nouvelle directive. C'est voulu : un appel au modèle n'a pas d'effet de bord, et une requête reconstruite relit l'état présent.

## 6. Sur la machine

`agent connect` annonce la capacité **`llm`** quand il a un fournisseur configuré :
- `--llm-provider` (le registre `provider.New` du binaire : `anthropic` aujourd'hui), `--llm-model`, la clé dans **`AGENT_CONNECT_LLM_API_KEY`** : jamais `ANTHROPIC_API_KEY`, que le filtre de Claude Code retire ou garde selon `CLAUDE_CODE_AUTH` ;
- le fournisseur et le modèle sont affichés au démarrage et dans « Mes machines » ;
- un **plafond d'appels simultanés** propre (`--llm-max-concurrent`, défaut 4), séparé du plafond des runs : un tour qui attend son modèle ne doit pas être bloqué par une analyse de 45 min ;
- l'appel passe par le même paquet `provider` que nos workers : mêmes erreurs typées, même gestion du cache de prompt.

La machine ne réessaie pas elle-même : une erreur passagère remonte typée, et c'est la politique de l'activity qui décide (§7). Elle rend aussi les tokens consommés : la visibilité des coûts les lira.

## 7. Échecs et relances

`CallLLMOnMachine` garde la politique de `CallLLM` (6 essais, `PermanentAPIError` et `ContextTooLong` non retentés), avec une **nouvelle directive à chaque essai**. Les erreurs de la machine sont typées et traversent le convertisseur sans être enveloppées :
- `RetryAfter` de son fournisseur → `NextRetryDelay`, comme aujourd'hui ;
- `ContextTooLong` → le message « forke-la » habituel ;
- `PermanentAPIError` (clé refusée, crédit épuisé) → non retenté, et la machine retire sa capacité `llm` jusqu'à ce que sa configuration change (comme le login de Claude Code) ;
- **machine perdue** (`HeartbeatTimeout`, qu'on peut garder court ici : une étape LLM dure quelques dizaines de secondes, 2 min suffisent) ou **refus** (machine pleine, capacité retirée) : en `prefer`, l'étape et la suite du tour passent sur la clé du serveur, et le tour le note ; en `require`, l'essai suivant cherche une autre machine de l'auteur, sinon le tour s'arrête avec un message clair.

Changer de modèle au milieu d'un tour est sans danger : la conversation est neutre (`ChatRequest`), et un appel d'outil d'un modèle se relit par un autre.

## 8. Sous-agents, tâches planifiées, résumés

- **Sous-agent** (`agent_<id>`) : il suit la route de son parent (la même machine), sauf si sa propre option est `never` : il prend alors la clé du serveur, ou échoue clairement si le serveur n'en a pas. Il hérite de l'ID de la machine choisie par le parent, comme il hérite déjà du canal.
- **Tâche planifiée** : elle tourne pour son utilisateur ; elle suit l'option de son agent, sur la machine de cet utilisateur. Machine éteinte : `prefer` passe sur la clé du serveur, `require` échoue (`task_logs`, et l'utilisateur est prévenu).
- **Résumé de fork et rapport au parent (phase 3.1)** : quand le serveur n'a pas de clé, ils passent par la machine de celui qui forke ou qui rapporte, avec la même directive `llm` (la `ChatRequest` du résumé). Avec une clé, rien ne change.

## 9. Ce qui part sur la machine, et ce qu'elle peut faire

La machine qui fait tourner le modèle **a l'autorité de l'agent** (déjà posé au §10 de `machines.md`) :
- **elle reçoit** le prompt de l'agent et ses skills, les définitions de ses outils, la conversation de la session (ce que l'auteur, membre, voit déjà ; les entrées et résultats `PrivateInput` des autres membres restent masqués par `conversation.Convert`), la mémoire de l'auteur seulement ;
- **elle décide** des appels d'outils que le tour exécute, avec toute l'allowlist de l'agent (`send_email`, `exec` sur nos workers, sous-agents…), sans injection de prompt nécessaire.

Dans un groupe qui se fait confiance, c'est acceptable ; c'est la raison de l'option par agent, `never` par défaut. **Traçabilité** : chaque message assistant d'un tour passé par une machine porte `machine_id` et le modèle qui a répondu ; le fil l'affiche (« via la machine de Victor · claude-sonnet »).

## 10. Ce qui change

- **Store** : `agents.llm_on_machine` ; `machine_id` et `model` sur les messages assistant ; capacité `llm` et ses réglages (fournisseur, modèle, plafond) dans l'état de la machine.
- **Workers** : `CallLLMOnMachine` ; le choix de la machine en début de tour et sa propagation aux sous-agents ; `CallLLM` inchangé pour la clé du serveur. Un changement de commandes de l'`AgentWorkflow` : on termine les runs ouverts au déploiement (pas de prod).
- **Passerelle** : directives `llm` avec leur requête (taille, compression), plafond `llm` séparé, résultat converti en `LLMTurnResponse`, erreurs typées.
- **`agent connect`** : la capacité `llm`, sa configuration, l'appel au fournisseur.
- **Serveur** : démarrage sans clé quand les machines sont actives ; l'option dans `/admin` ; l'affichage dans le fil, le panneau Agents et « Mes machines ».
- **Protocole** : une version de plus (sans compatibilité).

## 11. Tests

Contre le vrai serveur (`RealServer` des machines, fournisseur factice sur la machine) : un tour routé sur la machine et ses étapes sur la même machine ; `prefer` sans machine (clé du serveur) ; `require` sans machine (message clair) ; machine perdue en plein tour (`prefer` bascule, `require` cherche une autre machine) ; `ContextTooLong` et `RetryAfter` venus de la machine ; une requête de 1,5 Mo compressée ; un sous-agent qui suit son parent ; un serveur sans clé. Unitaires : routage, propagation aux sous-agents, conversion des erreurs, limite de taille.

## 12. Phases

| Phase | Contenu |
|---|---|
| **3.0** | Option par agent, choix de la machine par tour, `CallLLMOnMachine`, directive `llm` dans la WebSocket (taille, compression), capacité `llm` d'`agent connect`, plafond séparé, erreurs typées et repli, sous-agents, traçabilité et affichage |
| **3.1** | Résumés de fork et rapports par la machine, serveur sans clé, tâches planifiées |

## 13. Questions ouvertes

1. Politique de modèle d'entreprise (modèle imposé ou liste permise) : quand et à quel niveau (installation, agent) ?
2. `HeartbeatTimeout` d'une étape LLM : 2 min suffisent-elles sur une liaison lente avec une requête maximale ?
3. Un membre peut-il refuser que **ses** tours passent par sa machine pour un agent `prefer` (préférence utilisateur), ou est-ce le choix de l'admin seul ?
