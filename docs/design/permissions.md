# Conception : qui peut se servir de quels outils et agents

Statut : **version 1.1, à valider**, 10 octobre 2026. Rien n'est codé. Version 1 relue contre le code le même jour : les points de la relecture sont marqués *[rev. B1…, I1…, M1…]*.

## 1. Pourquoi

Aujourd'hui, ce qu'un tour peut faire dépend de son **agent** seul : sa liste d'outils (`agents.tools`, globs, refus par défaut, `Catalog.AllowedTools`). Tout membre de l'installation peut parler à tout agent, et donc se servir de tous ses outils. Pour un petit groupe qui fait confiance à l'admin, c'est suffisant tant que les outils n'engagent que l'installation ; ça ne l'est plus quand un outil ouvre une ressource qui n'est pas pour tout le monde : un serveur MCP branché sur le CRM de l'équipe commerciale, une base, un compte GitHub, `send_email` avec l'adresse de l'installation, `exec` sur le worker partagé.

Restreindre un **agent** ne suffit pas :
- **délégation** : un agent ouvert à tous qui a `agent_*` appelle l'agent restreint (`agent_commercial`), et l'utilisateur y accède par lui ;
- **liste large** : un agent à `*` reçoit `crm_*` dès que le serveur est branché ;
- deux agents peuvent avoir les mêmes outils.

L'unité qui compte est donc l'**outil** ; un agent n'est qu'un outil de plus (`agent_<id>`), et un serveur MCP un groupe d'outils.

## 2. Le principe

1. **Ouvert par défaut.** Rien ne change tant que l'admin n'écrit aucune règle. L'installation actuelle reste telle quelle.
2. **Une règle ferme ce qu'elle vise** et dit qui garde l'accès : « serveur MCP `crm` → groupe Commercial », « `agent_commercial` → groupe Commercial », « `exec` → Victor, Alice ».
3. **Qui est contrôlé : l'utilisateur du tour** (`AgentWorkflowInput.UserID`, celui que le tour sert, dont la mémoire est chargée et pour qui les outils agissent). Vérifié dans le code : il n'est jamais fourni par le modèle (clés réservées du `CallContext`, `tool/metadata.go`) et il est hérité partout :
   - tour de session : l'auteur authentifié du message ; un relais : le même message, donc le même `UserID` ;
   - sous-agent : l'utilisateur de son parent ;
   - tâche de fond : son demandeur, relu en base pour le tour qui la réveille ;
   - tâche planifiée : son propriétaire, **à chaque exécution** : un droit retiré s'applique au run suivant.
4. **Outils d'un tour = liste de l'agent ∩ droits de l'utilisateur.** Calculé au début du tour, là où il l'est déjà (`ListTools`, une activité : lire le catalogue y est déterministe). Le modèle ne voit que ceux-là, et le dispatch refuse les autres (`Resolutions` : absent = refusé, déjà le cas).
5. **Un agent qu'on n'a pas le droit d'utiliser ne répond pas** : c'est la règle sur `agent_<id>`, la même pour le `@`, l'agent d'une session, un fork et la délégation.
6. **L'admin a tout** (rôle `admin`, compte actif). **Un utilisateur désactivé n'a rien** *[rev. I4]* : aucun agent ne lui répond, admin ou non ; c'est ce qui arrête aussi ses tâches planifiées et ses tâches de fond réveillées (aujourd'hui `disabled_at` ne bloque que le login et Telegram).

## 3. Les règles

### 3.1 Cible

Une règle vise :
- **un serveur MCP** (`target_kind = mcp_server`, `target = crm`) : tous ses outils, y compris ceux qu'il ajoutera plus tard ;
- **ou un motif de nom** (`target_kind = pattern`, `target = agent_commercial`, `exec`, `crm_update_*`), mêmes globs que les listes d'outils des agents (`tool.MatchAny`).

Pourquoi une cible « serveur » à part : le préfixe `<serveur>_` n'est qu'une convention de nommage (`exposedName`). Un serveur nommé `web` donnerait `web_…`, et une règle `web_*` attraperait aussi `web_fetch` et `web_search`, outils intégrés. Il faut donc savoir **de quel serveur vient un outil**, ce que la table `tools` ne dit pas aujourd'hui (seul `kind = mcp`) :
- `tool.Tool` gagne `MCPServer string`, posé par `MCPClient.Discover` (`c.config.Name`) ;
- `tools` gagne la colonne `mcp_server` (vide pour un outil intégré), publiée avec le reste (`store.ToolRecord.MCPServer` ; `UpsertTool` est inconditionnel : remplie au premier redémarrage des workers).

Dans `/admin`, le serveur se choisit dans la liste des serveurs publiés (`SELECT DISTINCT mcp_server`) ; un motif se saisit.

**Une règle qui ne vise plus rien** *[rev. I2]* : le nom du serveur est celui de `worker.yaml`. Le renommer (`crm` → `sales`) retire les lignes `crm_*` et publie `sales_*` : la règle `crm` ne vise plus rien, et le CRM est ouvert à tous sans qu'aucune règle ait changé. La propriété du §3.3 ne protège pas de ça. Donc :
- à la saisie : « ne vise rien aujourd'hui » ;
- en continu : le balayage minute du serveur repère une règle qui ne vise plus aucun outil publié (alors qu'elle en visait : `access_rules.last_matched_at`) ; bandeau dans `/admin` et notification des admins, une fois par règle ;
- `CLAUDE.md` : renommer un serveur MCP = refaire ses règles.

### 3.2 Qui garde l'accès

Une règle porte des **groupes** et/ou des **utilisateurs**. Groupes : `groups` (nom), `group_members` (groupe, utilisateur), édités dans `/admin`. Un groupe sert à n'écrire une règle qu'une fois pour une équipe ; un utilisateur seul évite de créer un groupe pour une personne.

**Une règle sans détenteur** *[rev. I3]* (ses groupes supprimés, ses utilisateurs supprimés) fermerait l'outil à tous sauf aux admins, en silence. Supprimer un groupe **seul détenteur** d'une règle est refusé, avec les règles nommées (les modifier d'abord) ; la suppression d'un utilisateur seul détenteur rend la règle visible en tête de `/admin` (« sans détenteur : fermée à tous sauf aux admins »).

### 3.3 Plusieurs règles sur un outil

Un outil visé par **plusieurs** règles exige **chacune** : chaque règle est un verrou. Exemple : `crm` → Commercial, et `crm_update_*` → Managers : `crm_search` est ouvert aux commerciaux, `crm_update_contact` aux commerciaux **qui sont aussi** managers.

Propriété voulue : **ajouter une règle ne peut jamais ouvrir un accès**, seulement en fermer. L'union (une règle suffit) aurait le défaut inverse : `crm` → Commercial suffirait à passer `crm_update_*`. Un admin qui veut l'union écrit une règle qui porte les deux groupes.

**Règles larges** *[rev. M7]* : `agent_*` ou `*` vers un petit groupe retire tous les agents aux autres. La page d'une règle dit ce qu'elle retire : « N utilisateurs n'ont plus aucun agent » (et lesquels), calculé avant d'enregistrer.

### 3.4 Sans utilisateur

Un run sans `UserID` n'a droit qu'aux outils qu'aucune règle ne vise, et à un agent qu'aucune règle ne vise. Aujourd'hui tout tour a un utilisateur (web, Telegram par `GetUserByTelegramID`, tâche planifiée) : c'est un filet, pas un cas courant.

### 3.5 Résumé de la décision

```
allowed(user, tool) =
    user != nil && !user.disabled &&
    ( user.admin
   || every rule r matching tool: user ∈ r.users || user.groups ∩ r.groups ≠ ∅ )
    (no rule matching tool → allowed, user disabled excepted)
allowed(nil, tool) = no rule matches tool
```

Une fonction pure (`access.Allowed`, paquet sans base ni Temporal), testée seule, utilisée partout (catalogue, serveur, admin).

## 4. Où la règle s'applique

### 4.1 Le tour (enforcement)

- `ListToolsInput` gagne `UserID`. Le catalogue garde `AllowedTools(agentID)`, **non filtré**, pour l'inventaire de `/admin` (`web/admin/inventory.go`), et gagne `AllowedToolsFor(agentID, user)` pour le tour *[rev. I6]* : il retire les outils que l'utilisateur n'a pas, `agent_<id>` compris. Le dispatch d'`AgentWorkflow` refuse déjà ce qui n'est pas dans `Resolutions` : rien d'autre à changer pour l'enforcement des outils.
- `ListToolsOutput` gagne `Refused string` *[rev. I1]* : vide = l'agent est ouvert à l'utilisateur ; sinon la raison (« agent X not open to Bob »). Jamais un booléen « permis » : sa valeur zéro (faux de test, sortie d'un worker en retard) refuserait, l'inverse du principe. De même `Closed []string` (les outils de la liste de l'agent fermés à l'utilisateur, pour le prompt), vide par défaut.
- **Refus d'un agent** : `AgentWorkflow` s'arrête avant tout appel au modèle, avec dans sa sortie `Error` (« Tu n'as pas accès à l'agent X. Demande à un admin. ») et `ErrorType = AgentNotOpen`. Le participant écrit le `turn_end` d'erreur comme pour tout échec et notifie. **Mais il relaie quand même** *[rev. B2]* : aujourd'hui un tour en erreur ne relaie pas (`workflow/participant.go`, `switch` sur la raison) ; un droit retiré entre `Deliver` et le relais priverait alors tous les agents suivants du message. Le refus est celui d'un agent, pas du message : le `switch` traite `AgentNotOpen` comme un tour réussi pour le relais (sans `earlier_turns` de ce tour, qui n'a rien écrit).
- `turn_started` part avant le refus (le participant l'envoie avant de lancer l'enfant) : le fil montre « X travaille… » puis l'erreur, un éclair *[rev. M3]*. Assumé : une seule source pour la règle (le catalogue), pas une seconde lecture dans `CheckTurn`. Le serveur évite déjà ce cas pour un message neuf (§4.4).
- **Sous-agent** : `agent_X` n'est dans les outils du parent que si l'utilisateur y a droit ; le sous-agent refait `ListTools(X, user)` : ses propres outils, filtrés pour le même utilisateur.
- **Tâche de fond** : l'outil est résolu dans le tour qui la lance, donc déjà filtré. Un droit retiré pendant qu'elle tourne ne l'arrête pas (comme un tour en cours) ; le tour qui la réveille est filtré, et refusé si l'agent est fermé.
- **Tâche planifiée refusée** *[rev. M4]* : sur `AgentNotOpen`, `ScheduledAgentWorkflow` ne la relance pas à chaque cron : le schedule est mis en pause (`task_logs` `paused`, raison), le propriétaire notifié une fois. Un droit rendu ne la relance pas d'elle-même (il la reprend).
- **Délai** : le catalogue est rechargé toutes les 30 s ; un changement de règle vaut pour les tours qui commencent ensuite. Un tour en cours garde ses outils. Assumé et dit dans `/admin`.
- **Ce qui ne passe pas par `ListTools`**, vérifié : `query_workflow` reste dans sa session ; `when_task_done` n'exécute rien ; les replis de `CodingRunWorkflow`/`ImplementRunWorkflow` sont des enfants d'un outil déjà dispatché ; `agent machine-echo` et `agent claude-code-run` sont des commandes d'opérateur ; `AgentWorkflowInput.SystemPrompt` n'est posé par aucun code de production. Un outil publié sous `agent_` est déjà ignoré par le catalogue.

Le catalogue charge, avec les agents et les outils : les règles, et pour chaque utilisateur ses groupes, son rôle et `disabled_at` (installations de quelques dizaines de personnes : une requête par rechargement). `AllowedToolsFor` reste en mémoire, sans requête par tour.

`LoadSkillsForAgent` appelle aussi `AllowedTools` pour un `SystemPrompt` que le tour n'utilise pas : à vérifier au codage (le back-office le lit-il encore ?) ; s'il est mort, le retirer plutôt que le filtrer *[rev. I6]*.

### 4.2 Le prompt

`CallLLM` construit le prompt depuis les outils offerts (`req.Tools`, `AgentPrompt(agentID, tools)`) : filtrés, les sections de comportement ne parlent plus d'un outil retiré. Rien à faire de plus.

**Cache** *[rev. M1]* : la section mémoire n'est ajoutée qu'à un tour de session ou une tâche planifiée, et seulement si l'utilisateur a une mémoire. Dans une session partagée, le cache est déjà manqué quand l'un des deux en a une ; sinon des outils filtrés par utilisateur sont un coût nouveau (la liste d'outils est en tête du préfixe), mais seulement entre utilisateurs aux droits différents. Assumé.

Une ligne s'ajoute quand des outils de **la liste de l'agent** sont fermés à cet utilisateur (`Closed`, jamais le catalogue entier), triés, stables sur le tour : « Some of your tools are not open to Bob: crm_search, crm_update_contact. If asked for what they do, say an admin can give access. » Les noms seulement, pas les descriptions, ni les règles ni les groupes. Sans elle, l'agent fait comme si l'outil n'existait pas, ou promet ce qu'il ne peut pas faire.

### 4.3 L'historique *[rev. M2]*

Seuls les tours du **même** agent gardent leurs blocs `tool_use` (`conversation.Convert`) ; ceux d'un autre agent sont du texte. Le tour de Bob relit donc les tours du même agent pour Alice, dont des appels d'outils que Bob n'a pas.

L'API n'exige qu'**au moins une** définition d'outil dès que la requête porte des blocs d'outils : c'est le cas que `withdrawnTools` traite aujourd'hui, pour un outil disparu du catalogue en cours de tour (il ne regarde que les noms de `req.Tools` manquants). Un agent dont tous les outils sont fermés à Bob enverrait des blocs sans définition : refus de l'API. On généralise : la fonction compare les outils **appelés dans la conversation** aux outils **offerts**, et donne une définition minimale à chacun des appelés non offerts, hors du prompt, avec deux textes : « Withdrawn: … » (disparu du catalogue) et « Not open in this turn. Do not call it. » (fermé à cet utilisateur : il peut revenir au tour suivant). Un stub par nom plutôt qu'un seul : le modèle est prié de ne pas les appeler. Rien n'est révélé : le nom est déjà dans l'historique. Le résultat reste lisible comme aujourd'hui (§6).

### 4.4 Le serveur (ergonomie, pas sécurité)

Le serveur lit les règles en base (pas de catalogue à lui), gardées 3 s comme les états des sessions *[rev. M8]* :
- **`Deliver`** *[rev. B1]* : la liste des agents qui répondent se construit comme aujourd'hui (mentions, sinon l'agent de la session selon `agent_mode`), **puis** on retire ceux auxquels l'auteur n'a pas droit. Si des mentions existaient et qu'il n'en reste aucune, **personne ne répond** : jamais de repli sur l'agent de la session pour une demande destinée à un autre. `MaxQueued` et `part` se calculent sur la liste filtrée ; vide = rien n'est signalé à aucun participant. Le message est enregistré (il reste un message entre humains) ; l'événement `user_message` porte `refused_agents`, le fil affiche « @commercial ne t'est pas ouvert ». Telegram : une réponse texte dans la réponse du webhook.
- **Zone de saisie** : quand l'agent de la session est fermé au membre, elle le dit **avant** l'envoi (« L'agent de cette session ne t'est pas ouvert : tes messages vont aux membres seulement »), pas seulement le rail.
- **Ouvrir une session** (`Open`, `POST /sessions`) avec un agent fermé : refusé (403, « agent non ouvert »). Sans agent demandé : l'agent par défaut s'il est ouvert, sinon le premier ouvert dans l'ordre de `resolveAgentID` (`agent_id`), sinon refus *[rev. M5]*. Telegram (l'utilisateur ne choisit pas) : la première réponse est signée, pour qu'il sache à qui il parle.
- **Forker** *[rev. I5]* : un fork prend l'agent du parent. Agent fermé au membre qui forke = refus (403, même message qu'`Open`) : sinon il obtiendrait une session à lui dont l'agent ne lui répond jamais.
- **Listes d'agents** (accueil, « + » de l'arbre, rail « Agents à appeler ») : seulement ceux qui lui sont ouverts.
- **Être invité** dans une session dont l'agent est fermé : permis. On lit et on écrit aux autres membres ; l'agent ne répond pas à ses messages.

L'enforcement reste dans le tour (§4.1) : le serveur peut se tromper (règle changée entre-temps) sans ouvrir quoi que ce soit.

## 5. `/admin`

- **Groupes** : créer, renommer, supprimer (refusé s'il est seul détenteur d'une règle, §3.2), membres (par email).
- **Règles** : cible (serveur MCP dans une liste, ou motif), groupes et utilisateurs ; pour chacune, les outils qu'elle vise aujourd'hui et ce qu'elle retire (§3.3) ; en tête, les règles sans détenteur et celles qui ne visent plus rien (§3.1, §3.2).
- **Page d'un agent** : pour chaque outil de sa liste, « tous » ou les groupes/utilisateurs qui y ont droit ; une règle sur l'agent lui-même affichée en tête. Vue non filtrée (`AllowedTools`).
- **Page d'un utilisateur** : ses groupes, et une vue « ce qu'il peut utiliser » : par agent, ses outils ouverts et fermés (`access.Allowed`), pour vérifier une règle avant de la croire.
- Un outil `Sensitive` qu'aucune règle ne vise est signalé (« ouvert à tous »), sans rien changer par défaut.
- Délai de 30 s rappelé à chaque enregistrement.

## 6. Ce que les règles ne font pas

- **Elles portent sur l'action, pas sur ce qu'on voit.** Dans une session partagée, Bob lit le fil, donc les résultats des outils qu'Alice a lancés (sauf `PrivateInput`, déjà caché). Les tours de Bob relisent aussi ces résultats : un agent peut donc répéter à Bob ce que `crm_search` a rendu à Alice. Protéger des données = des sessions séparées. Masquer ces résultats aux tours d'un autre utilisateur viendrait plus tard (§8.4).
- **Une identité par serveur MCP** : un serveur a une `api_key` (celle du worker). Tous ceux qui y ont droit agissent avec le même compte ; la règle dit qui peut s'en servir, pas avec quels droits côté CRM. Une identité par utilisateur (OAuth par personne) est un autre chantier.
- **Un outil est identifié par son nom** *[rev. M6]* (clé de `tools`) : un même serveur sur deux queues, ou un outil intégré et un outil MCP homonymes sur deux workers, font basculer `mcp_server` au gré des publications. Déjà hors contrat (une queue = une capacité, mêmes outils partout) ; une règle « serveur » peut alors viser l'outil par intermittence.
- **Les machines** restent à leur propriétaire (déjà). `analyze_repo` et `implement_feature` sont des outils : une règle peut les fermer comme les autres, machine comprise.
- Le modèle utilisé, le budget, les skills : hors sujet.

## 7. Ce qui change

- **Paquet `access`** (nouveau, pur) : `Rule{Kind, Target, Groups, Users}`, `User{ID, Admin, Disabled, Groups}`, `Allowed(user *User, tool ToolRef{Name, MCPServer}, rules)`, `Matching(tool, rules)`, `Removes(rule, users, agents)` (§3.3).
- **`store`** : tables `groups`, `group_members`, `access_rules` (+ `access_rule_groups`, `access_rule_users` ; `last_matched_at`) ; suppression d'un groupe seul détenteur refusée dans la transaction ; colonne `tools.mcp_server` ; lectures pour le catalogue (`AccessSnapshot` : règles, groupes, rôle et `disabled_at` par utilisateur) et pour le serveur (`UserAccess(user)`).
- **`tool`** : `Tool.MCPServer`, posé par `Discover`, publié.
- **`activity`** : le catalogue tient l'instantané d'accès (`SetAccess`, rechargé avec le reste) ; `AllowedToolsFor(agentID, user)` filtre et rend `Refused` et `Closed` ; `ListToolsInput.UserID` ; `withdrawnTools` généralisé (§4.3) ; la ligne du prompt (§4.2).
- **`workflow`** : `AgentWorkflow` passe `UserID` à `ListTools`, s'arrête sur `Refused` (`AgentNotOpen`) ; le participant relaie sur `AgentNotOpen` ; `ScheduledAgentWorkflow` met en pause sur `AgentNotOpen`. Pas de prod : les runs ouverts sont terminés au déploiement, comme d'habitude.
- **`session`** : `Deliver` filtre après coup (§4.4), `Open` et `Fork` refusent un agent fermé ; listes d'agents filtrées (`cmd/agent/ui.go`, `api`), zone de saisie.
- **Serveur** : balayage des règles qui ne visent plus rien (§3.1).
- **`web/admin`** : groupes, règles, colonnes « qui » sur la page d'un agent, vue d'un utilisateur, bandeaux.
- **`CLAUDE.md`** : une section « Droits ».

## 8. Points ouverts, avec la proposition

1. **Nommer les outils fermés dans le prompt** (§4.2) : oui, les noms seuls, ceux de la liste de l'agent seulement.
2. **Intersection des règles** (§3.3) plutôt qu'union : oui, avec les garde-fous des changements hors règles (§3.1, §3.2, §3.3).
3. **Un agent fermé dans une session où l'invité écrit** (§4.4) : le message passe, l'agent ne répond pas ; toutes les mentions fermées = personne ne répond ; la zone de saisie prévient avant l'envoi.
4. **Masquer aux tours d'un autre utilisateur les résultats des outils qui lui sont fermés** : plus tard. Le mécanisme existe (`PrivateInput` pour un autre utilisateur dans `Convert`), ce serait une vingtaine de lignes ; mais tant que les membres lisent le fil, ça ne protège que de la répétition par l'agent, et ça dégrade son contexte.

## 9. Tests

- `access` : ouvert sans règle ; serveur contre motif (`web` serveur ≠ `web_fetch`) ; intersection ; admin ; admin désactivé ; utilisateur désactivé ; utilisateur direct et par groupe ; règle sans détenteur ; motif `*` ; sans utilisateur ; `Removes`.
- Catalogue : `AllowedToolsFor(agent, user)` filtre outils publiés et `agent_<id>`, `Refused`, `Closed` ; `*` et `agent_*` n'ouvrent pas un outil fermé ; `AllowedTools` (inventaire `/admin`) inchangé.
- `AgentWorkflow` : agent fermé = arrêt avant le modèle (`AgentNotOpen`) ; sous-agent fermé non offert ; sous-agent ouvert dont un outil est fermé ; sortie de `ListTools` sans les nouveaux champs = ouvert.
- Participant : refus `AgentNotOpen` puis relais au suivant.
- `ScheduledAgentWorkflow` : refusé = mis en pause, notifié une fois.
- `withdrawnTools` généralisé : outil appelé par Alice, fermé à Bob, défini en stub « not open » ; agent dont tous les outils sont fermés à Bob avec des blocs d'outils dans l'historique : requête valide.
- `session` : `Deliver` retire et dit ; toutes les mentions fermées = personne ne répond (pas l'agent de la session) ; `Open` et `Fork` refusent ; agent par défaut fermé ; listes filtrées.
- `store` (base jetable) : migrations, cascades, refus de supprimer un groupe seul détenteur, instantané.
- Balayage : règle qui ne vise plus rien après renommage d'un serveur = alerte une fois.
- Publication : `mcp_server` posé et lu.
- Smoke `RealServer` de `workflow/` (boucle du participant, `AgentWorkflow`) et `TestMachinesLLM_RealServer`.
