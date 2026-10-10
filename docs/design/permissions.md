# Conception : qui peut se servir de quels outils et agents

Statut : **version 1, à relire**, 10 octobre 2026. Rien n'est codé.

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
3. **Qui est contrôlé : l'utilisateur du tour** (`AgentWorkflowInput.UserID`, celui que le tour sert, dont la mémoire est chargée et pour qui les outils agissent) :
   - tour de session : l'auteur du message ; un relais : l'auteur du message d'origine (le même `UserID`) ;
   - sous-agent : l'utilisateur de son parent (déjà hérité) ;
   - tâche de fond : son demandeur ; la réveiller relance un tour pour lui ;
   - tâche planifiée : son propriétaire, **relu à chaque exécution** : un droit retiré s'applique au run suivant.
4. **Outils d'un tour = liste de l'agent ∩ droits de l'utilisateur.** Calculé au début du tour, là où il l'est déjà (`ListTools`). Le modèle ne voit que ceux-là, et le dispatch refuse les autres (`Resolutions` : absent = refusé, déjà le cas).
5. **Un agent qu'on n'a pas le droit d'utiliser ne répond pas** : c'est la règle sur `agent_<id>`, la même pour le `@`, l'agent d'une session et la délégation.
6. **L'admin a tout** (rôle `admin`).

## 3. Les règles

### 3.1 Cible

Une règle vise :
- **un serveur MCP** (`target_kind = mcp_server`, `target = crm`) : tous ses outils, y compris ceux qu'il ajoutera plus tard ;
- **ou un motif de nom** (`target_kind = pattern`, `target = agent_commercial`, `exec`, `crm_update_*`), mêmes globs que les listes d'outils des agents (`tool.MatchAny`).

Pourquoi une cible « serveur » à part : le préfixe `<serveur>_` n'est qu'une convention de nommage (`exposedName`). Un serveur nommé `web` donnerait `web_…`, et une règle `web_*` attraperait aussi `web_fetch` et `web_search`, outils intégrés. Il faut donc savoir **de quel serveur vient un outil**, ce que la table `tools` ne dit pas aujourd'hui (seul `kind = mcp`) :
- `tool.Tool` gagne `MCPServer string`, posé par `MCPClient.Discover` (`c.config.Name`) ;
- `tools` gagne la colonne `mcp_server` (vide pour un outil intégré), publiée avec le reste (`store.ToolRecord.MCPServer`).

Dans `/admin`, le serveur se choisit dans la liste des serveurs publiés (`SELECT DISTINCT mcp_server`) ; un motif se saisit. Un motif qui ne vise aucun outil actuel est accepté mais signalé (« ne vise rien aujourd'hui »).

### 3.2 Qui garde l'accès

Une règle porte des **groupes** et/ou des **utilisateurs**. Groupes : `groups` (nom), `group_members` (groupe, utilisateur), édités dans `/admin`. Un groupe sert à n'écrire une règle qu'une fois pour une équipe ; un utilisateur seul évite de créer un groupe pour une personne.

### 3.3 Plusieurs règles sur un outil

Un outil visé par **plusieurs** règles exige **chacune** : chaque règle est un verrou. Exemple : `crm` → Commercial, et `crm_update_*` → Managers : `crm_search` est ouvert aux commerciaux, `crm_update_contact` aux commerciaux **qui sont aussi** managers.

Propriété voulue : **ajouter une règle ne peut jamais ouvrir un accès**, seulement en fermer. L'union (une règle suffit) aurait le défaut inverse : `crm` → Commercial suffirait à passer `crm_update_*`. Un admin qui veut l'union écrit une règle qui porte les deux groupes.

### 3.4 Sans utilisateur

Un run sans `UserID` n'a droit qu'aux outils qu'aucune règle ne vise. Aujourd'hui tout tour a un utilisateur (web, Telegram par `GetUserByTelegramID`, tâche planifiée) : c'est un filet, pas un cas courant.

### 3.5 Résumé de la décision

```
allowed(user, tool) =
    user.admin
 || every rule r matching tool: user ∈ r.users || user.groups ∩ r.groups ≠ ∅
    (no rule matching tool → allowed)
```

Une fonction pure (`access.Allowed`, paquet sans base ni Temporal), testée seule, utilisée partout (catalogue, serveur, admin).

## 4. Où la règle s'applique

### 4.1 Le tour (enforcement)

- `ListToolsInput` gagne `UserID`. `Catalog.AllowedTools(agentID, user)` retire les outils que l'utilisateur n'a pas, `agent_<id>` compris. Le dispatch d'`AgentWorkflow` refuse déjà ce qui n'est pas dans `Resolutions` : rien d'autre à changer pour l'enforcement des outils. Le message d'un appel refusé reste « Tool %q is not available to this agent. » (le modèle ne l'appelle que s'il l'invente).
- `ListToolsOutput` gagne `AgentAllowed bool` : l'utilisateur n'a pas droit à l'agent du run (`agent_<id>` de l'agent lui-même). `AgentWorkflow` s'arrête alors avant tout appel au modèle, avec une erreur lisible dans sa sortie (`Error`, comme un échec de tour) : « Tu n'as pas accès à l'agent X. Demande à un admin. » Le participant écrit le `turn_end` d'erreur comme pour tout échec. Ce point couvre tout ce qui lance un tour : message, relais, réveil d'une tâche de fond, tâche planifiée, message livré avant qu'un droit soit retiré.
- **Sous-agent** : `agent_X` n'est dans les outils du parent que si l'utilisateur y a droit ; le sous-agent refait `ListTools(X, user)` : ses propres outils, filtrés pour le même utilisateur.
- **Tâche de fond** : l'outil est résolu dans le tour qui la lance, donc déjà filtré. Un droit retiré pendant qu'elle tourne ne l'arrête pas (comme un tour en cours) ; le tour qui la réveille est filtré.
- **Délai** : le catalogue est rechargé toutes les 30 s ; un changement de règle vaut pour les tours qui commencent ensuite. Un tour en cours garde ses outils. Assumé et dit dans `/admin`.

Le catalogue charge, avec les agents et les outils : les règles, et pour chaque utilisateur ses groupes (installations de quelques dizaines de personnes : une requête par rechargement). `AllowedTools` reste en mémoire, sans requête par tour.

### 4.2 Le prompt

`CallLLM` construit le prompt depuis les outils offerts (`req.Tools`, `AgentPrompt(agentID, tools)`) : filtrés, les sections de comportement ne parlent plus d'un outil retiré. Rien à faire de plus.

Le prompt dépend déjà de l'utilisateur (sa mémoire, relue à chaque appel) : dans une session partagée où Alice et Bob alternent, le cache de prompt est déjà manqué. Des outils filtrés par utilisateur ne coûtent rien de nouveau.

Une ligne s'ajoute quand des outils de l'agent sont fermés à cet utilisateur : « Some of your tools are not open to Bob: crm_search, crm_update_contact. If asked for what they do, say an admin can give access. » Elle nomme les outils (pas les règles ni les groupes) : sans elle, l'agent fait comme si l'outil n'existait pas, ou promet ce qu'il ne peut pas faire. **Point ouvert** (§8) : nommer ou seulement compter.

### 4.3 L'historique

Le tour de Bob relit les tours du même agent pour Alice, dont les blocs `tool_use` d'outils que Bob n'a pas. L'API exige qu'une requête qui porte des blocs d'outils définisse au moins un outil (`withdrawnTools`, aujourd'hui pour un outil disparu du catalogue en cours de tour). On l'étend : tout outil **appelé dans la conversation mais non offert** reçoit une définition minimale (« Not available in this turn. Do not call it. »), hors du prompt. Elle ne révèle rien : le nom est déjà dans l'historique. Le résultat, lui, reste lisible comme aujourd'hui (§6).

### 4.4 Le serveur (ergonomie, pas sécurité)

Le serveur lit les règles en base (pas de catalogue à lui) :
- **`Deliver`** : les agents appelés (mentions, ou agent de la session selon `agent_mode`) auxquels l'auteur n'a pas droit sont **retirés**, comme ceux au-delà de `maxAgentsPerMessage`. Le message est enregistré (il reste un message entre humains) ; l'événement `user_message` porte `refused_agents`, le fil affiche « @commercial ne t'est pas ouvert ». Telegram : une réponse texte dans la réponse du webhook. Rien n'est envoyé à un participant qui refuserait de toute façon (§4.1).
- **Ouvrir une session** (`Open`, `POST /sessions`) avec un agent fermé : refusé (403, « agent non ouvert »). L'agent par défaut fermé à un utilisateur : `Open` sans agent prend le premier agent qui lui est ouvert, sinon refuse.
- **Listes d'agents** (accueil, « + » de l'arbre, rail « Agents à appeler ») : seulement ceux qui lui sont ouverts.
- **Être invité** dans une session dont l'agent est fermé : permis. On lit et on écrit aux autres membres ; l'agent ne répond pas à ses messages (Deliver), et le rail le dit.

L'enforcement reste dans le tour (§4.1) : le serveur peut se tromper (règle changée entre-temps) sans ouvrir quoi que ce soit.

## 5. `/admin`

- **Groupes** : créer, renommer, supprimer, membres (par email).
- **Règles** : cible (serveur MCP dans une liste, ou motif), groupes et utilisateurs ; pour chacune, les outils qu'elle vise aujourd'hui.
- **Page d'un agent** : pour chaque outil de sa liste, « tous » ou les groupes/utilisateurs qui y ont droit ; une règle sur l'agent lui-même affichée en tête.
- **Page d'un utilisateur** : ses groupes, et une vue « ce qu'il peut utiliser » : par agent, ses outils ouverts et fermés (`access.Allowed`), pour vérifier une règle avant de la croire.
- Un outil `Sensitive` qu'aucune règle ne vise est signalé (« ouvert à tous »), sans rien changer par défaut.

## 6. Ce que les règles ne font pas

- **Elles portent sur l'action, pas sur ce qu'on voit.** Dans une session partagée, Bob lit le fil, donc les résultats des outils qu'Alice a lancés (sauf `PrivateInput`, déjà caché). Les tours de Bob relisent aussi ces résultats (l'agent les a dans son historique) : un agent peut donc répéter à Bob ce que `crm_search` a rendu à Alice. Protéger des données = des sessions séparées. Interdire d'inviter dans une session où ont tourné des outils fermés à l'invité, ou masquer ces résultats aux tours d'un autre utilisateur (comme `PrivateInput`), viendrait plus tard (§8).
- **Une identité par serveur MCP** : un serveur a une `api_key` (celle du worker). Tous ceux qui y ont droit agissent avec le même compte ; la règle dit qui peut s'en servir, pas avec quels droits côté CRM. Une identité par utilisateur (OAuth par personne) est un autre chantier.
- **Les machines** restent à leur propriétaire (déjà). `analyze_repo` et `implement_feature` sont des outils : une règle peut les fermer comme les autres.
- Le modèle utilisé, le budget, les skills : hors sujet.

## 7. Ce qui change

- **Paquet `access`** (nouveau, pur) : `Rule{Kind, Target, Groups, Users}`, `Allowed(user, groups, admin, tool ToolRef{Name, MCPServer}, rules)`, `Matching(tool, rules)`.
- **`store`** : tables `groups`, `group_members`, `access_rules` (+ `access_rule_groups`, `access_rule_users`, cascade sur suppression d'un groupe ou d'un utilisateur) ; colonne `tools.mcp_server` ; lectures pour le catalogue (`AccessSnapshot` : règles + groupes par utilisateur + admins) et pour le serveur (`UserAccess(user)`).
- **`tool`** : `Tool.MCPServer`, posé par `Discover`, publié.
- **`activity`** : le catalogue tient l'instantané d'accès (`SetAccess`, rechargé avec le reste) ; `AllowedTools(agentID, user)` filtre et rend `AgentAllowed` et les noms fermés (pour la ligne du prompt) ; `ListToolsInput.UserID` ; `withdrawnTools` étendu (§4.3).
- **`workflow`** : `AgentWorkflow` passe `UserID` à `ListTools`, s'arrête sur `!AgentAllowed`. Pas d'autre changement de commandes. Pas de prod : les runs ouverts sont terminés au déploiement, comme d'habitude.
- **`session`** : `Deliver` retire les agents fermés (`refused_agents`), `Open` refuse un agent fermé ; listes d'agents filtrées (`cmd/agent/ui.go`, `api`).
- **`web/admin`** : groupes, règles, colonnes « qui » sur la page d'un agent, vue d'un utilisateur.
- **`CLAUDE.md`** : une section « Droits ».

## 8. Points ouverts

1. **Nommer les outils fermés dans le prompt** (§4.2) ou seulement dire que certains le sont ? Nommer aide l'utilisateur à demander le bon accès ; ça lui apprend aussi qu'un serveur `crm` existe. Proposition : nommer.
2. **Intersection des règles** (§3.3) plutôt qu'union : à confirmer.
3. **Un agent fermé dans une session où l'invité écrit** (§4.4) : son message passe sans l'agent, ou refus du message ? Proposition : il passe, l'agent non.
4. **Masquer aux tours d'un autre utilisateur les résultats des outils qui lui sont fermés** (§6), comme `PrivateInput` : plus tard, ou dès maintenant ?

## 9. Tests

- `access` : ouvert sans règle ; serveur contre motif (`web` serveur ≠ `web_fetch`) ; intersection ; admin ; utilisateur direct et par groupe ; sans utilisateur.
- Catalogue : `AllowedTools(agent, user)` filtre outils publiés et `agent_<id>`, `AgentAllowed`, noms fermés ; `*` et `agent_*` n'ouvrent pas un outil fermé.
- `AgentWorkflow` (tests unitaires du workflow) : agent fermé = arrêt avant le modèle avec l'erreur ; sous-agent fermé non offert ; sous-agent ouvert dont un outil est fermé pour l'utilisateur ; tâche planifiée d'un utilisateur dont le droit a été retiré.
- `withdrawnTools` : outil appelé par Alice, absent pour Bob, défini en stub ; requête de Bob sans aucun outil offert mais avec des blocs d'outils valide.
- `session` : `Deliver` retire et dit, `Open` refuse, listes filtrées.
- `store` (base jetable) : migrations, cascades, instantané.
- Publication : `mcp_server` posé et lu.
- Smoke `RealServer` de `workflow/` et `TestMachinesLLM_RealServer` après le changement d'`AgentWorkflow`.
