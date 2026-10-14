# Droits — qui peut se servir de quel outil et de quel agent

Sigle **DR**. Source : `docs/design/permissions.md` (v1.1 et ses écarts, §10) et le code au 14 octobre 2026.

## Le contexte

Un agent fait ce que ses outils lui permettent. Certains outils ouvrent une ressource qui n'est pas pour
tout le monde : le CRM de l'équipe commerciale branché par un serveur MCP, l'envoi d'e-mails au nom de
l'installation, l'exécution de commandes sur le serveur partagé.

Restreindre un agent ne suffit pas : un agent ouvert à tous peut déléguer à l'agent restreint, et un
agent « à tout faire » reçoit les outils d'un serveur dès qu'on le branche. L'unité qui compte est donc
l'**outil** ; un agent n'est qu'un outil de plus (« appeler l'agent X »), et un serveur MCP un groupe
d'outils.

Le principe : **tout est ouvert tant que l'administrateur n'a rien fermé**. Une règle ferme ce qu'elle vise
et dit qui garde l'accès.

## Glossaire

### Acteurs

| Terme | Définition |
|---|---|
| **Utilisateur** | Personne qui a un compte sur l'installation. |
| **Administrateur** | Utilisateur au rôle admin : il gère les groupes et les règles, et peut se servir de tout (DR7). |
| **Compte désactivé** | Utilisateur que l'administrateur a désactivé : il ne peut plus se servir de rien, admin compris (DR8). |
| **Utilisateur du tour** | Celui pour qui un agent travaille pendant un tour : c'est lui que les règles contrôlent (DR12). |
| **Détenteur** | Utilisateur qui garde l'accès aux outils qu'une règle ferme : nommé par la règle, ou membre d'un groupe qu'elle nomme. |

### Ce que les règles protègent

| Terme | Définition |
|---|---|
| **Outil** | Capacité qu'un agent peut utiliser pendant un tour : chercher sur le web, envoyer un e-mail, exécuter une commande… Identifié par son nom. |
| **Serveur MCP** | Service externe branché par l'administrateur, qui fournit un ensemble d'outils (le CRM, une base…). Ses outils peuvent changer avec le temps. |
| **Provenance** | Le serveur MCP dont vient un outil ; un outil intégré n'en a pas. |
| **Agent (comme outil)** | Pour les droits, un agent est l'outil « appeler l'agent X » (`agent_X`) : le fermer ferme l'agent, quelle que soit la façon de l'appeler (DR5). |
| **Liste d'outils d'un agent** | Les outils qu'un agent peut utiliser par conception, choisis par l'administrateur pour son rôle. Ce n'est pas un droit : c'est le périmètre de l'agent (contexte Agents). |
| **Outil sensible** | Outil marqué comme engageant (exécution, écriture, e-mail) : il est signalé quand il reste ouvert à tous (DR29). |

### Règles

| Terme | Définition |
|---|---|
| **Règle d'accès** | Ferme un ensemble d'outils à tous, sauf à ses détenteurs. |
| **Cible** | Ce qu'une règle vise : un serveur MCP, ou un motif de nom. |
| **Motif** | Nom d'outil avec jokers (`crm_update_*`, `agent_*`, `*`). |
| **Groupe** | Ensemble nommé d'utilisateurs, pour écrire une règle une fois pour une équipe. |
| **Ouvert / fermé** | Un outil est ouvert à un utilisateur s'il peut s'en servir, fermé sinon. |
| **Règle sans détenteur** | Règle qui ne nomme ni groupe ni utilisateur : ses outils ne sont ouverts qu'aux administrateurs (DR33). |
| **Règle qui ne vise plus rien** | Règle qui visait des outils et n'en vise plus aucun, parce que le serveur a été renommé ou ses outils retirés (DR34). |

### Conversation (vocabulaire emprunté au contexte Conversations)

| Terme | Définition |
|---|---|
| **Tour** | Le travail d'un agent pour répondre à un message, d'un seul tenant. *(à confirmer : défini dans Conversations)* |
| **Session** | Conversation partagée entre des membres et un ou plusieurs agents ; elle a un agent par défaut, l'**agent de la session**. |
| **Mention** | `@nom` dans un message : appelle cet agent. |
| **Sous-agent** | Agent appelé par un autre agent pendant son tour (délégation). |
| **Tâche de fond** / **tâche planifiée** | Travail d'un agent lancé pour plus tard ou à heure fixe (contexte Travail en arrière-plan). |

## Règles de gestion, par lot

| Lot | Thème | Règles |
|---|---|---|
| 1 | Ouvert par défaut, fermer un outil | DR1, DR2, DR3, DR4, DR5 |
| 2 | Qui garde l'accès | DR6, DR7, DR8, DR9, DR10 |
| 3 | Un tour ne fait que ce qui est ouvert à son utilisateur | DR11, DR12, DR13, DR14, DR15 |
| 4 | Un agent fermé dans une session partagée | DR16, DR17, DR18, DR19, DR20, DR21, DR22, DR23 |
| 5 | Administrer les groupes et les règles | DR24, DR25, DR26, DR27, DR28, DR29 |
| 6 | Tâches planifiées refusées | DR30, DR31 |
| 7 | Règles qui dérivent | DR32, DR33, DR34, DR35, DR36 |

### Lot 1 — Ouvert par défaut, fermer un outil

Le cœur : sans lui, il n'y a rien à protéger. Un outil est ouvert à tous jusqu'à ce qu'une règle le ferme.

- **DR1** — Tant qu'aucune règle ne vise un outil, tout utilisateur actif peut s'en servir.
- **DR2** — Une règle ferme les outils qu'elle vise à tous, sauf à ses détenteurs : les utilisateurs qu'elle
  nomme et les membres des groupes qu'elle nomme.
- **DR3** — Une règle vise soit tous les outils d'un serveur MCP, y compris ceux qu'il ajoutera plus tard,
  soit les outils dont le nom correspond à un motif.
- **DR4** — Un outil appartient à un serveur MCP par sa provenance, jamais par son nom : une règle sur le
  serveur « web » ne vise pas l'outil intégré « web_fetch ».
- **DR5** — Fermer un agent, c'est fermer l'outil qui l'appelle : il ne répond plus, qu'on le mentionne,
  qu'il soit l'agent de la session, qu'on forke la session ou qu'un autre agent lui délègue.

### Lot 2 — Qui garde l'accès

Les cas où la réponse ne se lit pas sur une seule règle.

- **DR6** — Un outil visé par plusieurs règles exige chacune d'elles : ajouter une règle ne peut jamais
  ouvrir un accès, seulement en fermer. *Exemple : « serveur crm → Commercial » et « crm_update_* →
  Managers » : `crm_search` est ouvert aux commerciaux, `crm_update_contact` aux commerciaux qui sont aussi
  managers.*
- **DR7** — Un administrateur actif peut se servir de tout, quelles que soient les règles.
- **DR8** — Un compte désactivé ne peut se servir de rien, administrateur compris : aucun agent ne lui
  répond, et ses tâches planifiées et tâches de fond sont refusées.
- **DR9** — Sans utilisateur identifié, seul ce qu'aucune règle ne vise est permis.
- **DR10** — Un utilisateur tout juste créé n'est dans aucun groupe et n'est pas administrateur tant que
  les règles ne le connaissent pas. ⚠ **à requestionner** : conséquence d'un instantané rechargé toutes les
  30 s, pas un choix métier ; peut-être à fondre dans DR15.

### Lot 3 — Un tour ne fait que ce qui est ouvert à son utilisateur

Là où la règle s'applique vraiment : au début de chaque tour, pour celui que le tour sert.

- **DR11** — Les outils d'un tour sont ceux de la liste de l'agent qui sont ouverts à l'utilisateur du
  tour ; l'agent ne peut en appeler aucun autre.
- **DR12** — L'utilisateur du tour est celui que le tour sert, jamais choisi par le modèle : l'auteur du
  message ; pour un sous-agent, l'utilisateur de l'agent qui lui délègue ; pour une tâche de fond, celui
  qui l'a demandée ; pour une tâche planifiée, son propriétaire, à chaque exécution.
- **DR13** — Un agent fermé à l'utilisateur du tour ne répond pas : son tour s'arrête avant tout travail et
  le dit dans la session (« L'agent X n'est pas ouvert à Bob : un admin peut lui en donner l'accès. »).
- **DR14** — L'agent connaît les outils de sa liste qui sont fermés à l'utilisateur du tour, par leur nom
  seulement ; si on lui demande ce qu'ils font, il répond qu'un administrateur peut en donner l'accès.
- **DR15** — Un changement de règle vaut pour les tours qui commencent ensuite ; un tour en cours et une
  tâche de fond déjà lancée gardent leurs outils. ⚠ **à requestionner** : aujourd'hui « ensuite » veut
  dire « jusqu'à 30 s plus tard » (rechargement), délai technique annoncé dans l'administration.

### Lot 4 — Un agent fermé dans une session partagée

L'ergonomie : la session évite de solliciter un agent fermé et le dit à l'auteur, avant ou après l'envoi.
Le tour reste seul juge (DR13).

- **DR16** — Un message est toujours enregistré et lu par les membres, même si aucun des agents qu'il
  appelle n'est ouvert à son auteur.
- **DR17** — Les agents fermés à l'auteur d'un message ne lui répondent pas ; si toutes les mentions du
  message sont fermées, personne ne répond, jamais l'agent de la session à leur place.
- **DR18** — L'auteur apprend quels agents ne lui sont pas ouverts ; quand l'agent de la session lui est
  fermé, la zone de saisie le prévient avant l'envoi.
- **DR19** — On ne peut pas ouvrir une session avec un agent qui nous est fermé ; sans agent demandé, la
  session prend l'agent par défaut s'il nous est ouvert, sinon le premier qui l'est, sinon elle est
  refusée. ⚠ **à requestionner** : « le premier qui l'est » suit l'ordre des identifiants, arbitraire.
- **DR20** — On ne peut pas forker une session dont l'agent nous est fermé.
- **DR21** — On peut être invité dans une session dont l'agent nous est fermé : on y lit et on y écrit aux
  autres membres.
- **DR22** — La liste des agents qu'on peut appeler dans une session ne montre que ceux qui nous sont
  ouverts.
- **DR23** — Un message adressé à plusieurs agents continue vers les suivants quand l'un d'eux s'avère
  fermé à son auteur au moment de son tour.

### Lot 5 — Administrer les groupes et les règles

Ce qui permet à l'administrateur d'écrire une règle juste et de la vérifier avant de la croire.

- **DR24** — Seul un administrateur gère les groupes et les règles.
- **DR25** — Un groupe a un nom et des membres, ajoutés par leur e-mail ; un utilisateur peut être dans
  plusieurs groupes.
- **DR26** — Avant d'enregistrer une règle, l'administrateur voit les outils qu'elle vise aujourd'hui et les
  utilisateurs qu'elle laisserait sans aucun agent.
- **DR27** — Pour un utilisateur, l'administrateur voit ses groupes et, pour chaque agent, s'il lui répond
  et quels outils lui sont ouverts ou fermés.
- **DR28** — Pour un agent, l'administrateur voit qui peut se servir de lui et de chacun de ses outils.
- **DR29** — Un outil sensible qu'aucune règle ne vise est signalé « ouvert à tous », sans que rien ne
  change.

### Lot 6 — Tâches planifiées refusées

Une tâche qui revient seule ne doit pas échouer en boucle sans que personne le sache.

- **DR30** — Une tâche planifiée récurrente dont l'agent est fermé à son propriétaire est mise en pause, et
  son propriétaire prévenu une fois. ⚠ **à requestionner** : un accès rendu ne la relance pas, et rien ne
  permet de la reprendre ; il faut l'annuler et la replanifier.
- **DR31** — Une tâche planifiée unique dont l'agent est fermé à son propriétaire échoue.

### Lot 7 — Règles qui dérivent

Les changements faits hors des règles (un groupe supprimé, un serveur renommé) qui modifient en silence ce
qu'elles protègent.

- **DR32** — Supprimer un groupe qui est le seul détenteur d'une règle est refusé, en nommant les règles à
  modifier d'abord.
- **DR33** — Une règle sans détenteur est permise : ses outils ne sont ouverts qu'aux administrateurs, et
  elle est signalée.
- **DR34** — Une règle qui visait des outils et n'en vise plus aucun déclenche une alerte aux
  administrateurs actifs, une seule fois ; l'alerte s'éteint si la règle revise quelque chose, et repart de
  zéro si la règle est modifiée.
- **DR35** — Une règle qui n'a jamais rien visé est signalée dans l'administration, sans alerte.
- **DR36** — Renommer un serveur MCP rouvre ses outils à tous jusqu'à ce que ses règles soient refaites.
  ⚠ **à requestionner** : la seule façon d'ouvrir un accès sans toucher aux règles ; DR34 ne fait que
  l'alerter.

## Hors du contexte

Ce que les droits ne font pas, voulu :

- **Ils portent sur l'action, pas sur la lecture.** Dans une session partagée, Bob lit les résultats des
  outils qu'Alice a lancés, et un agent peut les lui répéter. Protéger des données, c'est des sessions
  séparées. Masquer ces résultats aux tours d'un autre utilisateur est reporté (choix §8.4 de la conception).
- **Une seule identité par serveur MCP** : tous ceux qui y ont droit agissent avec le même compte côté
  serveur. Une identité par personne est un autre chantier.
- **Les machines** restent à leur propriétaire : contexte Machines, pas une règle d'accès.
- La signature de la première réponse sur Telegram : contexte Canaux.

## Annexe — où vivent ces règles aujourd'hui

Pour la reprise seulement.

| Règles | Code |
|---|---|
| DR1–DR10 | `access/access.go` (`Allowed`, `Matches`, `Holds`, `Snapshot.User`) : déjà pur, sans dépendance |
| DR11, DR13, DR14 | `activity/catalog.go` (`AllowedToolsFor` : `Refused`, `Closed`) ; `workflow/agent.go` (arrêt sur `Refused`, `AgentNotOpenMessage`) ; `activity/prompt.go` (`closedSection`) ; `activity/llm.go` (`withdrawnTools`) |
| DR12 | `workflow/agent.go`, `workflow/participant.go`, `workflow/scheduled_agent.go`, tâches de fond (`UserID` hérité) |
| DR23 | `workflow/participant.go` (relais après `AgentNotOpen`) |
| DR15, DR10 | rechargement du catalogue toutes les 30 s (`cmd/agent`, `refreshCatalog`) |
| DR30, DR31 | `workflow/scheduled_agent.go` (`pauseRefused`), `activity` (`PauseSchedule`) |
| DR16–DR22 | `session/access.go`, `session/messages.go` (`Deliver`, `RefusedText`), `session/ops.go` (`Open`), `session/fork.go` ; `cmd/agent/ui.go` (zone de saisie, rail) |
| DR24–DR29 | `web/admin/access.go` et ses gabarits ; `access.Removes` (DR26) ; `store/access.go` |
| DR32 | `store/access.go` (`DeleteGroup`, `SoleHolderError`) |
| DR34, DR35 | `cmd/agent/access.go` (`runAccessSweep`) ; `store/access.go` (`ClaimUnmatchedAlert`) |
