# Conception : les skills d'un agent dans ses runs Claude Code

Statut : **version 1**, proposition, rien n'est fait. Le 9 octobre 2026.

## 1. Pourquoi

Un agent a des skills (`agents.skills`, dépôt `SKILLS_REPO` ou dossier `./skills`, chargées par `LoadSkillsForAgent` dans **son** prompt). Mais quand il lance `analyze_repo` ou `implement_feature`, c'est la CLI Claude Code qui travaille, avec un prompt système fixé par la plateforme (`machine.AnalyzeSystemPrompt`, `ImplementSystemPrompt`) : les skills de l'agent ne lui parviennent pas. La seule voie aujourd'hui est que l'agent recopie la consigne dans la tâche, à chaque appel, de mémoire.

Cas d'usage déclencheur : un agent « TDD » dont les implémentations suivent RED → GREEN → REFACTOR, un commit par étape. La consigne doit atteindre **la CLI**, à chaque run, sans dépendre de la mémoire de l'agent, et marcher **aux deux endroits** où un run tourne : la machine de l'utilisateur (`agent connect`, hors du réseau privé) et nos workers Claude Code (repli, réseau privé).

## 2. Le principe

1. **Une skill marquée pour les runs** : le frontmatter de son `SKILL.md` porte `runs: true`. Seules celles-là partent avec un run : une skill de recherche de marché n'a rien à faire dans une implémentation.
2. **Le workflow ne transporte que des références** : les noms des skills de l'agent marquées `runs`, posés dans l'entrée de l'outil par le dispatch (comme le `CallContext`, une clé réservée que le modèle ne peut pas fournir). Jamais leur contenu dans l'historique Temporal.
3. **Celui qui prépare le run lit les skills** et les écrit dans un **dossier du run** :
   - **vers une machine** : `RunOnMachine` (sur le worker principal, celui qui a les skills) les lit dans son magasin de skills et les met **dans la directive** (`machine.AnalyzeInput`/`ImplementInput.Skills` : nom et contenu) ; la machine les écrit dans son dossier de run ;
   - **sur un worker Claude Code** : `PrepareWorkspace` les lit dans **sa propre** source de skills. Les conteneurs n'en ont pas aujourd'hui (« No skills source configured ») : leur donner la même que le worker principal (`SKILLS_REPO`, ou `./skills` monté en dev).
4. **La CLI les reçoit pour ce run seulement**, sous forme d'un **plugin local** : `<run>/plugin/.claude-plugin/plugin.json` et `<run>/plugin/skills/<nom>/SKILL.md`, passé par `--plugin-dir`. Rien n'est écrit dans la configuration de l'utilisateur (`~/.claude`) ni dans celle de l'opérateur : le dossier est effacé avec le run.
5. **Un seul code** (paquet `machine`, sans base ni Temporal) écrit le plugin et ajoute l'option, partagé par `agent connect` et par les activities des workers.

Le format est déjà le bon : nos skills sont des `SKILL.md` avec `name` et `description` en frontmatter, le format des skills de Claude Code.

## 3. Vérification préalable (avant toute ligne de code)

Sur un vrai run (un appel minuscule, plafonné) :
- un plugin passé par `--plugin-dir` est-il chargé **malgré `--setting-sources user`** (que la plateforme pose pour qu'un dépôt cloné ne puisse pas imposer ses réglages) ?
- ses skills sont-elles proposées à la CLI, en mode `plan` (analyse) comme `acceptEdits` (implémentation), avec `--strict-mcp-config` ?
- le `CLAUDE.md` du dépôt cloné est-il lu avec `--setting-sources user` (question posée en marge : la règle d'un projet a sa place dans son `CLAUDE.md`) ?

Si `--plugin-dir` n'est pas chargé dans ce mode : repli sur `--append-system-prompt` avec le contenu des skills (moins bien : tout est chargé d'office, sans que la CLI choisisse), à trancher avec ce résultat.

## 4. Ce que la plateforme garantit, et ce qu'elle ne garantit pas

- **Les droits ne changent pas** : une skill est une consigne. La liste des outils de la CLI reste la nôtre (lecture seule en analyse ; `git add`/`commit`/`status` en implémentation, `Edit(.git/**)` refusé) : une skill qui contient un script ne peut pas l'exécuter.
- **Seul le `SKILL.md` part** (le magasin actuel ne charge que lui). Les ressources d'une skill (fichiers à côté) viendront plus tard si un besoin réel apparaît, bornées en nombre et en taille.
- **Pas de version épinglée** : le magasin de skills n'a pas de version adressable (`skills_version` est un compteur, le dépôt est tiré au dernier commit). Un run utilise la version que lit, au moment de le préparer, celui qui le prépare : le worker principal pour une machine, le conteneur pour le repli. Ils peuvent différer de quelques secondes après une modification du dépôt (rechargement à 30 s) : accepté. Le compteur est noté dans le résultat du run pour la traçabilité.
- **Taille** : 64 Kio de skills au plus par run (la directive voyage dans la WebSocket, limite de 4 Mio ; la CLI les lit toutes au démarrage) ; au-delà, refus du run avec la raison.
- **Confiance** : une skill est une instruction donnée à la CLI de l'utilisateur, sur sa machine : le modèle de confiance retenu (un groupe qui fait confiance à son admin) le permet ; l'admin qui ajoute une skill `runs: true` le fait pour les runs de tous les agents qui l'ont.

## 5. Ce qui change

- **`skill`** : `Skill.Runs` lu dans le frontmatter.
- **Catalogue / dispatch** : la liste des skills `runs` de l'agent posée dans l'entrée d'`analyze_repo`/`implement_feature` (clé réservée).
- **`machine`** : `Skills` dans les entrées de directive ; `WritePlugin(dir, skills)` et l'option `--plugin-dir` ; protocole +1.
- **Workers** : `RunOnMachine` (via le routage `CodingRunWorkflow`/`ImplementRunWorkflow`) lit les skills et les met dans la directive ; `PrepareWorkspace` les écrit pour un run de repli ; `RunClaudeCode` passe `--plugin-dir`.
- **`agent connect`** : écrit le plugin dans le dossier du run, passe l'option, l'efface avec le run.
- **Compose** : une source de skills pour `claude-code` et `claude-code-ro`.
- **Docs** : `CLAUDE.md`, `README.md`, et un exemple de skill `tdd` (`runs: true`) dans `skills/`.

## 6. Tests

Unitaires : frontmatter `runs`, sélection, `WritePlugin` (arborescence, noms nettoyés, taille), options de la CLI. `RealServer` des machines (CLI factice) : la directive porte les skills, la CLI factice reçoit `--plugin-dir` et y trouve le `SKILL.md` ; le repli aussi. Vérification réelle (§3), puis un essai avec la skill `tdd` sur une machine et sur le repli.
