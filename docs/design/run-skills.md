# Conception : les skills d'un agent dans ses runs Claude Code

Statut : **version 2.2, faite** (§11, avec ses écarts) ; vérifiée contre la vraie CLI le 10 octobre 2026 (§9). Version 1 le 9 octobre 2026, révisée le même jour après deux relectures contre le code et contre la CLI 2.1.280 de l'image (lue dans son binaire, sans appel au modèle). Les points de la première sont marqués *[rev. 1…16]*, ceux de la seconde *[rev2 1…4]*.

## 1. Pourquoi

Un agent a des skills (`agents.skills`, dépôt `SKILLS_REPO` ou dossier `./skills`, chargées par `LoadSkillsForAgent` dans **son** prompt). Mais quand il lance `analyze_repo` ou `implement_feature`, c'est la CLI Claude Code qui travaille, avec un prompt système fixé par la plateforme (`machine.AnalyzeSystemPrompt`, `ImplementSystemPrompt`) : les skills de l'agent ne lui parviennent pas. La seule voie aujourd'hui est que l'agent recopie la consigne dans la tâche, à chaque appel, de mémoire.

Cas d'usage déclencheur : un agent « TDD » dont les implémentations suivent RED → GREEN → REFACTOR, un commit par étape. La consigne doit atteindre **la CLI**, à chaque run, sans dépendre de la mémoire de l'agent, et marcher **aux deux endroits** où un run tourne : la machine de l'utilisateur (`agent connect`) et nos workers Claude Code (repli, réseau privé).

## 2. Ce que fait la CLI (lu dans le binaire 2.1.280)

- `--plugin-dir <dossier>` charge un plugin **pour cette session seulement** (répétable). Il existe aussi `--plugin-dir-no-mcp`, mais **caché** (`hideHelp`, pensé pour le SDK) : un contrat qui peut bouger, et qui n'apporte rien que `WritePlugin` n'offre déjà (aucun `.mcp.json` écrit). On prend `--plugin-dir`, documenté *[rev2 1]*. Une skill de plugin s'appelle `<plugin>:<skill>`, son nom doit être celui de son dossier.
- `claude plugin validate` ne valide que le manifeste : un `SKILL.md` avec `allowed-tools` ou `hooks` passe. Ce n'est pas un test de notre garantie (§4) *[rev2 2]*.
- Une skill de plugin peut porter, dans son frontmatter, bien plus qu'une consigne : `allowed-tools`, `hooks`, `mcpServers`, `shell`, `model`, `agent`, `context`… Un plugin peut aussi porter `hooks/hooks.json`, `.mcp.json`, `agents/`, `commands/` *[rev. 2]*.
- Un plugin invalide est ignoré **sans erreur** ; mais des réglages gérés (politique d'entreprise) qui interdisent le chargement de plugins font **sortir la CLI en erreur** *[rev. 3]*.
- Le message `system/init` du flux `stream-json` liste les commandes disponibles (`slash_commands`) : une skill de plugin chargée y figure. C'est le moyen de savoir si le plugin a pris *[rev. 9]*.
- **Le `CLAUDE.md` du dépôt cloné n'est pas lu** avec `--setting-sources user` (le chargeur de mémoire ne lit `CLAUDE.md` et `.claude/CLAUDE.md` du projet que sous `projectSettings`) ; le `~/.claude/CLAUDE.md` **est** lu : celui du propriétaire sur une machine, celui que `SeedConfigDir` copie depuis `CLAUDE_CONFIG_DIR` sur un worker. Le `~/.claude/skills/` du propriétaire est déjà chargé sur sa machine *[rev. 8]*.

## 3. Le principe

1. **Une skill marquée pour les runs** : son `SKILL.md` porte `runs: true` (booléen strict : `true` seul) *[rev. 14]*. Seules celles-là partent avec un run.
2. **Les noms voyagent, jamais le contenu** *[rev. 4]* : `LoadSkillsForAgent` (appelé au début de chaque tour et de chaque sous-agent) rend aussi `RunSkills`, les noms des skills `runs` de l'agent ; `AgentWorkflow` les met dans un nouveau champ de `tool.CallContext` (`RunSkills`, réservé d'office comme toute clé de contexte : le modèle ne peut pas le fournir). Le `CallContext` va à tout outil `NeedsCallContext` : quelques noms de plus par appel, assumé (« aucune liste de noms d'outils dans le code » interdit de cibler les deux outils de code). Sous-agent : son propre `LoadSkillsForAgent`. Tâche de fond : le `CallContext` du tour. Tâche planifiée : elle est éligible aux machines (son `UserID` est posé) et emporte donc ses skills `runs`, comme un tour *[rev2 4]*. Les noms ne quittent pas le serveur : le `tool_use` persisté garde l'entrée du modèle, un outil activity ne reçoit le `CallContext` que dans son contexte Go, jamais vers un serveur MCP ; reste l'historique Temporal (≈ 1 Kio par appel). Repli : l'entrée brute, `AnalyzeRepoInput` embarque le `CallContext`.
3. **Celui qui prépare le run lit les skills** et compose ce qu'il faut :
   - **vers une machine** : **`PickMachine`** compose l'entrée finale de la directive (figée dans `machine_directives.input`, que la passerelle relit à l'envoi et au `hello`), pas `RunOnMachine`, qui ne reçoit qu'un ID *[rev. 1]*. `MachineActivities` reçoit un lecteur de skills (celui de `LLMActivities`, `activityDeps.skills`) et ajoute `Skills` (`machine.RunSkill{Name, Description, Content}`) à `AnalyzeInput`/`ImplementInput`, **avant** de réserver la machine. `PickMachineInput` gagne `Skills []string` (le workflow y met `CallContext.RunSkills`) ; un refus (taille, nom invalide) est un **résultat** (`PickMachineOutput.Refused`, comme `NoMachine`), qui mène au repli, jamais une erreur retentée, et ne réserve rien. Un nom introuvable (le worker n'a pas la skill, ou pas de source de skills du tout) est dit dans le résultat (« skills not found: tdd »), jamais en silence. L'idempotence de `PickMachine` sur `(run, appel)` garde l'entrée du premier essai ;
   - **sur un worker Claude Code** : `PrepareWorkspace` (nouveau champ d'entrée : les noms) les lit dans **sa propre** source de skills (`ClaudeCodeActivities` reçoit un lecteur).
4. **La CLI les reçoit pour ce run seulement**, sous forme d'un **plugin local** passé par `--plugin-dir` : `<run>/plugin/.claude-plugin/plugin.json` et `<run>/plugin/skills/<nom>/SKILL.md`, effacé avec le run. Rien dans la configuration de l'utilisateur ni de l'opérateur. Plugin nommé `temporal-agent` (skills appelées `temporal-agent:tdd`) *[rev. 12]*.
5. **Un seul code** dans le paquet `machine` (sans dépendre de `skill`, ni de la base, ni de Temporal ; `GOOS=windows go build ./machine/...` passe) écrit le plugin et ajoute l'option, partagé par `agent connect` et par les activities des workers *[rev. 12]*.
6. **Le prompt système du run nomme les skills** *[vérif.]* : une skill disponible n'est pas une skill consultée (§9 : en analyse, la CLI ne l'a pas ouverte d'elle-même). Quand un run a des skills, une ligne s'ajoute au prompt système d'analyse ou d'implémentation : « The agent that started this run gave it skills: temporal-agent:tdd, … Load each with the Skill tool before you start, and follow them. » Composée par le même code que le plugin (`machine.SkillsPrompt(noms)`), depuis les noms validés seulement, jamais depuis une description ou un corps.

## 4. La garantie sur les droits *[rev. 2]*

Une skill ne doit être qu'une consigne. La CLI, elle, honorerait `allowed-tools`, `hooks`, `mcpServers` d'un frontmatter de skill. D'où la règle : **`WritePlugin` n'écrit jamais le texte source**. Il écrit :
- `plugin.json` qu'il compose lui-même (`name`, `version`, `description`) ;
- pour chaque skill, `skills/<nom>/SKILL.md` avec un frontmatter **qu'il compose** (`name` = le dossier, `description`) suivi du corps ;
- rien d'autre (pas de `hooks/`, `.mcp.json`, `agents/`, `commands/`).

Le magasin actuel ne garde déjà que `name`, `description` et le corps : la garantie tient tant que `WritePlugin` ne réutilise jamais un frontmatter source. Test : un `SKILL.md` source avec `allowed-tools: Bash(*)` et `hooks:` donne un plugin sans l'un ni l'autre. La liste des outils de la CLI reste la nôtre (lecture seule en analyse ; `git add`/`commit`/`status` en implémentation, `Edit(.git/**)` refusé) : une skill ne peut pas l'élargir, ni exécuter un script.

**Noms** *[rev. 7]* : un nom de skill devient un dossier : `WritePlugin` exige `^[a-z0-9][a-z0-9_-]{0,63}$`, dédoublonne, refuse sinon en le disant. (`skills/code_review` a `name: code-review` : c'est le nom du frontmatter qui compte.)

**Confiance** *[rev. 13]* : sur la machine d'un membre, une skill parle à **sa** CLI, avec **son** abonnement et **son** identité git (une skill pourrait demander de lire un fichier et de le commiter, poussé par sa clé : le `git` intermédiaire refuse `-F` hors du clone, mais une consigne reste une consigne). C'est le modèle de confiance retenu (un groupe qui fait confiance à son admin) ; en contrepartie, la visibilité : les noms des skills d'un run sont dits au démarrage du run dans le journal d'`agent connect`, dans le résultat (`ClaudeCodeOutput.Skills`), et un badge « runs » les signale dans `/admin`.

## 5. Ce qui peut empêcher le plugin

- **Politique d'entreprise** *[rev. 3]* : la CLI sort en erreur avant le premier outil. Sur une machine : reconnu (ligne de stderr, pas de résultat) comme un **refus** (`Refusal` → `DirectiveRefused`) : le clone a eu lieu, mais rien n'est payé *[rev2 3]*, le run passe au repli dans le même run, avec la raison dans la note. Sur un worker (réglages gérés de l'opérateur) : échec clair.
- **CLI trop ancienne** *[rev2 1]* : `agent connect` ne vérifie aujourd'hui que la présence de `claude`. Au démarrage, il lit `claude --help` et note si `--plugin-dir` existe (journalisé, et annoncé avec la capacité `claude-code`) ; sans l'option, un run qui a des skills est refusé (`Refusal` → repli), un run sans skills tourne comme aujourd'hui.
- **Plugin ignoré** *[rev. 9]* : le runner lit `slash_commands` du message `init` ; une skill attendue absente est dite dans le résultat (« skill tdd not loaded by the CLI »), le run continue.
- **Taille** *[rev. 6]* : trois bornes, 64 Kio de skills par run et 16 skills au plus : `PickMachine` avant de réserver ; `AnalyzeInput.Check`/`ImplementInput.Check` sur la machine (elle ne se fie pas au serveur pour ce qui la protège, `machines.md` §17.5) ; `WritePlugin` sur un worker de repli.

## 6. La source des skills des conteneurs *[rev. 5]*

Les conteneurs Claude Code n'ont aujourd'hui aucune source de skills. Un `SKILLS_REPO` privé cloné dans un conteneur garderait son jeton dans `.git/config`, lisible par l'utilisateur des runs (`RUN_AS_UID`) : sur `claude-code-ro`, c'est contredire sa promesse (une identité qui ne peut écrire nulle part). Donc :
- **`SKILLS_DIR`** : nouvelle option (magasin `FileStore`, sans secret), honorée par **`agent worker`**, **`agent dev`** (qui code aujourd'hui `./skills` en dur : `SKILLS_DIR` devient son réglage, défaut `./skills`) et **`agent server`** (qui ne lit aujourd'hui que `SKILLS_REPO` : sans lui, `/admin/skills` serait vide et le badge n'existerait pas). `SKILLS_REPO` et `SKILLS_DIR` ensemble = refus au démarrage. Le `FileStore` se recharge comme le dépôt, sur un saut de `skills_version` (`watchSkills`), sinon chaque changement de skill demanderait de redémarrer les conteneurs. Le compose pointe les deux conteneurs sur `/app/skills`, déjà monté en lecture seule *[rev2 1]* ;
- un `SKILLS_REPO` dans un conteneur reste possible pour un dépôt public ; privé, le cache passe dans un dossier 0700 du worker, et la documentation dit que son identité reste lisible par les runs.

## 7. Version *[rev. 10]*

Pas de version épinglée : un run utilise ce que lit celui qui le prépare. Corrigé : sur les workers, seul un saut de `skills_version` (webhook `/webhooks/skills`) recharge le magasin ; en `agent dev`, jamais (il faut redémarrer). Le résultat du run porte le commit du cache (`git rev-parse HEAD` du `GitStore`) quand il y en a un, plutôt que le compteur.

## 8. Ce qui change

- **`skill`** : `Skill.Runs` (frontmatter, booléen strict).
- **Activities** : `LoadSkillsForAgentOutput.RunSkills` ; `tool.CallContext.RunSkills` ; `MachineActivities` et `ClaudeCodeActivities` reçoivent un lecteur de skills ; `PickMachine` compose `Skills` ; `PrepareWorkspaceInput` gagne les noms.
- **`machine`** : `RunSkill`, `Skills` dans les entrées de directive, bornes dans `Check`, `WritePlugin` ; protocole +1 (`skills` absent = aucune, sans compatibilité) *[rev. 16]*.
- **`machine`** (suite) : `SkillsPrompt`, ajouté à `AnalyzeSystemPrompt`/`ImplementSystemPrompt` quand le run a des skills, machine **et** worker.
- **`claudecode`** : `--plugin-dir` ; lecture de `slash_commands` dans `init` ; détection de l'option par `claude --help` (machine).
- **Workers** : le dossier compagnon `<dir>.plugin`, écrit de façon idempotente (`PrepareWorkspace` est retenté), lisible par `RUN_AS_UID`, ajouté au balayage du démarrage (`claude_code_sweep.go`) *[rev. 11]*.
- **`agent connect`** : écrit le plugin dans le dossier du run, l'efface avec lui, journalise les skills.
- **Compose** : `SKILLS_DIR=/app/skills` pour `claude-code` et `claude-code-ro`.
- **`/admin`** : badge « runs » sur une skill.
- **Docs** : `CLAUDE.md`, `README.md`, et un exemple `skills/tdd/SKILL.md` (`runs: true`).

## 9. Vérification réelle, avant de coder

Le binaire a répondu au reste (§2) ; il faut encore un vrai run, minuscule et plafonné, pour : un plugin passé par `--plugin-dir` en mode `plan` et en `acceptEdits`, avec `--setting-sources user`, `--strict-mcp-config`, `--permission-prompts none` : la skill figure-t-elle dans `slash_commands`, et la CLI la consulte-t-elle quand la tâche s'y prête ? Et, pour la question du `CLAUDE.md` d'un dépôt (§2) : `CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD` avec `--add-dir <clone>` le charge-t-il sans les réglages du projet ? Si oui, une règle propre à un dépôt (« ce projet se code en TDD ») pourrait vivre dans son `CLAUDE.md` : chantier à part, à décider avec ce résultat.

**Fait le 10 octobre 2026** : CLI 2.1.280 du conteneur `claude-code-ro`, abonnement, `sonnet`, six appels plafonnés à 0,50 $ (≈ 0,43 $ en tout), dépôt Go jetable avec un `CLAUDE.md` (« commence par MANGUE ») et un plugin `temporal-agent` à une skill `house-report` (« commence par BANANE ; un test d'abord »), options d'un vrai run (`-p`, stream-json, `--permission-prompts none`, `--strict-mcp-config`, `--setting-sources user`, `--no-session-persistence`, prompt sur stdin, prompts système des deux modes, listes d'outils de l'implémentation) :

- **Chargement** : avec `--plugin-dir`, l'`init` liste le plugin (`temporal-agent@inline`, version du manifeste) et la skill dans `slash_commands` et `skills` (`temporal-agent:house-report`), en `plan` comme en `acceptEdits`. Sans lui, rien. L'outil `Skill` est là dans les deux modes.
- **Consultation** : en **`acceptEdits`**, la CLI a appelé `Skill` d'elle-même et suivi la règle TDD (`main_test.go` écrit, commit « Add Sub function with test »). En **`plan`**, elle **ne l'a pas ouverte** : rapport sans BANANE. Avec la ligne du §3.6 dans le prompt système, elle l'a chargée en premier et l'a suivie. D'où le §3.6 : on nomme les skills, on ne compte pas sur la description.
- **`CLAUDE.md` du dépôt** : avec `--setting-sources user` seul, pas chargé (pas de MANGUE), comme lu dans le binaire. Mais la CLI peut le **lire comme un fichier** : en implémentation, elle l'a ouvert, a vu la contradiction avec la skill, a suivi le dépôt et l'a dit dans son rapport. Avec `CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD=1` et `--add-dir <clone>`, il est **chargé** (MANGUE), toujours sans les réglages du projet. Une règle propre à un dépôt peut donc vivre dans son `CLAUDE.md` : chantier à part, non retenu ici (il ferait suivre au run des consignes écrites par quiconque peut pousser sur le dépôt, `--repos` décide déjà lesquels).
- **Divers** : `--add-dir` est variadique et avale un prompt passé en argument ; le runner passe la tâche sur stdin, rien à changer. `--plugin-dir` accepte aussi un `.zip` : non utilisé, `WritePlugin` écrit des fichiers qu'il nomme lui-même. Une commande non permise (`go version` en implémentation) est refusée sans bloquer (`permission_denials`).


## 10. Tests

Unitaires : frontmatter `runs` strict, sélection, `WritePlugin` (arborescence, frontmatter recomposé sans `allowed-tools` ni `hooks`, noms refusés, dédoublonnés, taille), bornes de `Check`, options de la CLI, lecture de `slash_commands`. `RealServer` des machines (CLI factice) : la directive porte les skills (et les garde à la relecture du `hello`) ; la CLI factice reçoit `--plugin-dir` et y trouve le `SKILL.md` ; CLI sans l'option → refus → repli ; politique qui refuse le plugin → refus → repli ; skill absente de `init` dite dans le résultat ; repli sur un worker avec `SKILLS_DIR`. Puis un essai avec la skill `tdd` sur une machine et sur le repli.

## 11. Ce qui est fait, et les écarts

Fait le 10 octobre 2026, branche `feature/run-skills`. Tout ce que décrivent les §3 à §8 et les tests du §10, sauf l'essai avec la vraie CLI (§10, dernière phrase : il paie, il reste à faire à la main).

**Où vit quoi.**
- `skill` : `Skill.Runs` (`runs: true` seul) ; `GitStore` clone dans un cache 0700 et dit son commit (`Version`, lu par `skill.Version`).
- `machine/skills.go` : `RunSkill`, `CheckRunSkills` (16, 64 Kio, nom), `UniqueSkills`, `WritePlugin`, `SkillsPrompt`, `SkillsNotLoaded`, `PluginName`, `SkillNotFound`/`SkillNotLoaded` ; `Skills` dans `AnalyzeInput`/`ImplementInput` (bornées par `Check`), `CodingOutput.SkillsMissing`, `CapRunSkills`, protocole 5.
- `claudecode` : `Params.PluginDirs`, `Result.SlashCommands`, `Runner.SupportsFlag`, `PluginDirFlag`, `PluginRefusal`.
- `store` : `PickRequest.Extra` (`run-skills` pour un run qui a des skills), `MachineLacksError`.
- `activity` : `Prompts.RunSkills` (le lecteur, `RunSkillReader`) et `SetVersion` ; `LoadSkillsForAgentOutput.RunSkills` ; `MachineActivities.Skills` et `PickMachine` (`withSkills`) ; `ClaudeCodeActivities.Skills`, `PrepareWorkspace` (`writePlugin`, `pluginDir`), `RunClaudeCode` (`Skills`), `.plugin` dans le balayage.
- `workflow` : `call.RunSkills` dans `AgentWorkflow` ; `PickMachineInput.Skills` et `Refused` dans `onMachine` ; `PrepareWorkspaceInput.Skills`/`RunClaudeCodeInput.Skills` dans les deux workflows de repli ; `ClaudeCodeOutput.Skills`, `SkillsMissing`, `SkillsVersion` et leurs lignes dans `Summary`.
- `agent connect` : `Coder.PluginDir` (lu par `newCoder`), le plugin dans `<run>/plugin`, refus (CLI ancienne, politique), noms journalisés ; `cmd/agent` : `skillSource` (`SKILLS_DIR`/`SKILLS_REPO`) pour les trois modes ; `/admin` : badge « runs ».

**Écarts et précisions.**
1. *§5, « annoncé avec la capacité `claude-code` ».* Une capacité à part, `run-skills`, annoncée quand Claude Code l'est et que la CLI a `--plugin-dir` ; « Mes machines » la montre. Corrigé à la relecture : `PickMachine` l'**exige** quand le run emporte des skills (`store.PickRequest.Extra`, en plus des capacités de la nature), sinon une machine prioritaire à la CLI ancienne prendrait le run pour le refuser, et une autre machine de l'utilisateur qui pouvait le faire ne le verrait jamais. Aucune n'a tout, mais une aurait les capacités de la nature : `store.MachineLacksError` (un `ErrNoMachine`, dans la même transaction, sans requête de plus : les machines sont déjà lues), dit au modèle (« your machine "X" has Claude Code but its CLI lacks --plugin-dir… », `PickMachineOutput.Lacks`, puis le repli avec cette note). Le refus sur la machine reste comme filet (une CLI changée depuis le démarrage d'`agent connect`).
2. *§6, `agent dev`.* Une seule règle pour les trois modes (`skillSource`) : `SKILLS_DIR`, sinon `SKILLS_REPO`, sinon le dossier du mode (`./skills` pour `agent dev`, aucun pour `server` et `worker`) ; les deux ensemble = refus au démarrage. `agent dev` lit donc un `SKILLS_REPO` posé sans `SKILLS_DIR` (il l'ignorait), toujours sans rechargement (§7). Le bouton de `/admin` s'appelle « Recharger » (un dossier se recharge aussi) et existe dès qu'une source existe.
3. *§3.3, « nom introuvable ».* Le lecteur ne rend que les skills marquées `runs` **là où il lit** : une skill présente mais non marquée y compte comme introuvable. Les noms sont dédoublonnés à la lecture (`Prompts.RunSkills`) et à l'écriture (`WritePlugin`).
4. *§3.3, sur le repli.* Des skills hors bornes font échouer `PrepareWorkspace` sans relance (`InvalidInput`) : le run ne part pas sans elles en silence. Sur une machine, `PickMachine` les refuse avant (`Refused` → repli, qui échoue alors de même, en le disant).
5. *§4, frontmatter recomposé.* `name` et `description` en scalaires entre guillemets (le JSON est du YAML) ; description coupée à 1 024 octets, remplacée par « Skill <nom> of the agent that started this run. » si vide. Manifeste : version fixe `1.0.0`. Le commit des skills va dans le résultat (`SkillsVersion`, « skills at <commit> »), pas dans le manifeste ; il ne vient que d'un `GitStore` (rien pour un dossier).
6. *§5, politique.* Reconnue par la ligne que la 2.1.280 écrit (lue dans le binaire) : « --plugin-dir is disabled by your organization's managed settings (disableSideloadFlags)… », seulement sans résultat de la CLI. Une politique qui laisse charger le plugin mais l'ignore (`strictKnownMarketplaces`…) tombe dans le cas du plugin ignoré (`slash_commands`).

   *Corrigé à la relecture.* Seuls stderr et l'erreur du runner comptent, jamais le rapport (un dépôt qui cite le réglage, un run arrêté en route) ; et un refus n'est un `Refusal` (repli) que si aucun outil n'a tourné (`Progress.ToolCalls == 0`, comme un login refusé), sinon une fin ordinaire, la ligne dans l'erreur : un run qui a travaillé garde son rapport partiel et n'est pas payé deux fois. Même garde sur un worker.
7. *§5, plugin ignoré.* Jugé seulement quand la CLI a rendu son résultat (son `init` est passé) : un run interrompu ne dit rien de ses skills. Sur un worker, le workflow compare (`claudeCodeResult.SlashCommands`, tout l'`init` dans l'historique) ; sur une machine, `agent connect`, qui le met dans `CodingOutput.SkillsMissing`.
8. *§5, CLI trop ancienne.* Vérifiée sur la machine seulement : l'image des workers a la 2.1.280. Une CLI sans l'option sur un worker échoue au premier run avec sa propre erreur (option inconnue), dite comme tout run sans résultat.
9. *Sortie.* En plus de `ClaudeCodeOutput.Skills` : `SkillsMissing` (« nom: raison », introuvable ou non chargée) et `SkillsVersion`, comme `Unpublished` pour les fichiers.
10. *Compose.* `SKILLS_DIR=/app/skills` est dans `docker-compose.yml`, hors du dépôt (le dossier parent), pour `claude-code`, `claude-code-ro` et `agent` (qui ne passe plus `SKILLS_REPO` : un `SKILLS_REPO` ajouté au `.env` ne change rien à `agent dev`) ; à recréer (`docker compose up -d`) pour qu'il compte.
11. *§3.3, `PickMachine` rejoué.* La directive déjà faite par un essai précédent dit ses skills : celles de son entrée, sa version (`skills_version`, mise dans l'entrée à côté de `skills` : `AnalyzeInput`/`ImplementInput.SkillsVersion`, que la machine ignore), et comme introuvables les noms demandés qu'elle ne porte pas ; jamais l'état des skills au moment du nouvel essai. Limite connue : un nouvel essai que les skills, rechargées entre-temps, font refuser (hors bornes) laisse la directive du premier, que le balayage close (`orphaned`, 10 min).
12. *Repli.* `PrepareWorkspace` lit et borne les skills avant le clone (hors bornes : rien de cloné), écrit le plugin après avoir donné le clone à `RUN_AS_UID`. Le cache d'un `GitStore` est remis à 0700 à chaque synchronisation, clone ou pull.

