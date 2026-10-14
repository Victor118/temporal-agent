# Temporal Agent — ce que fait la plateforme

Inventaire fonctionnel, écrit le 14 octobre 2026 à partir du code et des conceptions (`docs/design/`), pour
reprendre le projet en TDD · DDD, contexte par contexte. Langage métier seulement : ni Temporal, ni
PostgreSQL, ni HTTP. Format repris de la formation (`README-par-lots.md` de WealBet).

## Le produit

Une plateforme d'assistants IA **auto-hébergée** pour un groupe de personnes qui font confiance à son
administrateur (une équipe, une association, des amis) : pas de SaaS multi-clients.

Les membres discutent dans des **sessions** partagées avec des **agents**. Chaque agent a sa
personnalité, ses consignes et ses **outils** : chercher sur le web, lire et écrire des fichiers, produire
un PDF, appeler un service externe branché par un **serveur MCP**, déléguer à un autre agent, analyser ou
modifier un dépôt de code. Un agent peut travailler en tâche de fond, être planifié, poser une question à
l'utilisateur. Une session peut être **forkée** pour creuser un sujet à part, puis rapporter au parent.

Les membres peuvent prêter leur propre **machine** : le code s'y exécute avec leurs identifiants et leur
abonnement, et le modèle peut y tourner, plutôt que sur le serveur de l'installation.

L'administrateur gère les comptes, les agents et les **droits** : qui peut se servir de quel outil.

## Les contextes

Chaque contexte a son fichier, son glossaire et ses règles, préfixées par son sigle. Une règle garde son
numéro même si son texte change ; une règle supprimée laisse son numéro vide.

| Sigle | Contexte | Ce qu'il couvre | Fichier |
|---|---|---|---|
| **DR** | Droits | Qui peut se servir de quel outil et de quel agent | [droits.md](droits.md) |
| CV | Conversations | Sessions, membres, messages, qui répond, ordre de lecture, arrêter, forks, rapports | *à écrire* |
| AG | Agents | Persona, consignes, skills, liste d'outils, délégation à un autre agent | *à écrire* |
| OU | Outils | Outils intégrés, serveurs MCP, outils sensibles ou privés, fichiers publiés, documents | *à écrire* |
| TF | Travail en arrière-plan | Tâches de fond, tâches planifiées, questions à l'utilisateur | *à écrire* |
| MA | Machines | Inscription, confiance, choix d'une machine, analyse et implémentation de code, modèle sur la machine | *à écrire* |
| CO | Comptes | Utilisateurs, rôles, connexion, désactivation, mémoire d'un utilisateur | *à écrire* |
| CA | Canaux | Web, Telegram : ce qui entre, ce qui sort | *à écrire* |

## Conventions

| Marque | Sens |
|---|---|
| *(à confirmer)* | Définition déduite des règles ou du code, à valider en atelier |
| ⚠ **à requestionner** | Règle telle que le code la fait aujourd'hui, mais qui semble née d'une contrainte technique ou d'une improvisation plutôt que d'un choix métier. À trancher en atelier (`/bdd-spec`) avant de la réimplémenter |
| `code :` | En fin de fichier, où la règle vit aujourd'hui : pour la reprise, jamais une consigne de conception |

Dans un contexte, les règles sont regroupées en **lots** livrables dans l'ordre, du chemin nominal qui porte
le plus de valeur jusqu'aux cas limites.

## Du fichier de règles au code

1. Choisir un lot, écrire la demande (`workspace/<demande>.md` : contexte, règles concernées, décisions
   déjà prises, ce qui reste à éclaircir).
2. `/bdd-spec <demande> <nb questions>` : atelier, spécification par l'exemple.
3. `/iterate` : itérations, `tdd-analyze` puis `tdd-auto`.
4. Le domaine écrit en TDD est pur ; le code actuel (workflows, activités, base, HTTP) est rebranché dessus
   comme adaptateur, et son ancien code métier disparaît.

Le kit de la formation est adapté à Go dans `.claude/` (guide : `.claude/README.md`) : `tdd-run.sh`, les
gardes, les skills de patterns et la doctrine (`.claude/rules/DOCTRINE.md`). Son évaluation reste à refaire
pour Go.
