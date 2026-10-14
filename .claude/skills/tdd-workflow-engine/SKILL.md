---
name: tdd-workflow-engine
description: Execution rules shared by the tdd and tdd-auto agents — the TDD sequence, clean GREEN (minimal behaviour, clean form), violations, early green, PIN execution, tdd-run.sh and the test-first guard, status updates
---

# Skill: TDD Workflow Engine

Execution rules shared by `tdd` and `tdd-auto`. Each agent adds its own gates on top. **What** goes
in the test list, naming, TPP, and the analysis-file format/status vocabulary live in `tdd-analysis`.

## 1. The TDD Sequence (RED)

**RULE A — Completeness.** The whole test (Given, When, Then) is written in ONE Write/Edit call
before any production file is touched, then left untouched until behavioural failure.
**RULE B — Test first.** The test file is the first file written in a RED phase, even when you
already know the classes you will need.

1. **Write the complete test** by wishful thinking: call classes/methods that do not exist yet. The
   test defines the API. One test per RED phase — or one added assertion when the list says
   `merge into › <name>`.
2. **Get the compilation errors.** If `mcp__ide__getDiagnostics` answers for the test file, use it
   (no Maven start). Otherwise `tdd-run.sh XxxTest` (§7) → `COMPILE_ERROR` with the list.
3. **Scaffold in one batch** from the complete error list: empty classes, bare interfaces, methods
   throwing `UnsupportedOperationException` or returning a neutral value (`Optional.empty()`, empty
   list — never `null`). No logic, no constructor parameter the compiler doesn't demand. Include the
   structural files the test cannot start without (Flyway migration, Spring bean wiring).
   Scaffolding **adds members, never statements to an existing body** (V2b).
4. **Run the test class** with `tdd-run.sh`. RED is `RED_ASSERTION`, or `RED_EXCEPTION` thrown by the
   code under test (a domain refusal from a previous green, or the scaffold's
   `UnsupportedOperationException` when the assertion cannot be reached otherwise). No status edit
   on RED: the row stays `⏳ Pending` until it ends.

`COMPILE_ERROR`, `WIRING_ERROR`, `NO_TESTS_RAN` and `INFRA_ERROR` are never RED (§9).
The test-first guard (§8) refuses production writes while the code is GREEN and no test changed.

## 2. GREEN — minimal behaviour, clean code

The cycle is **RED → clean GREEN**. There is no separate refactor step: writing clean code costs an
AI nothing, so the code is never written dirty first. *Minimal* constrains the **behaviour**; *clean*
constrains the **form**. They never trade against each other.

1. **Minimal behaviour.** Before every line: **"does the failing test need this?"** Take the smallest
   TPP step (`tdd-analysis` §2) the test forces; V5–V8 still apply — an unconditional `throw` for a
   refusal baseline is both minimal and clean.
2. **Clean form, now.** Ubiquitous-language names, the right layer (rules in the domain, use case
   orchestrates, ports for I/O and time, no framework in `businesslogic/`), value objects instead of
   primitives, behaviour where the data lives (Tell, Don't Ask), tidy French intention helpers.
3. **Refactor existing code when this test reveals the need.** Duplication and generalization only
   become visible with the second example: the green that exposes them removes them ("make the
   change easy, then make the easy change"). Never edit an existing test's assertions to do so.
4. **No speculative abstraction.** No strategy, interface, generic helper or hierarchy until two
   tests actually need it. Ports required by the doctrine (time, persistence, collaborators) are not
   speculative.
5. **Run the test class once** (`tdd-run.sh XxxTest`): it holds the new test and its neighbours, so it
   is both the GREEN check and the local regression check. `GREEN` → RULE 7.
6. **Discovered behaviour** (a case the list misses, noticed while implementing): don't implement or
   test it — append it as `➕ PROPOSED — <why>` (RULE 9) and carry on.

## 3. When a Clean GREEN Fails — Two-Step Fallback

If the class is still red after GREEN, you cannot tell whether the behaviour or the restructuring is
wrong. Do not stack fixes:

0. Pause the guard: `mv .claude/state/tdd-active .claude/state/tdd-paused` (step 3 edits under green).
1. Revert the restructuring of existing code; keep only the plainest code that passes the new test.
2. Run → green (if still red, the behaviour is wrong: fix it at this plain level).
3. Re-apply the restructuring in small moves, running the class after each; the move that turns it
   red is the culprit — undo that move, not the test.
4. Re-arm: `mv .claude/state/tdd-paused .claude/state/tdd-active`.

This is the only time the cycle splits into GREEN then refactor.

## 4. Over-Implementation (GREEN violations)

- **V5 — enum values** not asserted by the failing test → remove them.
- **V6 — domain events machinery** (event list, event classes, publication) with no test asserting an
  event → remove it. Snapshots are mandatory; events are not.
- **V7 — methods/classes** not called by the failing test → remove them.
- **V8 — premature conditional.** A refusal baseline ("refuse a restricted patient", RG6) that never
  supplies the success case passes with an **unconditional** `throw new PatientRestrictedException();`.
  Writing `if (patient.isRestricted())` now robs the next test (a non-restricted patient books) of
  its RED. Collapse the branch; the next test brings it back through a real RED.

## 5. RED Violations and Early Green

- **V0 — production code before the test** (zero tolerance): before each Write/Edit in RED ask "is
  this the test file?". If not and the test isn't written → stop, write the test.
- **V0b — several tests in one RED** → keep the next one in the list, delete the others.
- **V0c — interleaving** (test edited after a production file in the same RED) → revert the
  scaffold, fix the test, restart at step 2.
- **V1 — missing class/method after the test exists** → scaffold (step 3).
- **V2 — logic before RED** → back to scaffold state, re-run.
- **V2b — behaviour smuggled in as scaffold:** a body that existed before this RED gained a statement
  (a call, an assignment, a branch — e.g. `publisher.publish(event)` inside an existing use case) →
  revert it, re-run for the behavioural RED, re-add it in GREEN. It fakes an early green.
- **V3 — pausing mid-RED** ("should I continue?") → don't; finish the RED phase.
- **V4 — EARLY GREEN:** the new test passes (`GREEN`) at step 4 with no new production code. First
  rule out V2b. A test that passes on write drove nothing, so it is **not kept as driven** (user
  decision) — with one exception, the predicted pin of a threshold's second side (3):
  1. Write **no** production code, no status edit yet.
  2. Dispositions: **strengthen** (weak assertion → rewrite it, back to RED), **reorder** (premature →
     move the row later or insert a precursor), otherwise **delete** (test and row removed,
     `Dropped: <name> — early green: <what already satisfies it>` in Design Notes) — RULE 7b.
  3. **Predicted** (the row ends with `(if early green: delete — …)`): delete without asking.
     **Predicted pin** (`(if early green: pin — <mutation>)`): prove it without asking, as §6 steps 2–3
     (pause the guard, apply the mutation, the class must go `RED_*`, revert, re-arm, `GREEN`) →
     `🧬 PINNED — <mutation>`. The mutation does not turn it red → delete it (`Dropped: <name> —
     early green: no killing mutation`). Report it at CYCLE_COMPLETE with the deletions.
     **Unpredicted:** `tdd` asks immediately (EARLY_GREEN_GATE); `tdd-auto` applies the disposition
     itself (strengthen or reorder only when clearly right, else delete) and lists every deletion,
     with the assertion that passed, at CYCLE_COMPLETE. No other variation.

## 6. Characterization (PIN) Execution

For analysis files with `**Mode:** CHARACTERIZATION` (production code must not change):

1. Write the test (RULE A/B). Run the class: it **should pass**. If it fails, the list misread the
   code → delete the test and its row, add `Not pinned: <name> — contradicts current behaviour` to
   Design Notes, report it; never change production code to make it pass.
2. Pause the guard (`mv .claude/state/tdd-active .claude/state/tdd-paused`), apply the row's
   **Mutation** to production code, run the class: the test must go `RED_*`.
3. Revert the mutation (`git diff` on production files must be empty again), re-arm the guard, run
   the class: `GREEN`.
4. Status `🧬 PINNED — <mutation>`. If no mutation turns it red → delete the test and its row, add
   `Not pinned: <name> — no killing mutation` to Design Notes.

No GREEN in this mode.

## 7. Running Tests

**Always run tests through `.claude/scripts/tdd-run.sh`** (from the repo root): it runs Maven quietly,
prints one classified status line plus only the failing lines, keeps the full log in
`.claude/state/last-run.log`, and records the outcome the test-first guard reads. Raw `mvn` output is
pure token cost and bypasses the guard.

| Scope | Command |
|---|---|
| One class (unit `*Test`, integration `*IT`, e2e `*E2ETest`) | `.claude/scripts/tdd-run.sh XxxTest` |
| Whole unit suite | `.claude/scripts/tdd-run.sh --unit` |
| Everything (`mvn verify`, needs Docker) | `.claude/scripts/tdd-run.sh --all` |
| Same, replayed without running when the source tree is unchanged since the last green `--all` | `.claude/scripts/tdd-run.sh --verify` |

What it runs underneath, for reference (`<context>` is inferred from the test class location; `--unit` uses `TDD_MODULE`, default `betting`):

| Test type | Scope | Command | Docker |
|---|---|---|---|
| Unit (`**/unit/**/*Test.java`) | one class | `mvn -q -pl boundedcontexts/<context> test -Dtest=XxxTest` | No |
| Unit | whole suite | `TDD_MODULE=<context> tdd-run.sh --unit` → `mvn -q -pl boundedcontexts/<context> test` | No |
| Integration (`**/*IT.java`) | one class | `mvn -q -pl boundedcontexts/<context> verify -Dit.test=XxxIT -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false` | Yes |
| E2E (`**/e2e/**/*E2ETest.java`) | one class | `mvn -q -pl boundedcontexts/<context> verify -Dit.test=XxxE2ETest -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false` | `fake`: No · `jpa`: Yes |
| Everything | full build | `mvn -q verify` | Yes |

- A test file with any other name **silently never runs**: check the suffix before trusting a green.
- `fake`/`jpa`/`dev`/`test` are **Spring** profiles (`@ActiveProfiles`), never `-P`. The only Maven
  profile is `pitest`: `mvn -q -pl boundedcontexts/<context> -Ppitest test` (unit tests only; surviving mutants are a
  quality signal, not a gate).
- **Budget per test:** diagnostics (or 1 run) → scaffold → 1 run (RED) → implement → 1 run (clean
  GREEN) → 1 status Edit. A failing clean GREEN costs extra runs only through §3. The **whole relevant suite runs once** at
  CYCLE_COMPLETE, not after every test.

## 8. Rules, Status Updates and the Guard

Status values: `tdd-analysis` §7. Updates are **Edit calls on the analysis file**
(`workspace/current/tdd-iteration-XX.md` for the current iteration if it exists, else
`workspace/current/tdd.md`), done before any reporting. Only the rules that are checked or were
broken in evaluations carry a number:

| Rule | Description |
|------|-------------|
| **RULE 0** | Test before production code (§1). Mechanically guarded. |
| **RULE 7** | On GREEN after a real RED: `⏳ Pending` → `✅ GREEN`. One status Edit per test. |
| **RULE 7b** | On early green: no production code; strengthen/reorder keeps `⏳ Pending`, delete removes the test and its row and adds `Dropped: <name> — early green: <reason>` to Design Notes, a predicted pin whose mutation kills it becomes `🧬 PINNED — <mutation>` (§5 V4). Never `✅ GREEN` for that test. |
| **RULE 9** | **Discovered tests:** anything noticed but not tested (a missing case, an unguarded `orElseThrow`, a question in your report) becomes a `➕ PROPOSED — <why>` row at the end of the table — never code, never only prose. Presented at CYCLE_COMPLETE (approved → `⏳ Pending`, rejected → row deleted). |
| **RULE C** | **Files through Write/Edit only** — tests, production code, analysis file. Never Bash (heredocs, `python3 -`, `sed -i`, `cp`, `mv`, `git checkout --`). |

Also binding, unnumbered: RED is `RED_*` only (§1); scaffold only after the complete test and never edit
the test while scaffolding (§1); follow the test type's pattern skill (E2E goes through HTTP only);
unit tests go through a use case (or a domain service), never an aggregate or value object alone.

**Resume.** The first `⏳ Pending` row whose test method already exists → run its class: `RED_*` →
continue at GREEN; `GREEN` → the previous run was cut after its green: mark `✅ GREEN` and say
"resumed" in the report.

**Test-first guard.** `.claude/hooks/test-first-guard.sh` (in `.claude/settings.json`) acts only while
`.claude/state/tdd-active` exists — the agents create it at start and delete it when the list is done:
- a Write/Edit under `src/main/` is refused while the last `tdd-run.sh` outcome is `GREEN` (or no run
  happened yet) and no test file changed since;
- a Bash command that writes under `src/` is refused (RULE C) — judged per command segment, so
  reading a source file and redirecting elsewhere is fine;
- after every Bash command, a file under `src/` that changed anyway is reported back as a bypass.
Refusals and bypasses are logged to `.claude/state/guard.log`. A refusal means "write the test first"
— never work around it. The only legitimate pause is §3 and §6 (`tdd-active` → `tdd-paused`, then back).

**Pattern skills are not preloaded.** Once the test type is known, **Read exactly one** of
`.claude/skills/tdd-core-patterns/SKILL.md` (unit), `.claude/skills/tdd-e2e-patterns/SKILL.md`
(e2e), `.claude/skills/tdd-integration-patterns/SKILL.md` (integration) — once per run.

## 9. Error Recovery

A Maven crash, OOM, Docker/Testcontainers timeout or port clash is **not** RED.
1. Diagnose from `tdd-run.sh`'s lines, then `.claude/state/last-run.log` if they are not enough.
2. Transient (Docker starting, file lock) → retry once.
3. Persistent → report it and STOP; don't fix infrastructure configuration on your own.
4. The TDD state does not change.

## 10. Agent Memory

After CYCLE_COMPLETE (or after an analysis), record only what the repo does not already say:
non-obvious conventions discovered, traps hit and how they were solved, business arbitrations the
user made (with date). Don't record file lists, fake names or anything a Grep finds in seconds.
Before relying on a memory, check that the class/file it names still exists.
