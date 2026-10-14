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
already know the types you will need.

1. **Write the complete test** by wishful thinking: call types, functions and methods that do not exist yet. The
   test defines the API. One test per RED phase — or one added assertion when the list says
   `merge into › <name>`.
2. **Get the compilation errors.** If `mcp__ide__getDiagnostics` answers for the test file, use it
   (no Go run). Otherwise `tdd-run.sh <package> <Test>` (§7) → `COMPILE_ERROR` with the list.
3. **Scaffold in one batch** from the complete error list: empty structs, bare interfaces, functions and
   methods returning zero values (`(T{}, false, nil)`, a nil slice, `nil` error) — or
   `panic("not implemented")` when the assertion cannot be reached otherwise. No logic, no field or
   parameter the compiler doesn't demand. Include the structural files the test cannot start without
   (the adapter's table migration, the fake's compile-time port assertion).
   Scaffolding **adds members, never statements to an existing body** (V2b).
4. **Run the test** with `tdd-run.sh <package> <Test>`. RED is `RED_ASSERTION` (a `t.Error`/`t.Fatal`:
   a wrong value, or an unexpected error from a previous green's refusal), or `RED_EXCEPTION`, a panic in
   the code under test (the scaffold's `panic("not implemented")` when the assertion cannot be reached
   otherwise). No status edit
   on RED: the row stays `⏳ Pending` until it ends.

`COMPILE_ERROR`, `WIRING_ERROR`, `NO_TESTS_RAN` and `INFRA_ERROR` are never RED (§9).
The test-first guard (§8) refuses production writes while the code is GREEN and no test changed.

## 2. GREEN — minimal behaviour, clean code

The cycle is **RED → clean GREEN**. There is no separate refactor step: writing clean code costs an
AI nothing, so the code is never written dirty first. *Minimal* constrains the **behaviour**; *clean*
constrains the **form**. They never trade against each other.

1. **Minimal behaviour.** Before every line: **"does the failing test need this?"** Take the smallest
   TPP step (`tdd-analysis` §2) the test forces; V5–V8 still apply — an unconditional `return ErrX` for a
   refusal baseline is both minimal and clean.
2. **Clean form, now.** Ubiquitous-language names, the right layer (rules in the domain, use case
   orchestrates, ports for I/O and time, nothing of Temporal, SQL or HTTP in `domain/` and `usecase/`),
   value objects instead of primitives, behaviour where the data lives (Tell, Don't Ask), tidy
   intention helpers (`.claude/rules/DOCTRINE.md`).
3. **Refactor existing code when this test reveals the need.** Duplication and generalization only
   become visible with the second example: the green that exposes them removes them ("make the
   change easy, then make the easy change"). Never edit an existing test's assertions to do so.
4. **No speculative abstraction.** No strategy, interface, generic helper or hierarchy until two
   tests actually need it. Ports required by the doctrine (time, persistence, collaborators) are not
   speculative.
5. **Run the test's package once** (`tdd-run.sh <package>`): it holds the new test and its neighbours, so it
   is both the GREEN check and the local regression check. `GREEN` → RULE 7.
6. **Discovered behaviour** (a case the list misses, noticed while implementing): don't implement or
   test it — append it as `➕ PROPOSED — <why>` (RULE 9) and carry on.

## 3. When a Clean GREEN Fails — Two-Step Fallback

If the package is still red after GREEN, you cannot tell whether the behaviour or the restructuring is
wrong. Do not stack fixes:

0. Pause the guard: `mv .claude/state/tdd-active .claude/state/tdd-paused` (step 3 edits under green).
1. Revert the restructuring of existing code; keep only the plainest code that passes the new test.
2. Run → green (if still red, the behaviour is wrong: fix it at this plain level).
3. Re-apply the restructuring in small moves, running the package after each; the move that turns it
   red is the culprit — undo that move, not the test.
4. Re-arm: `mv .claude/state/tdd-paused .claude/state/tdd-active`.

This is the only time the cycle splits into GREEN then refactor.

## 4. Over-Implementation (GREEN violations)

- **V5 — enum values** not asserted by the failing test → remove them.
- **V6 — domain events machinery** (event list, event types, publication) with no test asserting an
  event → remove it. Snapshots are mandatory; events are not.
- **V7 — functions, methods, types** not called by the failing test → remove them.
- **V8 — premature conditional.** A refusal baseline ("refuse an agent closed to the turn's user",
  DR13) that never supplies the success case passes with an **unconditional**
  `return TurnToolsResult{}, domain.ErrAgentNotOpen`. Writing `if !domain.Allowed(user, agent, rules)` now
  robs the next test (an open agent answers) of its RED. Collapse the branch; the next test brings it back through a real RED.

## 5. RED Violations and Early Green

- **V0 — production code before the test** (zero tolerance): before each Write/Edit in RED ask "is
  this the test file?". If not and the test isn't written → stop, write the test.
- **V0b — several tests in one RED** → keep the next one in the list, delete the others.
- **V0c — interleaving** (test edited after a production file in the same RED) → revert the
  scaffold, fix the test, restart at step 2.
- **V1 — missing type/function after the test exists** → scaffold (step 3).
- **V2 — logic before RED** → back to scaffold state, re-run.
- **V2b — behaviour smuggled in as scaffold:** a body that existed before this RED gained a statement
  (a call, an assignment, a branch — e.g. `alerts.Send(ctx, alert)` inside an existing use case) →
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
     (pause the guard, apply the mutation, the package must go `RED_*`, revert, re-arm, `GREEN`) →
     `🧬 PINNED — <mutation>`. The mutation does not turn it red → delete it (`Dropped: <name> —
     early green: no killing mutation`). Report it at CYCLE_COMPLETE with the deletions.
     **Unpredicted:** `tdd` asks immediately (EARLY_GREEN_GATE); `tdd-auto` applies the disposition
     itself (strengthen or reorder only when clearly right, else delete) and lists every deletion,
     with the assertion that passed, at CYCLE_COMPLETE. No other variation.

## 6. Characterization (PIN) Execution

For analysis files with `**Mode:** CHARACTERIZATION` (production code must not change):

1. Write the test (RULE A/B). Run the package: it **should pass**. If it fails, the list misread the
   code → delete the test and its row, add `Not pinned: <name> — contradicts current behaviour` to
   Design Notes, report it; never change production code to make it pass.
2. Pause the guard (`mv .claude/state/tdd-active .claude/state/tdd-paused`), apply the row's
   **Mutation** to production code, run the package: the test must go `RED_*`.
3. Revert the mutation (`git diff` on production files must be empty again), re-arm the guard, run
   the package: `GREEN`.
4. Status `🧬 PINNED — <mutation>`. If no mutation turns it red → delete the test and its row, add
   `Not pinned: <name> — no killing mutation` to Design Notes.

No GREEN in this mode.

## 7. Running Tests

**Always run tests through `.claude/scripts/tdd-run.sh`** (from the repo root): it runs `go test -json`
(Go is not installed on the host: in the `agent-dev` container, or through the build compose file), prints
one classified status line plus only the failing lines, keeps the full log in `.claude/state/last-run.log`,
and records the outcome the test-first guard reads. Raw `go test` output is pure token cost and bypasses
the guard. A target is a **package**, plus a **test name** to run that test alone.

| Scope | Command |
|---|---|
| One test (any level) | `.claude/scripts/tdd-run.sh ./access/usecase TestTurnTools_ClosedToolsAreLeftOut` |
| One row of a table test | `.claude/scripts/tdd-run.sh ./access/domain "TestAllowed_EveryRuleIsALock/a seller cannot update it"` |
| One package (the test and its neighbours) | `.claude/scripts/tdd-run.sh ./access/usecase` |
| A test, its package found by its declaration | `.claude/scripts/tdd-run.sh TestTurnTools_ClosedToolsAreLeftOut` |
| The module's unit suite (all but `adapters/`) | `TDD_MODULE=access .claude/scripts/tdd-run.sh --unit` (default module: `access`) |
| Everything (`go vet ./...` + `go test ./...`, ~30 s) | `.claude/scripts/tdd-run.sh --all` |
| Same, replayed without running when the source tree is unchanged since the last green `--all` | `.claude/scripts/tdd-run.sh --verify` |

What it runs underneath, for reference (always `-json -count=1`, a test name anchored `^Name$`):

| Level | Package | Command | Infrastructure |
|---|---|---|---|
| Unit | `<module>/domain`, `<module>/usecase` | `go test -run '^TestX$' ./<module>/usecase` | No |
| Unit | whole module | `go test <every package of ./<module>/... but adapters/>` | No |
| Integration | `<module>/adapters/postgres` | same, with `TEST_DATABASE_URL` (a throwaway database) | PostgreSQL |
| Integration | `<module>/adapters/temporalio` | same (the SDK's `testsuite`, in process) | No |
| E2E | `<module>/adapters/httpapi` | same (`httptest`) | No |
| Everything | repository | `go vet ./... && go test ./...` | none: tests needing it skip |

- An integration test without `TEST_DATABASE_URL` skips: `tdd-run.sh` answers `INFRA_ERROR`, never GREEN.
  The procedure for a throwaway database is in the header of `tdd-run.sh` and in
  `tdd-integration-patterns`; the `RealServer` smoke tests are never run by an agent.
- A function that is not `func TestXxx(t *testing.T)` in a `_test.go` file **silently never runs**; a test
  name that matches nothing is `NO_TESTS_RAN`. Check the name before trusting a green.
- There is no mutation-testing tool in the build: a mutation (engine §6, §5 V4) is applied by hand.
- **Budget per test:** diagnostics (or 1 run) → scaffold → 1 run (RED) → implement → 1 run of the
  package (clean GREEN) → 1 status Edit. A failing clean GREEN costs extra runs only through §3. The **whole relevant suite runs once** at
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
| **RULE 9** | **Discovered tests:** anything noticed but not tested (a missing case, an error ignored or a `(T, bool)` not checked, a question in your report) becomes a `➕ PROPOSED — <why>` row at the end of the table — never code, never only prose. Presented at CYCLE_COMPLETE (approved → `⏳ Pending`, rejected → row deleted). |
| **RULE C** | **Files through Write/Edit only** — tests, production code, analysis file. Never Bash (heredocs, `python3 -`, `sed -i`, `cp`, `mv`, `git checkout --`). |

Also binding, unnumbered: RED is `RED_*` only (§1); scaffold only after the complete test and never edit
the test while scaffolding (§1); follow the test type's pattern skill (E2E goes through HTTP only);
unit tests go through a use case (or a domain service), never an aggregate or value object alone.

**Resume.** The first `⏳ Pending` row whose test function already exists → run it: `RED_*` →
continue at GREEN; `GREEN` → the previous run was cut after its green: mark `✅ GREEN` and say
"resumed" in the report.

**Test-first guard.** `.claude/hooks/test-first-guard.sh` (in `.claude/settings.json`) acts only while
`.claude/state/tdd-active` exists — the agents create it at start and delete it when the list is done:
- a Write/Edit of a production `.go` file (not `_test.go`) is refused while the last `tdd-run.sh` outcome
  is `GREEN` (or no run happened yet) and no test file (`*_test.go`, `testdata/`) changed since;
- a Bash command that writes a Go file is refused (RULE C) — `gofmt -w`, `go fmt`, `go generate`
  included; judged per command segment, so reading a source file and redirecting elsewhere is fine;
- after every Bash command, a Go file that changed anyway is reported back as a bypass.
Refusals and bypasses are logged to `.claude/state/guard.log`. A refusal means "write the test first"
— never work around it. The only legitimate pause is §3 and §6 (`tdd-active` → `tdd-paused`, then back).

**Pattern skills are not preloaded.** Once the test type is known, **Read exactly one** of
`.claude/skills/tdd-core-patterns/SKILL.md` (unit), `.claude/skills/tdd-e2e-patterns/SKILL.md`
(e2e), `.claude/skills/tdd-integration-patterns/SKILL.md` (integration) — once per run.

## 9. Error Recovery

A container that is not running, a module download that fails, a database that does not answer, a test
timeout (a hang), or a target whose tests all skip is **not** RED (`INFRA_ERROR`).
1. Diagnose from `tdd-run.sh`'s lines, then `.claude/state/last-run.log` if they are not enough.
2. Transient (container starting, network) → retry once.
3. Persistent → report it and STOP; don't fix infrastructure configuration on your own.
4. The TDD state does not change.

## 10. Agent Memory

After CYCLE_COMPLETE (or after an analysis), record only what the repo does not already say:
non-obvious conventions discovered, traps hit and how they were solved, business arbitrations the
user made (with date). Don't record file lists, fake names or anything a Grep finds in seconds.
Before relying on a memory, check that the type/file it names still exists.
