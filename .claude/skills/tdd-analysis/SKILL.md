---
name: tdd-analysis
description: Test-list rules shared by tdd-analyze, tdd and tdd-auto — test naming, TPP, granularity, the A0–A6 analysis pipeline, characterization (PIN) mode, test type detection and the analysis-file contract
---

# Skill: TDD Analysis

Single source for **what goes in a test list and in which order**. `tdd-analyze` runs the pipeline
below; `tdd` and `tdd-auto` run it only when they receive a requirement without a list. Execution
rules (RED, clean GREEN, violations, commands) live in `tdd-workflow-engine`.

## 1. Test Naming — Final Label, First Implementation (FLFI)

The name states the **final** business behaviour from day one; only the implementation is
progressive. A name that will have to change later is a wrong name.

**Convention (user decision, overrides `should_…_when_…` in CODE-STYLE.md):**
- Test methods in **short French**: `devrait_[résultat]`, adding `_quand_[condition]` **only** when
  the result alone does not tell two tests apart. No accents, no apostrophes in identifiers.
- Given-When-Then intention helpers in French (`unPatientRestreint()`, `quandIlReserve(...)`,
  `alorsLeRendezVousEst(...)`). Production classes and methods stay **English**.
- Carry only the **discriminating** behaviour. The complete expected state is the assertion's job
  (snapshot); do not recite it in the name.
- Business language only: no "exception", "null", "HTTP 409", "Optional". A domain expert must
  understand it. The e2e **Contradiction** column may name statuses; the label may not.

| ❌ | ✅ |
|---|---|
| `devrait_lever_une_exception` | `devrait_refuser_la_reservation_d_un_patient_restreint` (RG6) |
| `devrait_marcher_quand_valide` | `devrait_reserver_les_creneaux_consecutifs_du_motif` (RG2, RG4) |
| `devrait_creer_un_rdv_de_30_min_avec_statut_confirme_et_3_creneaux` | `devrait_reserver_trois_creneaux_pour_un_motif_de_30_minutes` |

Cite the rule(s) a test proves (`RG14`) in the list's **Contradiction** column, and in commits.

## 2. Transformation Priority Premise (TPP)

| Priority | Transformation | Description |
|----------|---------------|-------------|
| 1 | {} → nil | No code → return nothing |
| 2 | nil → constant | Return a hard-coded value |
| 3 | constant → variable | Replace constant with variable/parameter |
| 4 | unconditional → conditional | Add branching |
| 5 | scalar → collection | Single value → list |
| 6 | statement → recursion | Recursive call |
| 7 | selection → iteration | Conditional → loop |
| 8 | value → mutated value | Transform existing value |

TPP is an **ordering heuristic**, not a column to fill: order tests so each needs the **smallest
next transformation**. No loop, collection or recursion before a test forces it; with collections,
start empty or single-element. In sociable use-case tests most steps are "add a branch" — writing
`nil → constant (2)` on every row is decoration. Note `(TPP: …)` in the Contradiction cell only when
it settles a real choice (conditional vs loop, scalar vs collection, why this test comes first).

**Refusal baselines are unconditional.** "Refuse when X" never supplies the success case, so its
green is an unconditional `throw`. Describe it as *always refuses*; the conditional belongs to the
later test that contradicts it (engine V8) — unless an existing test already supplies the success
case, in which case the refusal is conditional at once.

**Obvious implementation beats triangulation.** When the general code is as simple as the constant
(pass a parameter through, read a field), the first test already asks for it — do **not** add a second
example whose only job is to defeat a hard-coded value (e.g. "same booking at 14:30"). Triangulate only
when the generalization is not obvious from one example (a formula, a loop, a rule whose shape one
example cannot reveal).

**Contradiction-driven sequencing — the column that matters.** Test 1 is the simplest baseline.
Each later test must be impossible to pass with the previous green — that impossibility is written in
the **Contradiction** column. If you cannot write it, the test does not belong in the list.

## 3. Test List Granularity

TPP orders *transformations*, not tests. A test earns a line only if it will still be in the suite
when the feature is done. One RED may cross two rungs when both are one design decision.

For candidate tests A then B:
1. **Deletion test (decisive).** If the plan can already say "A is deleted once B is green", do not
   write A. A test scheduled for deletion is scaffolding, not a final label.
2. **Subset test.** Same Given, same When, `assert(A) ⊂ assert(B)` → one test (B's name and
   assertion). Assertions that can fail independently → two tests.
3. **Counting test (against over-merging).** Count the independent production decisions the green
   makes. One decision → one test however many fields it touches; several decisions that could each
   be wrong alone → split.
4. **Merge test.** A candidate whose scenario (fakes, Given, When) an **existing test** already runs
   becomes an extra assertion in that test, not a near-duplicate method (write `merge into › <name>`
   in the Contradiction cell).

*Split on reasons to fail, not on step size — and never write a test you already plan to delete.*

**V9 — Over-split list.** A test whose assertion is a strict subset of the next one's (same
Given/When), or whose deletion is announced → merge into the later test, renumber, re-check TPP.

## 4. Requirement Analysis Pipeline (A0–A6)

Mandatory for `tdd-analyze`; mandatory for `tdd`/`tdd-auto` when no list arrived with the
requirement (it is then the only analysis the cycle gets). Skip it entirely when a list exists.

- **A0 TYPE** — detect the test type (§6). Vague requirement → ask **business** questions and stop.
  Several use cases → propose one analysis per use case.
- **A1 EXPLORE** (never skip, keep it scoped) — in the feature area only:
  - existing test names: `Grep` pattern `void devrait_` (or `@Test`) with `output_mode: content`
    restricted to `boundedcontexts/<context>/src/test/java/**/<feature>/**` (`<context>`: `appointments` or `reliability`, from the RG — `CLAUDE.md`) — names only, don't Read whole test files;
  - aggregates, value objects, ports and `Fake…` in `businesslogic/models/<aggregate>/`; use cases in
    `businesslogic/usecases/`; refusal style (domain exceptions vs result type); `DateProvider`-like
    clock port; fixtures/helpers.
  - Read a production file only when its content decides whether a candidate test can fail, and the
    target test class when a candidate may merge into an existing scenario (§3.4). A claim about
    existing code written in the analysis is quoted from the file, not recalled.
- **A2 DECOMPOSE** — one atomic business rule = one candidate. Order of discovery: simplest happy
  path → refusals/preconditions → edges → implicit rules → combinations. Cohesive assertions about
  one behaviour stay in one test. **Only the rules asked for**: structural invariants (existence of
  referenced entities) are in; protective rules nobody requested are not — mention them in Design
  Notes in one sentence. No test on an aggregate or value object alone: domain changes are driven
  through the use case.
  **Time-bound rules** (RG14–RG17, RG20–RG22, RG24–RG25): pin the threshold with the **edge values
  themselves** — one test exactly at the threshold, one a minute inside it (e.g. RG14: booking
  exactly 2h before is accepted; 1h59 before is refused). Decide inclusivity from the rule's wording
  and say so. The clock is always the fake, set in the Given.
- **A3 DEDUPLICATE — the new-behaviour gate.** For each candidate, name the production line its RED
  will force. Drop any candidate an existing test already proves, or current production code already
  satisfies. Keep a test only if it **can fail against at least one implementation the previous green
  could legitimately have produced** — a hard-coded value that the obvious implementation would never
  write does not count (§2). No paranoid regression guards, no design-locking tests, no outer-layer
  re-assertion of a behaviour that crosses no untested production code — except in characterization
  mode (§5). Each drop is listed in Design Notes (`- Dropped: <candidate> — <reason>`, plain text),
  and a design choice left uncovered is stated there too: it rests on the code and the review, not on
  the suite.
  **Predicted early green:** when a kept test may pass depending on how the previous green is
  written, append a suffix to its Contradiction cell. The **second side of a threshold** (the edge
  value a legitimate implementation could put on the wrong side: `isBefore` vs `!isAfter`) gets
  `(if early green: pin — <mutation>)`, naming the mutation that must turn it red: it is the only
  test that proves the rule's inclusivity, so it stays if the mutation kills it (engine §5). Any other
  predicted early green gets `(if early green: delete — <why>)`. Either way the executor acts without
  a question.
- **A4 ORDER** — TPP + contradiction ordering (§2), then granularity checks (§3).
- **A5 LABEL** — FLFI names (§1).
- **A6 WRITE** — write the analysis file (§7), then show it. More than ~8 tests → propose a split
  into sub-requirements of 3–5 tests.

Do not prescribe implementation ("add an if", "add a field") — the Contradiction column says what
the previous green **cannot do**, not how to fix it.

## 5. Characterization Mode (PIN)

Triggered by `CHARACTERIZATION:` in the request (the orchestrator's PIN iterations). The production
code exists and **must not change**; the goal is to pin behaviours a coming refactor will move.

- A3 is inverted: keep exactly the behaviours **no existing test proves** (the listed gaps).
- No TPP ordering; order by the code path they cover.
- Contradiction column becomes **Mutation**: the smallest production change that must turn the test
  red (e.g. `refuse at >= 2h instead of > 2h`). A behaviour with no killing mutation is not pinned.
- Write `**Mode:** CHARACTERIZATION` under the Objective. Execution: `tdd-workflow-engine` §6.

## 6. Test Type Detection

1. **Explicit prefix** wins: `unit:` · `e2e:` · `integration:`.
2. **E2E indicators:** api, endpoint, http, rest, route, controller, status code, request, response, json.
3. **Integration indicators:** postgres, database, sql, persist, jpa, testcontainers, adapter/repository implementation.
4. **Default:** unit (use case + domain through its ports, fakes wired by hand).

"actually use unit/e2e/integration" → switch immediately.

## 7. Analysis File Contract

**Path:** `workspace/current/tdd-iteration-XX.md` when the request names an iteration (zero-padded,
e.g. "Iteration 03"), else `workspace/current/tdd.md` (overwrite). Every agent — `tdd-analyze`,
`tdd`, `tdd-auto`, `iteration-orchestrator` — uses these paths.

```markdown
# TDD Analysis — [title]

> **Objective:** [1–3 self-contained sentences: what the system must do and why, with RG refs]

**Mode:** BEHAVIOR | CHARACTERIZATION
**Test Type:** UNIT | E2E | INTEGRATION ([detection reason])
**Test class:** `boundedcontexts/<context>/src/test/java/com/wealcome/<context>/unit/businesslogic/usecases/<feature>/<UseCase>Test.java`
(unit — mirrors `businesslogic/usecases/<feature>/`; a domain-service test goes under `unit/businesslogic/models/`;
e2e: `e2e/<Feature>E2ETest.java`; integration: `integration/<adapter package>/<Adapter>IT.java`, e.g. `integration/adapters/secondary/persistence/jpa/`)

## Ordered Test List

| # | Test Name | Contradiction | Status |
|---|-----------|---------------|--------|
| 1 | devrait_… | Baseline — [RGx] … | ⏳ Pending |
| 2 | devrait_… | [RGx] … the previous green cannot … `(TPP: conditional → loop)` · `(if early green: pin — <mutation>)` | ⏳ Pending |

## Files to Create
- [path from `boundedcontexts/<context>/src/main/java/com/wealcome/<context>/`]

## Design Notes
- [reusable fakes, helpers, conventions; decisions taken (inclusivity, refusal style); `Dropped: …`; open questions]
```

Columns are a **fixed schema** (producer and consumers parse them). In CHARACTERIZATION mode the
Contradiction column holds the Mutation. Optional suffixes in the Contradiction cell:
- `(TPP: …)` — only when the transformation guides a real choice (§2);
- `(if early green: pin — <mutation>)` — a **predicted** early green on a threshold's second side
  (§4 A3); the executor proves the mutation kills it and keeps it `🧬 PINNED`, or deletes it;
- `(if early green: delete — <why>)` — any other **predicted** early green; the executor deletes the
  test without asking.

**Status vocabulary — the only allowed values:**

| Status | Meaning | Set by |
|---|---|---|
| `⏳ Pending` | not finished (a written-but-red test is still Pending; resume re-runs it) | analysis |
| `✅ GREEN` | passed after a real RED → GREEN | RULE 7 |
| `🧬 PINNED — <mutation>` | test that passed on write, proven red by the recorded mutation, mutation reverted: a characterization test, or a threshold's second side | engine §6, §5 V4 |
| `➕ PROPOSED — <why>` | test discovered during implementation, appended, **not written**; awaits user approval | RULE 9 |

There is **no status for an early green**: a test that passes on write drove nothing, so it leaves
the suite and the table (RULE 7b), with `Dropped: <name> — early green: <reason>` in Design Notes —
unless it is strengthened or reordered, in which case it stays `⏳ Pending`, or it carries
`(if early green: pin — …)` and its mutation kills it, in which case it becomes `🧬 PINNED`. An approved
`➕ PROPOSED` row becomes `⏳ Pending`; a rejected one is deleted. A characterization test that cannot
be pinned leaves the table with `Not pinned: <name> — <reason>` in Design Notes.
