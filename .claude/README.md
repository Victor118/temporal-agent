# TDD Kit — maintainer's guide

For the humans who maintain `.claude/`. Agents don't load this file.

## The flow

```
/iterate <task> ─> iteration-orchestrator ── per BEHAVIOR/PIN iteration ──> tdd-analyze ──> tdd-auto
                        │                                                  (test list)    (RED → clean GREEN)
                        ├── REFACTOR iterations: restructures itself under an untouched suite
                        └── CHORE iterations (contract, docs, config): does them itself

one requirement:  tdd-analyze ──> tdd-auto        learning / demo:  tdd  (gate before every RED and GREEN)
```

Artifacts: `workspace/iterations.md` (plan) · `workspace/current/tdd-iteration-XX.md` or
`workspace/current/tdd.md` (test list and statuses).

## Who owns which rule

| File | Owns |
|------|------|
| `skills/tdd-analysis` | test naming, contradiction, TPP heuristic, granularity, pipeline A0–A6, PIN mode, test type detection, analysis file format, **status vocabulary** |
| `skills/tdd-workflow-engine` | RED sequence, clean GREEN, fallback, violations V0–V8, early green, PIN execution, `tdd-run.sh`, RULE 0/7/7b/9/C, test-first guard |
| `skills/tdd-{core,e2e,integration}-patterns` | how to write each test type in WealDoc (read on demand, one per run) |
| `skills/tdd-testing-patterns` | doubles, fakes, snapshots, clock, fixtures (preloaded) |
| `agents/tdd-analyze`, `tdd-auto`, `tdd` | state machine and gates only |
| `agents/iteration-orchestrator` | iteration kinds, plan file, verification, commits |
| `rules/ENFORCEMENT.md` | what is mechanical, and the few rules no skill owns |

A rule is written **once**, in its owner; everything else points to it (`engine §N`, `tdd-analysis §N`).

## Mechanics

| Script | What | When |
|--------|------|------|
| `scripts/tdd-run.sh` | runs tests, prints one classified status (`GREEN`, `RED_ASSERTION`, `RED_EXCEPTION`, `COMPILE_ERROR`, `WIRING_ERROR`, `NO_TESTS_RAN`, `INFRA_ERROR`); `--verify` replays the last green `--all` on an unchanged source tree; `--snapshot` / `--changed-since-snapshot` tell what an iteration changed, independently of commits | every test run by an agent |
| `hooks/test-first-guard.sh` | refuses `src/main` writes while GREEN and no test changed, and Bash writes under `src/` | `.claude/settings.json`; armed only while `.claude/state/tdd-active` exists (created/deleted by `tdd`, `tdd-auto`). While `.claude/state/refactor-active` exists (created/deleted by `iteration-orchestrator` around a REFACTOR), refuses any write under `src/test/`, by Write/Edit or Bash |
| `hooks/bash-bypass-detector.sh` | PostToolUse: reports and logs `src/` files a Bash command changed anyway | `.claude/settings.json`, same markers |
| `scripts/bash-writes.sh` | the one judgement of "does this Bash command write source files" (per segment, redirects to non-source targets ignored), sourced by `test-first-guard.sh` and called by `eval-metrics.sh` | through the guard and the metrics |
| `scripts/lint-kit.sh` | stale names, dangling `§`/`RULE`, unknown statuses, wrong paths, missing skills/scripts, unwired hooks, size budgets | after every edit under `.claude/` |
| `scripts/eval-diff.sh` | compares an analysis with a golden list (tests, order, dropped, test class path) | during an evaluation |
| `scripts/eval-metrics.sh` | tool calls, runs, raw Maven runs, writes, refusals, bypasses, tokens, RULE 9 misses from a transcript | during an evaluation |
| `scripts/eval-plan.sh` | compares an orchestrator plan file with `golden/o1-plan.md` (Kinds, order), checks its invariants, and counts gates reached / skipped and refactor-guard refusals from the main transcript | during an O1 evaluation |
| `scripts/eval-mechanics.sh` | self-test of `--snapshot` / `--changed-since-snapshot`, the refactor guard and the bypass detector, on untracked probe files (seconds, no agent, no Maven) | after editing `tdd-run.sh` or a hook |
| `eval/budget.txt` | byte budget of preloaded files; `lint-kit.sh` flags +15% | raise only on purpose |

Runtime state lives in `.claude/state/` (git-ignored): `last-run`, `last-run.log`, `tdd-active`,
`refactor-active`, `iteration-base` (the iteration snapshot), `bash-start`, `guard.log` (refusals and
bypasses), `tdd-paused` (the guard paused during engine §3 and §6 only). If an agent crashes, delete
`tdd-active` or `refactor-active` (both are ignored after 12 hours anyway).

## Changing the kit

1. Edit the owner file, not a copy of the rule elsewhere.
2. `.claude/scripts/lint-kit.sh` → 0 problems. Edited `tdd-run.sh` or a hook? `.claude/scripts/eval-mechanics.sh` → `GREEN`.
3. Behavioural change (analysis, sequencing, gates)? Run the evaluation (`eval/README.md`) on an eval
   branch and add a row to `eval/results.md`: E1–E3 for `tdd-analyze` / `tdd-auto`, O1 and its traps
   for `iteration-orchestrator`. Update a golden file only on purpose, in its own commit.
4. New status, rule or section? Add it to its owner **and** to `lint-kit.sh`'s known lists if needed.
