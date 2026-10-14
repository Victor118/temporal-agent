# Enforcement

What is enforced, and where. Rules live in one place each; this file only adds what no skill owns.

## Where each rule lives

| Concern | Owner |
|---|---|
| Test-first, RED/GREEN sequence, violations V0–V8, statuses updates | `tdd-workflow-engine` skill |
| Test list: naming, contradiction, granularity, statuses vocabulary | `tdd-analysis` skill |
| Gates (`go red`, `go green`, early-green dispositions) | `tdd` / `tdd-auto` agents |
| Iteration kinds, verification, commits | `iteration-orchestrator` agent |
| Layers and dependency direction | `ARCHITECTURE.md` |

## Mechanical guarantees (not prose)

- **Test-first guard** — `.claude/hooks/test-first-guard.sh` (PreToolUse, `.claude/settings.json`). While
  a TDD agent runs (`.claude/state/tdd-active`), a write to `src/main/` is refused when the last test
  run was GREEN and no test file changed since; a Bash command writing under `src/` is refused;
  `bash-bypass-detector.sh` reports what slipped through. While the orchestrator restructures a REFACTOR
  (`.claude/state/refactor-active`), any write under `src/test/` is refused, by Write/Edit or Bash.
- **Test outcome** — `.claude/scripts/tdd-run.sh` classifies a run as `GREEN`, `RED_ASSERTION`,
  `RED_EXCEPTION`, `COMPILE_ERROR`, `WIRING_ERROR`, `NO_TESTS_RAN` or `INFRA_ERROR`. Only `RED_*` is RED.
- **Kit consistency** — `.claude/scripts/lint-kit.sh`: stale names, dangling `§`/`RULE` references,
  unknown statuses, wrong analysis paths, unwired hooks, size budgets. Run it after editing anything
  under `.claude/`.

## Rules no skill owns

1. **No defensive code without a failing test** — no null check, validation or `Objects.requireNonNull`
   in a value object unless a test fails without it.
2. **Dependency inversion** — `businesslogic/` imports no Spring, JPA, Jackson or `adapters` class; it
   defines ports, adapters implement them. A violation is fixed by moving the concrete code out and
   depending on a port, never by weakening the test. A context never imports another context
   (`com.wealcome.appointments` ↔ `com.wealcome.reliability`): they exchange published events.
3. **Snapshot assertions** — tests assert on `toSnapshot()` rather than on aggregates. Aggregate
   equality (id-only vs full state) is chosen at the first test that needs it (`CLAUDE.md`), then kept.
4. **The user's technical expectations win** over default patterns (assertion library, refusal style,
   design pattern), within test-first, no-defensive-code and dependency inversion.
5. **User gates are real stops** — an agent that ends with `⛔ … PAUSED` is not resumed or replaced
   until the user answers.

## When a rule is broken

Say which rule, undo the offending change (not the test), and continue. No ceremony.
