# TDD Kit — maintainer's guide

For the humans who maintain `.claude/`. Agents don't load this file.

The kit comes from the TDD · DDD · Clean Architecture training of 12 October 2026 (written for another
language and build), adapted to this Go project: it takes the platform over context by context
(`docs/fonctionnel/`, rules `DR1`… for Droits), each context becoming a hexagonal module
(`<module>/domain`, `usecase`, `adapters/…`; `.claude/rules/DOCTRINE.md`). The mechanics — states, human
gates, statuses, violations V0–V8, RULE n, PIN, REFACTOR/CHORE, the plan's write-ahead — are the training's,
unchanged.

## The flow

```
/iterate <task> ─> iteration-orchestrator ── per BEHAVIOR/PIN iteration ──> tdd-analyze ──> tdd-auto
                        │                                                  (test list)    (RED → clean GREEN)
                        ├── REFACTOR iterations: restructures itself under an untouched suite
                        └── CHORE iterations (contract, docs, config): does them itself

one requirement:  tdd-analyze ──> tdd-auto        learning / demo:  tdd  (gate before every RED and GREEN)
before the code:  /bdd-spec <request> <n> (specification by example) · /event-storming
```

Artifacts: `workspace/iterations.md` (plan) · `workspace/current/tdd-iteration-XX.md` or
`workspace/current/tdd.md` (test list and statuses) · `workspace/<request>-spec-by-example.md`.

## Who owns which rule

| File | Owns |
|------|------|
| `skills/tdd-analysis` | test naming, contradiction, TPP heuristic, granularity, pipeline A0–A6, PIN mode, test type detection, analysis file format, **status vocabulary** |
| `skills/tdd-workflow-engine` | RED sequence, clean GREEN, fallback, violations V0–V8, early green, PIN execution, `tdd-run.sh`, RULE 0/7/7b/9/C, test-first guard |
| `skills/tdd-{core,e2e,integration}-patterns` | how to write each test type in Go here (read on demand, one per run) |
| `skills/tdd-testing-patterns` | **test levels** (which package is unit, integration, e2e), doubles, fakes, snapshots, clock, fixtures (preloaded) |
| `agents/tdd-analyze`, `tdd-auto`, `tdd` | state machine and gates only |
| `agents/iteration-orchestrator` | iteration kinds, plan file, verification, commits |
| `rules/DOCTRINE.md` | the design doctrine the agents cite: modules, ports, time, fakes, snapshots, refusals |
| `rules/ARCHITECTURE.md`, `CODE-STYLE.md`, `ANTI-PATTERNS.md`, `IDENTITY.md` | layers, Go style, what to reject, stance |
| `rules/ENFORCEMENT.md` | what is mechanical, and the few rules no skill owns |

A rule is written **once**, in its owner; everything else points to it (`engine §N`, `tdd-analysis §N`).

**Rules are scoped.** Claude Code loads `.claude/rules/*.md` into every session of the project, like
`CLAUDE.md`. Most of this repository (workflows, gateway, `store/`, `session/`, `cmd/agent`…) predates the
hexagonal modules and does not follow them, so each rule carries a `paths:` frontmatter that loads it only
for `**/domain/**`, `**/usecase/**`, `**/adapters/**` (and, for some, `workspace/**`, `docs/fonctionnel/**`).
The TDD agents do not depend on that loading: they read `DOCTRINE.md` and their skills explicitly.

## Mechanics

| Script | What | When |
|--------|------|------|
| `scripts/tdd-run.sh` | runs `go test -json` (Go is not on the host: `go` if in the PATH, else `docker exec agent-dev`, else the build compose file; `TDD_GO` overrides), prints one classified status (`GREEN`, `RED_ASSERTION`, `RED_EXCEPTION` = a panic, `COMPILE_ERROR` = build or vet, `WIRING_ERROR` = the test binary failed outside any test, `NO_TESTS_RAN`, `INFRA_ERROR` = no container, no database, a timeout, or a target whose tests all skip); target = `<package> [<Test>[/<row>]]` or `<Test>` alone; `--unit` = the module `$TDD_MODULE` (default `access`) but its adapters; `--all` = `go vet ./...` + `go test ./...`; `--verify` replays the last green `--all` on an unchanged tree; `--snapshot` / `--changed-since-snapshot` tell what an iteration changed, independently of commits | every test run by an agent |
| `hooks/test-first-guard.sh` | refuses writes of production `.go` files while GREEN and no test (`*_test.go`, `testdata/`) changed, and Bash writes of Go files (`gofmt -w`, `go fmt`, `go generate` included) | `.claude/settings.json`; armed only while `.claude/state/tdd-active` exists (created/deleted by `tdd`, `tdd-auto`). While `.claude/state/refactor-active` exists (created/deleted by `iteration-orchestrator` around a REFACTOR), refuses any write of a test file, by Write/Edit or Bash |
| `hooks/bash-bypass-detector.sh` | PostToolUse: reports and logs Go files a Bash command changed anyway | `.claude/settings.json`, same markers |
| `scripts/bash-writes.sh` | the one judgement of "does this Bash command write Go files" (per segment, redirects to non-source targets ignored), sourced by `test-first-guard.sh` | through the guard |
| `scripts/lint-kit.sh` | stale names (leftovers of the training's language, build and domain), dangling `§`/`RULE`, unknown statuses, wrong paths, missing skills/scripts, unwired hooks, size budgets | after every edit under `.claude/` |
| `scripts/eval-mechanics.sh` | self-test of `--snapshot` / `--changed-since-snapshot`, the refactor guard, the test-first guard and the bypass detector, on untracked probe files in `./zzmechanicsprobe/` (seconds, no agent, no Go run) | after editing `tdd-run.sh` or a hook |
| `budget.txt` | byte budget of preloaded files; `lint-kit.sh` flags +15% | raise only on purpose |

Runtime state lives in `.claude/state/` (git-ignored): `last-run`, `last-run.log`, `tdd-active`,
`refactor-active`, `iteration-base` (the iteration snapshot), `bash-start`, `guard.log` (refusals and
bypasses), `tdd-paused` (the guard paused during engine §3 and §6 only). If an agent crashes, delete
`tdd-active` or `refactor-active` (both are ignored after 12 hours anyway).

`settings.json` (versioned) wires the hooks and refuses to read `.env` files and keys; personal settings
stay in `settings.local.json` (git-ignored).

## Not carried over from the training

- **The evaluation** (`eval/`: golden test lists and plans, `eval-diff.sh`, `eval-metrics.sh`,
  `eval-plan.sh`): its goldens were written for the training's project. **It is to be rebuilt for Go**
  (goldens on a lot of `docs/fonctionnel/droits.md`) before a behavioural change of the agents can be
  measured; until then such a change is reviewed by hand.
- A copy of `commands/event-storming.md` that sat at the root of `.claude/`.

## Changing the kit

1. Edit the owner file, not a copy of the rule elsewhere.
2. `.claude/scripts/lint-kit.sh` → 0 problems. Edited `tdd-run.sh` or a hook? `.claude/scripts/eval-mechanics.sh` → `GREEN`.
3. Behavioural change (analysis, sequencing, gates)? No evaluation exists yet for Go (above): say so in
   the commit, and try the change on a real lot before relying on it.
4. New status, rule or section? Add it to its owner **and** to `lint-kit.sh`'s known lists if needed.
