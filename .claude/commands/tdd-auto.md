---
description: Autonomous TDD workflow (migrated to tdd-auto agent)
---

# /tdd-auto -> TDD Auto Agent

The autonomous TDD workflow uses the **`tdd-auto` custom agent** for continuous RED-GREEN execution without human-in-the-loop gates.

## How to use

The `tdd-auto` agent flows through RED → GREEN for each test automatically, only pausing at CYCLE_COMPLETE. You can explicitly invoke it:

```
"use the tdd-auto agent to implement: DR13 — un agent fermé à l'utilisateur du tour ne répond pas"
"use the tdd-auto agent for: e2e: exposer l'aperçu d'une règle d'accès en HTTP (DR26)"
"use the tdd-auto agent for: integration: persister les règles d'accès en PostgreSQL"
```

## Test type prefixes

| Prefix | Test Type |
|--------|-----------|
| (none) | Unit test (default) |
| `unit:` | Unit test (explicit) |
| `e2e:` | E2E test |
| `integration:` | Integration test |

The agent auto-detects test type from keywords. Use prefixes to override.

## Which one?

| Use | For | Gates |
|-----|-----|-------|
| `/iterate <task>` | delivering a multi-iteration task | plan, each iteration, commits |
| `tdd-analyze` → `tdd-auto` | delivering one requirement | only at the end (proposed tests; early greens are deleted and reported) |
| `tdd` | learning, demos, reviewing every step | before every RED and every GREEN |
