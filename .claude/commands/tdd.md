---
description: TDD workflow (migrated to tdd agent)
---

# /tdd -> TDD Agent

The TDD workflow has been migrated to the **`tdd` custom agent** for stronger identity and enforcement.

## How to use

The `tdd` agent is automatically delegated by Claude when you describe a feature requirement. You can also explicitly invoke it:

```
"use the tdd agent to implement: DR13 — un agent fermé à l'utilisateur du tour ne répond pas"
"use the tdd agent for: e2e: exposer l'aperçu d'une règle d'accès en HTTP (DR26)"
"use the tdd agent for: integration: persister les règles d'accès en PostgreSQL"
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
