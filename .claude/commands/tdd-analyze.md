---
description: Analyze a business requirement into a contradiction-driven test list (workspace/current/)
---

# /tdd-analyze -> TDD Analyze Agent

The TDD analysis workflow uses the **`tdd-analyze` custom agent**.

## How to use

The `tdd-analyze` agent is automatically delegated by Claude when you want to analyze a requirement before coding. You can also explicitly invoke it:

```
"use the tdd-analyze agent to analyze: DR15 — un changement de règle vaut pour les tours qui commencent ensuite"
"use the tdd-analyze agent to analyze: e2e: exposer l'aperçu d'une règle d'accès en HTTP (DR26)"
"use the tdd-analyze agent to analyze: integration: persister les règles d'accès en PostgreSQL"
```

## What it produces

Written to `workspace/current/tdd.md` (or `workspace/current/tdd-iteration-XX.md`), with:
- **FLFI names** — final business names, `Test<Subject>_<FinalBehaviour>` (English, like the code)
- **Ordering** — each test forces what the previous green cannot do (TPP as a heuristic)
- **Dropped candidates** — every test that would bring no new behaviour, with its reason
- **Predicted early greens** — marked so the executor deletes them, or pins a threshold's second side, without asking
- **Design notes** — existing fakes, models, and patterns to reuse

## Next steps

Pass the test list to an execution agent:
- Interactive: `use the tdd agent to implement workspace/current/tdd.md`
- Autonomous: `use the tdd-auto agent to implement workspace/current/tdd.md`

## Which one?

| Use | For | Gates |
|-----|-----|-------|
| `/iterate <task>` | delivering a multi-iteration task | plan, each iteration, commits |
| `tdd-analyze` → `tdd-auto` | delivering one requirement | only at the end (proposed tests; early greens are deleted and reported) |
| `tdd` | learning, demos, reviewing every step | before every RED and every GREEN |
