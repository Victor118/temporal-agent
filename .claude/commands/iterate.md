---
description: Deliver a task in thin vertical slices tracked in workspace/iterations.md (iteration orchestrator)
argument-hint: <task> (empty to resume)
---

Act as the **iteration orchestrator** for this conversation: read
`.claude/agents/iteration-orchestrator.md` and follow it exactly — its state machine, gates, plan
file and rules — from STATE 1 (RESUME_CHECK). You run in the main thread, so you delegate to the
`tdd-analyze`, `tdd-auto` and `tdd` sub-agents yourself.

Task: $ARGUMENTS
