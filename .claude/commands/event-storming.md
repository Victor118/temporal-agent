---
description: Run an Event Storming workshop to discover domain events, aggregates, and bounded contexts
---

# /event-storming -> Event Storming Agent

The Event Storming workflow uses the **`event-storming` custom agent**.

## Delegation instructions for the parent agent

**CRITICAL:** When delegating to the `event-storming` agent, the parent agent MUST NOT ask it to "produce the output", "run a complete workshop", or "go through all activities autonomously". The event-storming agent is **interactive by design** — it will use `AskUserQuestion` to collaborate with the user at every activity.

The parent agent prompt MUST:
- Pass the user's focus area or arguments (e.g., "based on the spec ride_booking.md")
- Let the agent drive the interaction — do NOT pre-digest the spec or list business rules in the prompt
- Keep the prompt short: "Facilitate an Event Storming workshop. Context: {user_args}"

The parent agent prompt MUST NOT:
- Summarize the spec content for the agent (it will read it silently itself)
- Ask the agent to "produce" or "generate" the deliverable
- Include phrases like "go through all 10 activities and produce the full output"

## How to use

The `event-storming` agent is automatically delegated by Claude when you want to run an Event Storming session. You can also explicitly invoke it:

```
"use the event-storming agent to explore the full ride booking domain"
"use the event-storming agent to focus on: US1 — Book a Ride"
"use the event-storming agent to discover aggregates and bounded contexts for the ride booking system"
```

## What it produces

A structured Event Storming board with:
- **Domain Events** — all business facts discovered (orange stickies)
- **Commands & Actors** — who triggers what (blue/yellow stickies)
- **Policies** — automated business reactions (lilac stickies)
- **Read Models** — information needed for decisions (green stickies)
- **External Systems** — third-party integrations (pink stickies)
- **Aggregates** — consistency boundaries (large yellow)
- **Bounded Context Map** — strategic design output
- **Hot Spots** — open questions and ambiguities

Output is written to `workspace/event-storming.md`.

## Next steps

Feed the discovered bounded contexts and aggregates into:
- BDD workshop: `use the bdd-workshop agent to explore: [aggregate/context]`
- TDD analysis: `use the tdd-analyze agent to analyze: [use case from discovered context]`
