---
paths:
  - "**/domain/**"
  - "**/usecase/**"
  - "**/adapters/**"
  - "workspace/**"
  - "docs/fonctionnel/**"
---

# Clean Architecture Rules

## The Dependency Rule

**THE OVERRIDING RULE**: Dependencies can ONLY point inward.

```
cmd/agent, workflows, store, server (composition and the code being taken over)
    ↓
<module>/adapters/    postgres/, temporal/, http/ … (primary and secondary)
    ↓
<module>/usecase/     use cases (primary ports) + the ports they need (secondary ports)
    ↓
<module>/domain/      core business, pure Go
```

A **module** is one bounded context of `docs/fonctionnel/` (`access/` = Droits, DR rules). Its layout,
and the rest of the doctrine, is in `DOCTRINE.md`.

### Non-Negotiable Principles

1. **`domain/` has ZERO dependencies outside the standard library**
   - Imports nothing of the project but other `domain/` packages
   - No `database/sql`, pgx, `go.temporal.io/…`, `net/http`, `encoding/json`, no struct tags
   - Pure business logic only

2. **Dependencies point inward ALWAYS**
   - `adapters` → `usecase` → `domain`, never the reverse
   - The Go compiler refuses import cycles; it does not refuse an inward package importing an outward
     one that imports nothing back: that check is ours (`iteration-orchestrator` VERIFY)

3. **Infrastructure adapts to the domain**
   - `usecase/` declares the ports (interfaces) it calls, in business names
   - Adapters implement them; Go's implicit interfaces need no `implements`: an adapter asserts it at
     compile time (`var _ usecase.RuleBook = (*RuleBook)(nil)`)

4. **Use cases orchestrate, they don't contain business logic**
   - Business rules live in `domain/`
   - Use cases load through ports, ask the domain, save through ports
   - The transaction boundary is the use case (through a port when one is needed)

## Layer Responsibilities

### `domain/` (innermost)
- Aggregates, entities, value objects, domain services
- Domain errors (sentinels or typed errors) and domain events, when a test asks for them
- No ports: an entity receives values and other domain objects, never a port

### `usecase/`
- One use case per file: a type with its dependencies (ports) and one method (`Run`, `Open`, …)
- Commands and queries (CQRS): a write use case returns no read model; a read use case changes nothing
- The secondary ports, declared where they are used: `RuleBook`, `Directory`, `Toolbox`, `Clock` — never
  `Repository`, `Manager`, `Service`

### `adapters/`
- Secondary: `postgres/` (SQL), `temporal/` (activities, workflows, signals), `memory/` (in-memory fakes,
  reused by tests and dev wiring)
- Primary: `http/` (handlers), `temporal/` (an activity that calls a use case), a CLI command
- The code being taken over (workflows, activities, `store/`, `session/`, `cmd/agent`) becomes adapters:
  it calls use cases and implements ports, and its old business code disappears

## Boundary Crossing Rules

### Data Crossing Boundaries
✅ **DO**: Use simple structs, value objects, snapshots, or primitives
❌ **DON'T**: Pass `*sql.Rows`, `workflow.Context`, `*http.Request` or JSON documents into `usecase/` or `domain/`

`context.Context` is the one exception: a port method that does I/O takes a `ctx` first, as in all Go code.
The domain never takes one.

### Port and Adapter Pattern
- **Ports**: interfaces declared by `usecase/`, small (one to three methods), named after the business
- **Adapters**: implementations under `adapters/`
- Primary adapters drive the application (HTTP handlers, Temporal activities, CLI)
- Secondary adapters are driven by it (PostgreSQL, Temporal client, MCP, model providers)

## Testing Implications

- **Domain and use case tests**: no infrastructure, hand-written fakes for the ports only (unit)
- **Adapter tests**: against real infrastructure: a throwaway PostgreSQL, Temporal's `testsuite` (integration)
- **HTTP tests**: `httptest` on the wired module (e2e)

## Common Violations to Reject

❌ SQL, a Temporal type or `net/http` in `domain/` or `usecase/`
❌ A business rule decided in a workflow, an activity, a handler or a SQL query
❌ `time.Now()` in `domain/` or `usecase/`
❌ A module importing another module's `domain/` or `usecase/`
❌ An interface declared next to its only implementation "for testing"

## The Litmus Test

Before writing any code, ask:
- Can I test this without PostgreSQL, Temporal or an HTTP server?
- Can I swap the database without changing the domain?
- Can I move this use case from an activity to an HTTP handler without touching it?
- Is my domain expressing business concepts only?

If any answer is "no", the architecture is compromised.
