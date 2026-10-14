---
paths:
  - "**/domain/**"
  - "**/usecase/**"
  - "**/adapters/**"
---

# Anti-Patterns to Reject

## Domain Anti-Patterns

### Anemic Domain Model
❌ Structs with exported fields that use cases fill in and read back
❌ Business logic in a use case, a workflow or a handler instead of the domain
❌ Domain types used as JSON or SQL rows
✅ Rich types with behavior and invariants

### Large Aggregates
❌ Including entire object graphs
❌ Loading collections when only a count is needed
❌ Parent tracking all children
✅ Small aggregates, reference by ID

### Aggregate References
❌ `owner *User`
❌ Direct navigation between aggregates
✅ `owner UserID`

### Bidirectional Associations
❌ Parent knows children, children know parent
✅ Unidirectional, query when needed

## Architecture Anti-Patterns

### Smart Adapters
❌ Business rules in an HTTP handler, an activity, a workflow or a SQL `WHERE`
❌ Validation beyond input format in adapters
✅ Adapters only translate to and from use cases

### Infrastructure Leak
❌ `json:` / `db:` tags, `database/sql`, Temporal or `net/http` types in `domain/`
❌ `usecase/` importing an adapter
✅ Pure domain, infrastructure adapts

### Shared Mutable State
❌ Package-level mutable variables, `init()` wiring
❌ A slice or map of an aggregate handed out without a copy
✅ Values copied at the boundary, isolated aggregates

## Testing Anti-Patterns

See the `tdd-testing-patterns` skill for testing patterns and anti-patterns.

## Code Quality Anti-Patterns

### Primitive Obsession
❌ `string` for every ID, `int` for a duration in minutes
❌ Validation scattered everywhere
✅ Named types and value objects with encapsulated validation

### Feature Envy
❌ A function that mostly reads another type's data
❌ Reaching through objects for data
✅ Move behavior to where data lives

### God Packages
❌ One package doing everything (`util`, `common`, `helpers`)
❌ 1000-line files
✅ Packages named for what they provide, cohesive files

### Long Parameter Lists
❌ `Open(ctx, id, user, agent, title, channel, mode, fork)`
✅ A command struct

### Interface Pollution
❌ An interface per struct, declared by its implementer
❌ Large interfaces "in case"
✅ Small interfaces declared by the consumer

## Integration Anti-Patterns

### Distributed Monolith
❌ A module calling into another module's internals
❌ Two modules writing the same table
✅ A port in the caller, an adapter that calls the other module's use case or reacts to its events

### Missing ACL
❌ External model leaking into the domain (an MCP payload, a Temporal history, a model provider's response)
❌ Direct use of third-party types
✅ The adapter translates into the module's own types

### Chatty Integration
❌ Several calls to fulfil one use case
❌ N+1 queries
✅ One port call that returns what the use case needs
