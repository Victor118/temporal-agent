---
paths:
  - "**/domain/**"
  - "**/usecase/**"
  - "**/adapters/**"
---

# Code Style Rules

Code, identifiers, comments and test names are in **English**, like the rest of the repository; the
business rules they implement are French (`docs/fonctionnel/`) and are cited by number (`DR6`).

## Naming Conventions

### Domain Objects
- **Aggregates**: Noun (`Rule`, `Group`, `Session`)
- **Value Objects**: Descriptive noun (`Pattern`, `ToolName`, `UserID`)
- **Domain Services**: Verb phrase or policy (`Allowed`, `Removes`)
- **Domain Events**: Past tense (`RuleChanged`, `AgentClosed`)
- **Errors**: `Err…` sentinels (`ErrAgentNotOpen`), `…Error` types (`*SoleHolderError`)

### Use Cases
- **Use Cases**: imperative verb, one per file (`OpenTurnTools`, `DeleteGroup`); a write use case is a
  command handler, a read use case a query
- **Commands / queries**: a struct named after the intention (`DeleteGroup{GroupID}`), passed to the use
  case's single method
- **Ports**: business nouns (`RuleBook`, `Directory`, `Toolbox`, `Clock`), never `…Repository` or `I…`

### Packages
- Short, lower case, no underscore, named for what they provide (`domain`, `usecase`, `memory`, `postgres`)
- No stutter: `memory.RuleBook`, not `memory.MemoryRuleBook`; `usecase.DeleteGroup`, not `usecase.DeleteGroupUseCase`

### Tests
- **Test functions**: `Test<Subject>_<FinalBehaviour>` in English CamelCase, business language — see
  `tdd-analysis` §1 (`TestAllowed_EveryRuleIsALock`)
- **Example rows** of one behaviour: a table and `t.Run(<row name>)`, row names in plain words
- **Helpers**: `given…` / `when…` / `then…`, with `t.Helper()`
- **Test files**: `<file under test>_test.go`, beside it

## Go Principles

### Absence and errors
- Absence is `(T, bool)` or a domain sentinel, never a nil pointer that means "none"
- Every error is returned or handled, never dropped; wrap with `%w` only across a layer that adds context
- Validation (in a constructor returning `(T, error)`) only when a test proves the need

### Immutability
- Value objects: small structs with unexported fields and value receivers
- Snapshots: flat structs of raw values; a slice in a snapshot is a copy (`slices.Clone`)
- Never hand out a reference to internal state (a slice, a map) without copying it

### Visibility
- Unexported by default
- Exported only what crosses a package boundary (`domain` → `usecase` → `adapters`)
- An adapter's internals stay unexported; only its constructor and the port methods are exported

### Object Construction
- A constructor (`NewX`) when the zero value is not valid; otherwise the zero value works
- No builder unless variations really multiply; a functional option only when a test needs two settings
- Dependencies are passed explicitly to the use case (struct fields or a constructor), never globals

## Code Organization

### Package Structure
- One module per bounded context, `domain` / `usecase` / `adapters` inside (`DOCTRINE.md`)
- One aggregate per file in `domain/`, one use case per file in `usecase/`
- Clear boundaries between modules: no import of another module's `domain/` or `usecase/`

### Size
- Single Responsibility: a file holds one concept; ~150 lines is a signal to split
- Functions: 10–20 lines, one level of abstraction; extract when a comment explains a block

### Interfaces
- Small (one to three methods), declared by their consumer
- No interface with a single implementation unless it is a port (a fake is the second one)

## Domain Specific Rules

### No Anemic Models
- Behavior lives with data
- Methods that decide, not exported fields
- No setters

### Tell, Don't Ask
- Objects do things, they don't expose data
- No getter chains
- Encapsulate decisions

### No Primitive Obsession
- Wrap primitives in named types or value objects (`type GroupID string`) when a test or a mix-up shows the need
- Type safety over convenience
