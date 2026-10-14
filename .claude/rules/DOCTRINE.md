---
paths:
  - "**/domain/**"
  - "**/usecase/**"
  - "**/adapters/**"
  - "workspace/**"
  - "docs/fonctionnel/**"
---

# Design Doctrine (Go)

The design rules the TDD agents cite. They hold for the **hexagonal modules** only; the code being taken
over (workflows, gateway, `store/`, `session/`, `cmd/agent`…) does not follow them yet and is not judged by
them. A decision particular to one module is recorded in that module's iteration plan
(`workspace/iterations.md`, **Decisions & Constraints**), then in `CLAUDE.md` once delivered.

## Modules are bounded contexts

- **One module per context** of `docs/fonctionnel/README.md` (DR Droits, CV Conversations, AG Agents,
  OU Outils, TF Travail en arrière-plan, MA Machines, CO Comptes, CA Canaux). Its rules are cited by their
  number (`DR6`) in test lists, test comments and commits.
- **A module never imports another module's `domain/` or `usecase/`.** When a context needs another, it
  declares a port in its own `usecase/` and an adapter implements it by calling the other module's use case
  (or by reacting to what the other published). Vocabulary borrowed from another context stays in that
  context's words at the boundary.
- **Layout** (Go package per directory):
  ```
  <module>/                 e.g. access/ (Droits)
  ├── domain/               aggregates, value objects, domain services, domain errors; standard library only
  ├── usecase/              one use case per file + the ports it calls (interfaces, business names)
  └── adapters/
      ├── memory/           in-memory fakes of the ports + the fixed clock (production code, see below)
      ├── postgres/         SQL adapters
      ├── temporal/         activities and workflows that call use cases; Temporal client adapters
      └── http/             handlers
  ```
  A directory exists only when a test needs it. A package named like the library it adapts (`http`,
  `temporal`) imports that library under an alias (`nethttp "net/http"`, `sdktemporal
  "go.temporal.io/sdk/temporal"`).
- **The composition root is `cmd/agent`** (and the workers' runtime): it builds the adapters and hands
  them to the use cases. No global, no `init()` wiring.

## Ports

- **Declared by the consumer**, in `usecase/` (Go: accept interfaces, declare them where they are used),
  named after the business: `RuleBook`, `Directory`, `Toolbox`, `Clock` — never `…Repository`,
  `…Service`, `I…`.
- **A port declares only what a use case calls.** What tests need to seed or read (`Feed`, `Snapshots`)
  lives on the fake, never on the port.
- **Only use cases call ports.** An entity receives values and other domain objects, never a port.
- A port method doing I/O takes `ctx context.Context` first and returns an `error`; absence is
  `(T, bool)` or a domain sentinel (`ErrNotFound`), never a nil pointer meaning "none".

## Time is a port

Many rules are time-bound (a rule changed "for the turns that start next", a task paused "once", a sweep
"after 5 minutes"). `domain/` and `usecase/` never call `time.Now()` (nor `time.Since`, `time.After`,
`time.Sleep`): a use case reads `Clock.Now()` and hands the instant to the domain. Its fake is
`memory.Clock`, set in the Given; the real one, `time.Now`, lives in an adapter. Each threshold is tested
on both sides of its edge.

## Fakes live in the production tree

In-memory fakes are secondary adapters, `<module>/adapters/memory/`, not `_test.go` helpers: unit tests,
e2e tests and a dev wiring reuse them. They assert the port at compile time
(`var _ usecase.RuleBook = (*RuleBook)(nil)`), copy on read and on write (snapshots, below), and return
deterministic orders. Hand-written; no mocking library, no generated mock.

## Aggregates and snapshots

- **Small aggregates, referenced by ID** (`UserID`, not `*User`), one consistency boundary each, sized by
  the invariants they must keep together.
- **An aggregate exposes behaviour, not data:** unexported fields, no getters or setters, methods that
  decide (Tell, Don't Ask). Value objects are small comparable structs with value receivers, built by a
  constructor (`NewPattern(s) (Pattern, error)`) only when a test needs the validation.
- **Snapshots:** an aggregate exports its state with `Snapshot()` — a flat struct of raw values, comparable
  with `==` whenever it can be — and is rebuilt with `FromSnapshot(s)`. Adapters and fakes map snapshots
  only; tests assert on snapshots. Snapshot types carry no struct tags: an adapter maps them to its own
  rows or DTOs.
- **Aggregate equality** (compare snapshots, or compare the aggregates with `==` / `reflect.DeepEqual`) is
  chosen at the first test that needs it, then kept.
- **Reads are deterministic.** Go randomizes map iteration: a snapshot or a read built from a map sorts
  (`slices.Sort`, `slices.SortFunc`); a SQL query that returns a list has an `ORDER BY`.

## Refusals

- A business refusal is an `error` the domain declares: a sentinel (`var ErrAgentNotOpen = errors.New(…)`)
  or a typed error when it carries data (`*NotOpenError{Agent, User}`), tested with `errors.Is` /
  `errors.As`. The shape is chosen at the first refusal test of the module, then kept.
- Never a `panic` for a business refusal; `panic` is for programming errors (and a scaffold's
  `panic("not implemented")`, engine §1).
- **The domain never formats.** It returns raw values and typed refusals; wording (the French of the web
  pages, Telegram messages, the model's prompt) belongs to the adapters.

## Tests

- **Levels, by package** (`tdd-testing-patterns` owns this): unit = `domain/` and `usecase/` with fakes;
  integration = `adapters/postgres/` on a throwaway database, `adapters/temporal/` with the SDK's
  `testsuite`; e2e = `adapters/http/` through `httptest` on the wired module.
- Tests are run by `.claude/scripts/tdd-run.sh`, never by a raw `go test`.
- The use case is the unit: `domain/` is driven through it; a domain service gets its own test only when
  its logic reads better specified alone.
