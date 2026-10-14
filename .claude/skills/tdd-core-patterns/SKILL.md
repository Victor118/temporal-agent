---
name: tdd-core-patterns
description: Strategic testing philosophy for the business logic of a Go hexagonal module (domain/ and usecase/) - sociable tests through use cases, outside-in TDD, time as a port, and core testing decisions
---

# Skill: TDD Core Patterns

## Purpose

Strategic testing philosophy for the business logic of a module (`<module>/domain`, `<module>/usecase`),
using Martin Fowler's **sociable tests**. It defines **what to test** and **how to approach** unit tests:
through use cases, with real domain values and fake ports.

- **TDD sequence, violations:** `tdd-workflow-engine` · **TPP, test list, status vocabulary:** `tdd-analysis`.
- **Test levels, doubles, fixtures, assertions:** `tdd-testing-patterns` · **Doctrine:** `.claude/rules/DOCTRINE.md`.

> **Names below are illustrative, not design commitments.** They use `access` (Droits, DR1–DR36 in
> `docs/fonctionnel/droits.md`): `TurnTools`, `RuleBook`, `Directory`, `Toolbox`, `memory.Clock` only
> show the shape. Let the real names emerge from the tests and the rules. Today `access/` is a single pure
> package (`access/access.go`): it becomes `access/domain` through a REFACTOR, never by rewriting it
> under a new test.

## Testing Philosophy: Outside-In, Sociable, Wishful Thinking

**Wishful thinking:** the test is written first, calling types and functions that do not exist yet —
see `tdd-workflow-engine` for the canonical sequence.

**Outside-In:**
- Start from the use case with a business scenario (one example of a DR rule)
- Let aggregates and value objects emerge from what the use case needs
- Begin with simple types, evolve to rich domain types when a test demands it

**Sociable:**
- The use case works with **real** aggregates, entities and value objects
- Only **ports** are faked (rule book, directory of users, toolbox, clock, notifier, another module)
- Tests describe business scenarios, not type structure

**What we avoid:** inside-out design (aggregates before behaviour), solitary tests doubling domain types,
production code before the test, a test that reaches the domain through a workflow or a handler.

**Decision rule:** wishful thinking first, outside-in, sociable inside `domain/` + `usecase/`, isolated
from infrastructure.

## Core Principles

- < 10 ms per test, memory only — no database, network, file system or Temporal
- Hand-written fakes from `adapters/memory`, **no mocking library**
- **Time is a port:** domain and use cases never call `time.Now()`; they read `Clock`, faked by `memory.Clock`
- Assert through `Snapshot()` (`ENFORCEMENT.md`); aggregate equality decided at the first test that needs it
- Absence as `(T, bool)` or a sentinel, refusals as domain errors, unexported by default
- **NEVER add defensive code (nil checks, validation) or getters/setters unless a failing test demands it**

## Test Categories

### Primary target: use cases
- **Production:** `<module>/usecase/<use_case>.go` (one use case per file, its ports beside it)
- **Test:** `<module>/usecase/<use_case>_test.go`, `package usecase_test`
- **Why:** validates complete business scenarios with low maintenance cost

### Secondary target: domain services (when justified)
- **Test:** `<module>/domain/<service>_test.go`
- Only for complex isolated logic: matching, a calculation, a cross-aggregate rule that reads better
  specified alone (today's `access.Allowed`, DR1–DR9, is such a decision table)

### Not tested directly: aggregates, entities, value objects
Their behaviour and invariants are verified through use case scenarios. Isolated tests of `Rule` or
`Pattern` over-specify structure and break on every refactoring.

## Primary Testing Through Use Cases — Example

```go
// access/usecase/turn_tools_test.go
package usecase_test

var now = time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)

type world struct {
	rules     *memory.RuleBook
	directory *memory.Directory
	toolbox   *memory.Toolbox
	turnTools usecase.TurnTools
}

func newWorld() *world {
	w := &world{rules: memory.NewRuleBook(), directory: memory.NewDirectory(), toolbox: memory.NewToolbox()}
	w.turnTools = usecase.TurnTools{Rules: w.rules, Users: w.directory, Tools: w.toolbox}
	return w
}

func TestTurnTools_ClosedToolsAreLeftOut(t *testing.T) { // DR2, DR11
	w := newWorld()
	w.givenAgent("dev", "web_fetch", "exec")
	w.givenRule(domain.RuleSnapshot{ID: 1, Pattern: "exec", Users: []string{"alice"}})

	got := w.whenATurnStarts(t, "dev", "bob")

	if want := []string{"web_fetch"}; !slices.Equal(got.Open, want) {
		t.Errorf("open tools = %v, want %v", got.Open, want)
	}
}

func TestTurnTools_ADisabledUserGetsNothing(t *testing.T) { // DR8
	w := newWorld()
	w.givenAgent("dev", "web_fetch")
	w.directory.Feed(domain.UserSnapshot{ID: "bob", Disabled: true})

	_, err := w.turnTools.Run(context.Background(), usecase.StartTurn{Agent: "dev", User: "bob"})

	if !errors.Is(err, domain.ErrAgentNotOpen) {
		t.Fatalf("err = %v, want ErrAgentNotOpen", err)
	}
}

func (w *world) whenATurnStarts(t *testing.T, agent, user string) usecase.TurnToolsResult {
	t.Helper()
	got, err := w.turnTools.Run(context.Background(), usecase.StartTurn{Agent: agent, User: user})
	if err != nil {
		t.Fatalf("turn of %s for %s: %v", agent, user, err)
	}
	return got
}
```

### Time-bound rules: both sides of the threshold

A time-bound rule gets **two tests**, one on each side of the edge, driven by the fixed clock (and the
inclusivity decided from the rule's wording, written in the list):

```go
func TestSweep_AlertsARuleThatStoppedMatching(t *testing.T) { …  clock.At = lastMatched.Add(time.Minute) … }
func TestSweep_StaysSilentWhileARuleStillMatches(t *testing.T) { … clock.At = lastMatched … }
```

## Secondary Testing — Domain Service (When Justified)

```go
// access/domain/allowed_test.go
func TestAllowed_ServerIsNotAPrefix(t *testing.T) { // DR4
	rules := []Rule{{Kind: KindMCPServer, Target: "web", Groups: []string{"research"}}}
	bob := &User{ID: "bob"}
	if Allowed(bob, FromMCP("web", "web_search"), rules) {
		t.Error("the web server's tool open to bob")
	}
	if !Allowed(bob, BuiltIn("web_fetch"), rules) {
		t.Error("web_fetch, a built-in tool, closed by the rule on the server web")
	}
}
```

## Machinery Introduced Only When a Test Demands It

A module starts with no event bus, outbox, transaction port or base aggregate type. Do not create them up
front; add each one when a failing test needs it.

- **Domain events** (e.g. DR34's alert when a rule stops matching): assert through a spy port that
  receives them, never by bloating the store fake.
- **Read side / queries** (e.g. DR27, what a user may use, agent by agent): a query use case is tested
  through its method with the same outside-in discipline, but:
  - it returns a flat read model (a struct of raw values), asserted directly — **no snapshot**;
  - build the expected read model from the read shape, not from an aggregate's `Snapshot()`;
  - it is side-effect free: no event, no store change;
  - fake its read port in unit tests; real SQL in `adapters/postgres` tests; `GET` in `adapters/httpapi` tests.

## Anti-Patterns

### Solitary tests on domain types

```go
// BAD - an interface over a domain type, to double it
type ruleMatcher interface{ Matches(domain.Tool) bool }

// GOOD - real values, fake ports only
w.givenRule(domain.RuleSnapshot{ID: 1, Pattern: "exec", Users: []string{"alice"}})
```

### Real infrastructure or real time in unit tests

```go
// BAD
db, _ := sql.Open("pgx", os.Getenv("DATABASE_URL")) // slow, not a unit test
time.Now()                                          // non-deterministic, threshold untestable
env := suite.NewTestWorkflowEnvironment()           // a workflow is an adapter, not the unit

// GOOD
rules := memory.NewRuleBook()
clock := &memory.Clock{At: now}
```

## Test Organization and Commands

```
access/
├── domain/        allowed.go, allowed_test.go            # SECONDARY (justified only)
├── usecase/       turn_tools.go, turn_tools_test.go      # PRIMARY
└── adapters/      memory/ (fakes), postgres/, temporal/, http/ — see the integration and e2e skills
```

- One test: `.claude/scripts/tdd-run.sh ./access/usecase TestTurnTools_ClosedToolsAreLeftOut` · the
  module's unit suite: `--unit` with `TDD_MODULE=access` (engine §7)

## Quick Checklist

- [ ] Testing through a use case (domain service only when justified)
- [ ] `Test<Subject>_<FinalBehaviour>`, DR cited
- [ ] Real domain values; fakes only for ports, from `adapters/memory`
- [ ] No `time.Now()` — `memory.Clock`; time-bound rules tested on both sides of their threshold
- [ ] Assertions on snapshots; no mocking library
- [ ] < 10 ms, memory only
- [ ] No defensive code, getters or setters without a failing test

## Related Skills

- **`tdd-workflow-engine`** — TDD sequence, clean GREEN, violations
- **`tdd-analysis`** — test list, TPP, naming, status vocabulary
- **`tdd-testing-patterns`** — test levels, doubles, fixtures, assertions
- **`tdd-integration-patterns`** / **`tdd-e2e-patterns`** — adapter and HTTP levels
