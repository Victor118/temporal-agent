---
name: tdd-testing-patterns
description: Test levels, doubles, fixtures and assertions for the Go hexagonal modules — package testing, table-driven tests, hand-written fakes, the clock as a port, snapshot comparisons
---

# Skill: TDD Testing Patterns

**Focus:** HOW to write test code — levels, naming, doubles, fixtures, assertions — for the Go hexagonal
modules (`<module>/domain`, `usecase`, `adapters/…`; one module per context of `docs/fonctionnel/`, rules
cited as `DR6`; examples use `access`, the Droits context). Doctrine: `.claude/rules/DOCTRINE.md`.
The TEST FIRST sequence and violations (V0–V8) belong to `tdd-workflow-engine`; test lists, TPP order and
statuses (`⏳ Pending` · `✅ GREEN` · `➕ PROPOSED — <why>` · `🧬 PINNED — <mutation>`) belong to
`tdd-analysis`. Type and function names below are illustrative.

## Test Levels (this section owns the distinction)

Go has one test runner; the **level is the package** a test lives in. No build tag, no `-short`.

| Level | Package | Runs against | Without its infrastructure |
|---|---|---|---|
| unit | `<module>/domain`, `<module>/usecase` | fakes of `adapters/memory`, the fixed clock | always runs |
| integration | `<module>/adapters/postgres` | a throwaway PostgreSQL (`TEST_DATABASE_URL`) | `t.Skip` → `tdd-run.sh` says `INFRA_ERROR` |
| integration | `<module>/adapters/temporalio` | the SDK's `testsuite` (in process) | always runs |
| e2e | `<module>/adapters/httpapi` | `httptest` on the module wired with `memory` adapters | always runs |
| smoke | the existing `…RealServer` tests (`workflow/`, `gateway/`) | a real Temporal server (`TEMPORAL_SMOKE_HOST`) | skipped; long, run by hand (`CLAUDE.md`) |

`tdd-run.sh --unit` runs every package of the module but `adapters/`; `--all` runs everything, and the
tests needing a database or a Temporal server skip there (counted, not failed). A test that needs an
environment variable skips without it — exactly like `store/postgres_test.go` (`TEST_DATABASE_URL`).

Files: `<file under test>_test.go`, beside the code. Use-case and e2e tests are **black-box**:
`package usecase_test`, `package http_test` (they see only the exported API, and may import
`adapters/memory` without an import cycle). A domain-service test may be `package domain`.
A function runs only if it is `func TestXxx(t *testing.T)` in a `_test.go` file — `Testxxx` (lower case)
or a missing `*testing.T` never runs: check the name before trusting a green (`go vet` reports it).

## Test Naming

`Test<Subject>_<FinalBehaviour>`, English CamelCase, business language; `tdd-analysis` §1 owns the rule.

```go
// GOOD
func TestAllowed_EveryRuleIsALock(t *testing.T)            // DR6
func TestTurnTools_ClosedToolsAreLeftOut(t *testing.T)     // DR11

// BAD
func TestAllowed2(t *testing.T)                            // what behaviour?
func TestTurnTools_ReturnsErrorWhenNil(t *testing.T)       // technical words
```

Example rows of one behaviour are a table with `t.Run`, row names in plain words:

```go
func TestAllowed_EveryRuleIsALock(t *testing.T) { // DR6
	rules := []domain.Rule{onServer("crm", "sales"), onPattern("crm_update_*", "managers")}
	for _, row := range []struct {
		name string
		user domain.User
		tool domain.Tool
		want bool
	}{
		{"a seller searches the CRM", seller, crmSearch, true},
		{"a seller cannot update it", seller, crmUpdate, false},
		{"a seller who manages may", sellerManager, crmUpdate, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			if got := domain.Allowed(row.user, row.tool, rules); got != row.want {
				t.Errorf("allowed = %v, want %v", got, row.want)
			}
		})
	}
}
```

Given/When/Then helpers are English (`givenARuleOn`, `whenBobOpensATurn`, `thenTheToolsAre`) and call
`t.Helper()` so a failure points at the test line.

## Test Doubles (No Mocking Library)

```
What dependency?
├── Store of aggregates   → Fake in adapters/memory (snapshot copy on write and read)
├── Clock                 → memory.Clock, fixed, set in the Given
├── ID generation         → Stub (predictable IDs)
├── Collaborator port     → Fake or Spy (notifier, another module behind a port)
└── Domain type / VO      → Real value — NEVER doubled
```

Fakes are production code in `<module>/adapters/memory/`, so unit tests, e2e tests and a dev wiring share
them; each asserts its port at compile time. Hand-written: no testify mock, no gomock, no generated code.

## Store Fake — Snapshot Pattern (Mandatory)

```go
// access/usecase/rule_book.go — the port, declared by its consumer
type RuleBook interface {
	Save(ctx context.Context, rule domain.Rule) error
	Get(ctx context.Context, id domain.RuleID) (domain.Rule, bool, error)
}

// access/adapters/memory/rule_book.go
var _ usecase.RuleBook = (*RuleBook)(nil)

type RuleBook struct{ rules map[domain.RuleID]domain.RuleSnapshot }

func NewRuleBook() *RuleBook { return &RuleBook{rules: map[domain.RuleID]domain.RuleSnapshot{}} }

func (b *RuleBook) Save(_ context.Context, rule domain.Rule) error {
	s := rule.Snapshot() // a copy: changing rule afterwards changes nothing here
	b.rules[s.ID] = s
	return nil
}

func (b *RuleBook) Get(_ context.Context, id domain.RuleID) (domain.Rule, bool, error) {
	s, ok := b.rules[id]
	if !ok {
		return domain.Rule{}, false, nil
	}
	return domain.FromSnapshot(s), true, nil // copy on read
}

// Snapshots is for tests only: not on the port.
func (b *RuleBook) Snapshots() []domain.RuleSnapshot {
	all := slices.Collect(maps.Values(b.rules))
	slices.SortFunc(all, func(a, b domain.RuleSnapshot) int { return cmp.Compare(a.ID, b.ID) })
	return all
}
```

**Why:** the use case must call `Save` explicitly; a loaded copy changed without `Save` changes nothing;
no reference leaks between test steps. A snapshot holding a slice clones it (`slices.Clone`).
**Reads are deterministic:** a fake or snapshot built from a map sorts — never assert on map order.
**No `Reset()` or event lists** on a fake unless a test demands it (engine V6).

## Time Is a Port

```go
// access/usecase/clock.go
type Clock interface{ Now() time.Time }

// access/adapters/memory/clock.go
type Clock struct{ At time.Time }

func (c *Clock) Now() time.Time { return c.At }
```

Time-bound rules are tested **on both sides of the threshold**, inclusivity decided explicitly and
written in the test list:

```go
var now = time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)

func TestTurnTools_ARuleAppliesToTheTurnsStartingFromItsChange(t *testing.T) { // DR15
	clock := &memory.Clock{At: now}
	… a rule closing exec, changed at now …
	if tools := whenATurnStartsAt(t, now); slices.Contains(tools, "exec") { t.Errorf(…) }
}

func TestTurnTools_ATurnStartedBeforeTheChangeKeepsItsTools(t *testing.T) { // DR15
	… the same rule, the turn started at now.Add(-time.Second) …
}
```

## Assertions

- Compare **whole values** in one assertion: `==` on a comparable snapshot; `slices.Equal` for a slice of
  comparable values; `reflect.DeepEqual` for anything else (maps, nested slices). Print both sides:
  `t.Errorf("rules = %+v, want %+v", got, want)`.
- `t.Fatalf` when the rest of the test cannot run (an unexpected error, a missing value); `t.Errorf`
  otherwise, so several differences show at once.
- No assertion library and no new dependency (`go-cmp` included) unless the user decides it; say so first.
- Assert on `Snapshot()`, consistent with the equality choice already made (`DOCTRINE.md`).
- Snapshots carry raw values (`"exec"`, `time.Time`, `42`), never formatted French.

## Use Case Results and Refusals

The refusal shape (sentinel or typed error) is chosen at the first refusal test, then kept. Whatever the
shape, assert the **specific** refusal and **no side effect**:

```go
err := deleteGroup.Run(ctx, usecase.DeleteGroup{Group: "sales"})
if !errors.Is(err, domain.ErrSoleHolder) {          // or errors.As(err, &soleHolder)
	t.Fatalf("err = %v, want ErrSoleHolder", err)
}
if got := groups.Snapshots(); !reflect.DeepEqual(got, before) {
	t.Errorf("groups changed: %+v", got)             // nothing deleted
}
```

## Spy and Stub

```go
type Alerts struct{ sent []domain.Alert } // spy: a collaborator port

func (a *Alerts) Send(_ context.Context, alert domain.Alert) error { a.sent = append(a.sent, alert); return nil }
func (a *Alerts) Sent() []domain.Alert                         { return slices.Clone(a.sent) }

type IDs struct{ Next domain.RuleID } // stub

func (s *IDs) NewRuleID() domain.RuleID { return s.Next }
```

Assert on what the spy received (`slices.Equal(alerts.Sent(), want)`), not on call counts, unless the
count is the rule (DR34: "une seule fois").

## Domain Events

Nothing exists yet. Introduce event machinery **only when a test asserts an event**, in the simplest
shape it needs (often a spy port). Never add event lists to aggregates or fakes ahead of it (engine V6).

## Fixtures

Given data through intention helpers and small constructors (Object Mother) returning real aggregates via
`domain.FromSnapshot(...)` or the domain's constructor; a builder only when variations multiply.
Seed date-sensitive data **relative to the fixed clock** (`now.Add(-24 * time.Hour)`), never absolute dates
that expire. In e2e and integration (shared server or database), a test asserting exact content uses its
**own IDs** (`zz-…`, as `store/` tests do).

## Anti-Patterns

- A mocking library, or doubling a domain type — use real values.
- Several behaviours in one test — one behaviour per test; rows of one behaviour in one table.
- `time.Now()` in domain, use case or test — go through `Clock`.
- Testing only one side of a time threshold.
- Asserting on map iteration order.
- Fakes in `_test.go` files that the composition root and other levels cannot reuse.
- `t.Parallel()` on tests sharing a fake or a database without isolating their data.

## Quick Checklist

- [ ] `Test<Subject>_<FinalBehaviour>`, English, business words, rule cited in the list (`DR11`)
- [ ] Right package for the level; black-box `_test` package for use cases and HTTP
- [ ] Real domain values; fakes, spies, stubs from `adapters/memory`
- [ ] Whole-value comparison on snapshots; refusals with `errors.Is` / `errors.As` and no side effect
- [ ] Time via `Clock`, both sides of each threshold
- [ ] Package `testing` only; no new dependency unannounced

## Related Skills

- **`tdd-workflow-engine`** — TDD sequence, violations, `tdd-run.sh`
- `tdd-analysis` — test lists, TPP ordering, test type detection, statuses
- `tdd-core-patterns` — what to test (sociable tests through use cases)
- `tdd-integration-patterns` / `tdd-e2e-patterns` — adapters and HTTP
