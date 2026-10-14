---
name: tdd-integration-patterns
description: Integration testing patterns for the secondary adapters of a Go module - adapters/postgres against a throwaway PostgreSQL, adapters/temporal with the SDK's testsuite
---

# Skill: TDD Integration Patterns

## Purpose

Integration tests validate **adapters** against real infrastructure: a `postgres` adapter (implementing a
port of `usecase/`) against a throwaway PostgreSQL, a `temporal` adapter (an activity or a workflow that
calls a use case) in the SDK's test environment. They prove the mapping, the queries and the wiring, not
business rules: DR1–DR36 are tested by unit tests through use cases with fakes (`tdd-core-patterns`).

For the TDD sequence, statuses and test-list rules, see `tdd-workflow-engine` and `tdd-analysis`; for the
test levels, `tdd-testing-patterns`.

## Scope

| Integration test | Not integration test |
|------------------|----------------------|
| `postgres.RuleBook`: save/get round-trip, not found, overwrite by ID, queries (the rules targeting a server, ordered) | Business rules (DR…) → unit tests with fakes |
| Column mapping of every snapshot field (`kind`, `target`, groups, users, `last_matched_at`) | HTTP contract → `tdd-e2e-patterns` |
| The table the adapter needs exists after migration (`CREATE TABLE IF NOT EXISTS`, the store's way) | Domain invariants, cross-aggregate rules |
| An activity adapter turns a use case's refusal into the error type the workflow reads | A workflow deciding a business rule (it must not) |

## PostgreSQL adapters

### Conventions

- **Location:** `<module>/adapters/postgres/<adapter>.go` and `<adapter>_test.go`, `package postgres`
  (or `postgres_test`). Driver: `database/sql` with `github.com/jackc/pgx/v5/stdlib`, like `store/`.
- **Throwaway database only**, never `agent`: the test reads `TEST_DATABASE_URL` and skips without it,
  exactly as `store/postgres_test.go` (`openTestStore`). Procedure (`CLAUDE.md`, « Build & Run »):
  ```
  docker compose exec -T postgres createdb -U agent agent_dev_tmp
  TDD_GO="docker compose -f ../docker-compose.yml exec -T -e TEST_DATABASE_URL=postgres://agent:agent@postgres:5432/agent_dev_tmp?sslmode=disable agent go" \
    .claude/scripts/tdd-run.sh ./access/adapters/postgres TestRuleBook_ReadsBackWhatItSaved
  docker compose exec -T postgres dropdb -U agent agent_dev_tmp
  ```
  Without `TEST_DATABASE_URL`, `tdd-run.sh` answers `INFRA_ERROR` (every test skipped): not a RED, never a
  GREEN. The compose stack is the user's to start; ask, never `docker compose up` on your own. Never run
  `store/` tests and gateway `RealServer` tests on the same database at once (both reset tables).
- **Schema:** the module's tables are created by its adapter's migration (`CREATE TABLE IF NOT EXISTS`,
  under the advisory lock `store/postgres.go` uses), and the composition root runs it at start-up. Where
  that migration lives (the adapter, or `store/`'s list while both coexist) is decided at the first
  integration test of the module and recorded in the plan's Decisions.
- **Each test isolates its data:** IDs of its own (`zz-…`), deleted before (a leftover from an aborted run)
  and after (`t.Cleanup`). No `TRUNCATE` of a table other tests use.

### Helper

```go
// access/adapters/postgres/postgres_test.go
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set: a throwaway database (CLAUDE.md, Build & Run)")
	}
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	return db
}
```

### Adapter test

```go
func TestRuleBook_ReadsBackWhatItSaved(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	const id = domain.RuleID(990001) // this test's own row
	removeRule(t, db, id)
	t.Cleanup(func() { removeRule(t, db, id) })
	book := NewRuleBook(db)
	rule := domain.FromSnapshot(domain.RuleSnapshot{ID: id, Kind: "mcp_server", Target: "crm",
		Groups: []string{"sales"}, Users: []string{"alice"}})

	if err := book.Save(ctx, rule); err != nil {
		t.Fatal(err)
	}

	got, ok, err := book.Get(ctx, id)
	if err != nil || !ok {
		t.Fatalf("get: %v, found %v", err, ok)
	}
	if !reflect.DeepEqual(got.Snapshot(), rule.Snapshot()) {
		t.Errorf("read back %+v, want %+v", got.Snapshot(), rule.Snapshot())
	}
}

func TestRuleBook_FindsNothingForAnUnknownRule(t *testing.T) { … ok == false, err == nil … }
```

Names are illustrative; use whatever the unit tests have already driven. Aim for one round-trip per
aggregate state that maps differently.

### Principles

1. **Round-trip through the port** — `Save` then `Get`, compare the whole `Snapshot()`.
2. **Real domain values, real database** — never a `memory` fake here.
3. **Seed reference data with raw SQL, read back through the port** (and the reverse: write through the
   port, read with raw SQL). Seeding with the adapter's own `Save` makes a read-only test tautological: a
   symmetric mapping bug passes both ways. No method is added to a port for a test.
4. **Deterministic reads** — a query returning a list has an `ORDER BY`; a snapshot built from a set sorts.
5. **Time is data** — timestamps come from literals or `memory.Clock`, never `now()` in SQL or `time.Now()`.
   PostgreSQL keeps microseconds: no nanosecond literal.

### RED phase: scaffold batch

Written by wishful thinking, the test fails at compile time until the scaffold batch adds the adapter
type with behavioural-failure bodies **and its table**: without the migration the test fails on
`relation does not exist`, a setup failure, not a behavioural RED.

```go
// access/adapters/postgres/rule_book.go
var _ usecase.RuleBook = (*RuleBook)(nil)

type RuleBook struct{ db *sql.DB }

func NewRuleBook(db *sql.DB) *RuleBook { return &RuleBook{db: db} }

func (b *RuleBook) Save(ctx context.Context, rule domain.Rule) error { return nil } // scaffold

func (b *RuleBook) Get(ctx context.Context, id domain.RuleID) (domain.Rule, bool, error) {
	return domain.Rule{}, false, nil // scaffold: behavioural failure
}
```

## Temporal adapters

A Temporal adapter is either an **activity** that calls a use case (primary: it drives the module) or a
**client** that a port hides (secondary: signal, start, query a workflow). Neither decides a rule.

- **Activity → use case:** `testsuite.WorkflowTestSuite` → `NewTestActivityEnvironment()`, register the
  activity struct wired with `memory` fakes, `env.ExecuteActivity(...)`, assert its result and the fakes'
  snapshots. A refusal must come out as the `temporal.ApplicationError` type the workflow reads (non
  retryable when it is final) — that mapping is what the test proves.
- **Workflow:** `NewTestWorkflowEnvironment()`, activities registered as **hand-written functions** under
  their real names (`env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: "NotifyStep"})`),
  as `workflow/ask_user_test.go` does — not `env.OnActivity` mocks.
- Time in a workflow is `workflow.Now`, driven by the test environment's clock; never `time.Now()`.
- A workflow's commands are part of its contract: changing them breaks the replay of open runs
  (`CLAUDE.md`, Conventions). Behaviour that only a real server shows (signals during the end of a
  workflow, continue-as-new) belongs to the `RealServer` smoke tests, run by hand, never by an agent.

```go
func TestTurnToolsActivity_ARefusalIsFinal(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	acts := &Activities{TurnTools: usecase.TurnTools{Rules: memory.NewRuleBook(), Users: closedDirectory(), Tools: toolbox()}}
	env.RegisterActivity(acts)

	_, err := env.ExecuteActivity(acts.TurnTools, StartTurnInput{Agent: "dev", User: "bob"})

	var appErr *sdktemporal.ApplicationError
	if !errors.As(err, &appErr) || appErr.Type() != "AgentNotOpen" || !appErr.NonRetryable() {
		t.Fatalf("err = %v, want a final AgentNotOpen", err)
	}
}
```

## Anti-Patterns

| ❌ | ✅ |
|----|----|
| Asserting a business rule (DR…) in an adapter test | Unit test through the use case with fakes |
| A `memory` fake in a `postgres` test | The adapter on the throwaway database |
| The `agent` database, or `DATABASE_URL` | `TEST_DATABASE_URL` on `agent_dev_tmp` |
| Seeding with the adapter under test, then reading with it | Raw SQL seed, read through the port |
| Relying on rows another test left | Own IDs, deleted before and after |
| Asserting one field after a round-trip | Compare the whole `Snapshot()` |
| `env.OnActivity(...).Return(...)` mocks | Hand-written activity functions registered by name |
| A workflow test that needs a real server | A `RealServer` smoke test, by hand |

## Related Skills

- **`tdd-workflow-engine`** — TDD sequence, enforcement, `tdd-run.sh`
- `tdd-analysis` — test lists, naming, statuses
- `tdd-testing-patterns` — test levels, doubles, fixtures, assertions
- `tdd-core-patterns` — unit tests of the business logic (fakes)
- `tdd-e2e-patterns` — HTTP-boundary tests
