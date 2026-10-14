---
name: tdd-e2e-patterns
description: E2E testing patterns for a Go module's HTTP adapter - black-box tests through httptest on the wired module, and the RealServer smoke tests that exist
---

# Skill: TDD E2E Patterns

## Purpose

End-to-end tests verify a module **through HTTP**: request → handler (`adapters/httpapi`) → use case →
domain → secondary adapters (`memory` fakes) → response. They never reach into domain internals. TDD
sequence and violations live in `tdd-workflow-engine`; test lists, naming and statuses in `tdd-analysis`;
test levels in `tdd-testing-patterns`.

> Routes, handler names and statuses below are **illustrative**. The real ones are decided per iteration,
> from the rules (`docs/fonctionnel/<context>.md`) and the routes that exist (`cmd/agent/routes.go`,
> the JSON API under `/sessions`, `/api/admin`; the htmx pages are rendered by `web/chat` and `web/admin`).

## Core Principle: HTTP Boundary Only

```
Tests interact ONLY via HTTP requests and responses.
NO construction of aggregates or value objects, NO call to a use case in an assertion.
Importing usecase and adapters/memory to WIRE the module (and seed its fakes) IS allowed.
Reading a fake in an assertion is a last resort — prefer a GET through the API.
```

```
Test (httptest) ──HTTP──▶ handler ──▶ use case ──▶ domain ──▶ secondary adapters
                          adapters/httpapi   usecase/      domain/      (memory fakes)
```

| Allowed | Forbidden |
|---------|-----------|
| `httptest.NewRequest(http.MethodPost, "/admin/access/rules", body)` | `domain.FromSnapshot(...)` in an assertion |
| `rec.Code != http.StatusCreated` | `turnTools.Run(ctx, …)` to check the result |
| decoding the JSON into a local `struct` of raw values | importing `domain` to compare responses |
| `memory.NewRuleBook()` + `Feed(...)` for the Given | an aggregate built by hand to call the handler |

## Wiring

The test builds the module the way the composition root does, with `memory` adapters and the fixed clock,
and hands the handler to `httptest`: a recorder for one request, a server when the test needs a real
client (redirects, cookies, a stream).

```go
// access/adapters/httpapi/rules_test.go
package http_test

import (
	"net/http"
	"net/http/httptest"
	…
)

type app struct {
	rules   *memory.RuleBook
	handler http.Handler
}

func newApp(t *testing.T) *app {
	t.Helper()
	rules := memory.NewRuleBook()
	clock := &memory.Clock{At: time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC)}
	h := accesshttp.NewHandler(usecase.SaveRule{Rules: rules, Clock: clock}, usecase.Preview{Rules: rules})
	return &app{rules: rules, handler: h}
}

func (a *app) do(t *testing.T, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	a.handler.ServeHTTP(rec, req)
	return rec
}
```

Authentication and the admin role are the server's middleware (`auth/`, `requireMember`, the `admin`
check), not the module's handler: the e2e test of a module starts behind them; a test that the middleware
is mounted belongs where the routes are (`cmd/agent/routes_test.go`).

Seed date-sensitive data **relative to the fixed clock**, so fixtures never expire.

## Test Structure

```go
func TestRulesAPI_PreviewsTheToolsARuleWouldClose(t *testing.T) { // DR26
	a := newApp(t)
	a.givenTools("crm_search", "crm_update_contact", "web_fetch")

	rec := a.do(t, http.MethodPost, "/admin/access/preview", `{"kind":"mcp_server","target":"crm"}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", rec.Code, rec.Body)
	}
	var got struct {
		Tools []string `json:"tools"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if want := []string{"crm_search", "crm_update_contact"}; !slices.Equal(got.Tools, want) {
		t.Errorf("tools = %v, want %v", got.Tools, want)
	}
}

func TestRulesAPI_RefusesToDeleteTheSoleHolderOfARule(t *testing.T) { // DR32
	… rec.Code == http.StatusConflict, the rules to change named in the body …
}
```

Illustrative statuses, as the JSON API already answers: `201` created, `400` a malformed request, `403` a
right missing, `404` unknown (or not visible to this user), `409` a state that refuses (`ErrTurnOver`,
a sole holder), `429` a limit.

### The domain never formats

A JSON response carries **raw values**: `"target": "crm"`, `"since": "2026-10-14T10:00:00Z"`,
`"kind": "mcp_server"`. Never a French label: wording belongs to the pages (`web/…` templates) and the
channels. A page is not an e2e target of a module: its rendering is tested where it is built
(`web/chat/views_test.go`, `render_test.go`).

## RED Phase for E2E Tests

Write the test as if the route exists (wishful thinking), then scaffold just enough for a **behavioural**
failure, not a compile error or a 404:

```go
// access/adapters/httpapi/handler.go
func NewHandler(save usecase.SaveRule, preview usecase.Preview) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/access/preview", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotImplemented) // scaffold
	})
	return mux
}
```

GREEN decodes the request, calls the use case, maps its refusals to statuses and writes the raw values.
The handler is mounted by `cmd/agent` (a CHORE or the same BEHAVIOR iteration, decided in the plan).

## Smoke Tests Against a Real Temporal Server (described, not run by agents)

The repository has end-to-end smoke tests above this level, named `…RealServer`:
`workflow/` (participants, signals, continue-as-new, background tasks; ~6 min, `TEMPORAL_SMOKE_HOST`)
and `gateway/` (machines over a real WebSocket, a real Temporal server **and** a throwaway database;
~50 s, plus ~65 s for the model on a machine). Their exact commands are in `CLAUDE.md` (« Build & Run »);
they skip without their variables, so `tdd-run.sh --all` counts them as skipped. An agent never runs them
and never adds to them on its own: when an iteration changes what they cover (a participant's loop, a
workflow's commands, the gateway), VERIFY says they must be run by hand, and the user decides.

## File Organization

```
access/adapters/httpapi/
├── handler.go           routes of the module, NewHandler(use cases…)
├── rules.go             one file per resource
└── rules_test.go        package http_test: wiring helper + tests
```

- Naming: `Test<Resource>API_<FinalBehaviour>`; one test file per resource.
- One test: `.claude/scripts/tdd-run.sh ./access/adapters/httpapi TestRulesAPI_…` (engine §7). No
  infrastructure needed: e2e tests run in `--all`, not in `--unit`.

## Anti-Patterns

| Wrong | Right |
|-------|-------|
| Calling the use case or building an aggregate in the test | HTTP only |
| A package-level fake shared between tests | A fresh `newApp(t)` per test |
| A fixture date hard-coded in the past | A date computed from the fixed clock |
| Asserting a formatted string (`"il y a 5 min"`) | Asserting the raw value |
| Business logic in the handler to make the test pass | The handler adapts HTTP to the use case |
| Starting the whole server (`agent dev`) for a module's e2e | `httptest` on the module's handler |
| A `RealServer` test written or run by an agent | Described in the plan, run by the user |

## Related Skills

- **`tdd-workflow-engine`** — TDD sequence, violations, `tdd-run.sh`
- `tdd-analysis` — test lists, naming, statuses
- `tdd-testing-patterns` — test levels, doubles, fixtures, assertions
- `tdd-core-patterns` — unit tests of `domain/` and `usecase/`
- `tdd-integration-patterns` — `postgres` and `temporal` adapters
