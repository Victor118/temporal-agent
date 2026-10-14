---
name: tdd-e2e-patterns
description: E2E testing patterns for Spring Boot with HTTP-boundary black-box testing
---

# Skill: TDD E2E Patterns

## Purpose

End-to-End tests for WealDoc verify the **complete stack through HTTP**: request → controller →
use case → domain → secondary adapters (fake or real) → response. They never reach into domain
internals. TDD sequence and violations live in `tdd-workflow-engine`; test lists, naming and statuses
in `tdd-analysis`.

> Endpoint paths, controller names and HTTP statuses below are **illustrative**. The real ones are
> decided per iteration, from the business rules (`RG1`…`RG26`, `README.md`).

## Core Principle: HTTP Boundary Only

```
Tests interact ONLY via HTTP requests/responses.
NO import of aggregates, value objects or use cases in tests.
Importing port interfaces and Fake… implementations for fixture wiring IS allowed.
Reading a fake in an assertion is a last resort — prefer a GET through the API.
```

```
Test (MockMvc) ──HTTP──▶ Controller ──▶ UseCase ──▶ Domain models ──▶ Secondary adapters
                         adapters/primary/web   businesslogic/…            (Fake… or Sql…)
```

| Allowed | Forbidden |
|---------|-----------|
| `mockMvc.perform(post("/appointments"))` | `import com.wealcome.appointments.businesslogic.models.Appointment` |
| `andExpect(status().isCreated())` | `new AppointmentId(...)` |
| `jsonPath("$.startsAt").value("2026-09-14T09:10:00")` | `bookAppointmentUseCase.handle(command)` |
| `import ...businesslogic.models.DateProvider` (wiring) | Direct aggregate construction |
| `import ...businesslogic.models.DeterministicDateProvider` (fixtures) | Asserting on a domain object |

## Boot 4 Wiring

A context module (`appointments`, `reliability`; examples use `appointments`) has no `@SpringBootApplication` (it does not depend on `wealdoc-app`). E2E tests need a
**test-scoped** application class in the **test's own package** (`e2e/`, not a subpackage):

```java
// boundedcontexts/appointments/src/test/java/com/wealcome/appointments/e2e/E2ETestApplication.java
@SpringBootApplication(scanBasePackages = {
        "com.wealcome.appointments.adapters",
        "com.wealcome.appointments.configuration"
})
class E2ETestApplication {
}
```

Scan `adapters` and `configuration` only: scanning `com.wealcome.appointments` would also pick up
`IntegrationTestApplication`. MockMvc under Boot 4 needs the test-scoped
`spring-boot-starter-webmvc-test` and
`org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc` (the Boot 3 package does
not resolve).

Fixtures go in `e2e/config/E2ETestConfiguration`. Seed date-sensitive data **relative to the fake
clock**, so fixtures never expire:

```java
@TestConfiguration
public class E2ETestConfiguration {

    static final LocalDateTime NOW = LocalDateTime.of(2026, 9, 14, 8, 0);

    @Bean
    @Primary
    DeterministicDateProvider dateProvider() {
        return new DeterministicDateProvider(NOW);
    }

    // @Bean @Primary Fake… per port, seeded with dates computed from NOW (e.g. NOW.plusDays(1))
}
```

## Test Modes

### `fake` (default profile)

In-memory `Fake…` implementations (production tree, `businesslogic/models/`), deterministic clock,
**no Docker**. Target < 100ms per test; this is the TDD loop.

### `jpa`

Only this profile needs PostgreSQL, through Testcontainers (`Sql…` adapters in
`adapters/secondary/persistence/jpa/`). Adapter correctness is proven by `*IT.java` tests
(`tdd-integration-patterns`); a `jpa` E2E test is rare and only justified when the wiring itself
is at risk. Target < 5s per test.

## Shared Spring Context

The context is **shared across E2E classes** (same configuration → cached context). Consequences:

- A fake without `clear()` keeps data from other classes: a test asserting **exact content**
  (a full list, a count) must use a **dedicated id** nobody else touches.
- If a fake exposes `clear()`, call it in `@BeforeEach` — never hold a fake in a `static` field.

## Test Structure

```java
@SpringBootTest(classes = E2ETestApplication.class)
@AutoConfigureMockMvc
@Import(E2ETestConfiguration.class)
class BookAppointmentE2ETest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void devrait_reserver_un_creneau_libre() throws Exception {
        mockMvc.perform(post("/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                            {"appointmentId": "a1b2c3d4-0000-4000-8000-000000000001",
                             "patientId": "p-001", "practitionerId": "dr-martin",
                             "startsAt": "2026-09-15T09:10:00", "durationMinutes": 30}
                            """))
                .andExpect(status().isCreated());

        mockMvc.perform(get("/appointments/{id}", "a1b2c3d4-0000-4000-8000-000000000001"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.startsAt").value("2026-09-15T09:10:00"))
                .andExpect(jsonPath("$.durationMinutes").value(30));
    }

    @Test
    void devrait_refuser_quand_les_creneaux_ne_sont_pas_libres() throws Exception { // RG3
        // slot pre-booked in E2ETestConfiguration
        mockMvc.perform(post("/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(bookingOn("2026-09-15T10:00:00")))
                .andExpect(status().isConflict());
    }

    @Test
    void devrait_refuser_un_patient_restreint() throws Exception { // RG6
        mockMvc.perform(post("/appointments")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(bookingFor("p-restricted")))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status").value("RESTRICTED"));
    }
}
```

Illustrative statuses: `201` created, `409` slots not free (RG3), `403` restricted patient (RG6),
`422` for other business refusals, `404` unknown resource. Intention helpers (`bookingOn`,
`bookingFor`) are named in French in real tests.

### The back never formats

Assert on **raw values**: `durationMinutes: 30`, `startsAt: "2026-09-14T09:10:00"`,
`status: "RESTRICTED"`. Never `"30 min"`, `"lundi 14 septembre"` or a French label — French
belongs to the front.

### Request patterns

```java
mockMvc.perform(get("/appointments/{id}", appointmentId)).andExpect(status().isOk());
mockMvc.perform(get("/appointments").param("patientId", patientId)).andExpect(status().isOk());
mockMvc.perform(delete("/appointments/{id}", appointmentId)).andExpect(status().isNoContent());
```

## Verification

1. **Via the API (preferred)** — POST, then GET and assert the JSON.
2. **Via SQL (`jpa` only, when no read endpoint exists yet)** — `jdbcTemplate.queryForMap(...)`
   on the columns the use case must have written. Never in `fake` mode.

## File Organization and Build

```
boundedcontexts/appointments/src/test/java/com/wealcome/appointments/e2e/
├── E2ETestApplication.java
├── config/
│   └── E2ETestConfiguration.java
├── BookAppointmentE2ETest.java
└── CancelAppointmentE2ETest.java
```

- Naming: `{Action}{Resource}E2ETest.java`, under `e2e/`. Failsafe includes
  `**/e2e/**/*E2ETest.java`; any other name (`BookAppointmentE2E.java`, `…EndToEndTest.java`)
  **silently never runs** — green by absence.
- `mvn test` skips E2E (Surefire runs `**/unit/**/*Test.java` only). Use `mvn verify`.
- One class: `.claude/scripts/tdd-run.sh BookAppointmentE2ETest` (engine §7)
- Document each new endpoint in `http/wealdoc.http`.

## Anti-Patterns

| Wrong | Right |
|-------|-------|
| Calling `BookAppointmentUseCase` or building an `Appointment` in the test | HTTP only |
| `static` fake shared by hand between tests | Injected bean; `clear()` or dedicated id |
| Fixture date hard-coded in the past (`2025-01-10`) | Date computed from the fake clock |
| Asserting a formatted string (`"30 minutes"`) | Asserting the raw value (`30`) |
| `@CrossOrigin` on a controller | CORS only in `WebCorsConfiguration` (`dev`, `fake`) |
| `E2ETestApplication` scanning `com.wealcome.appointments` | Scan `adapters` + `configuration` |
| Demo seeds used as E2E fixtures | Test config feeds its own data (seeds live in `wealdoc-app`) |
| Business logic in the controller to make the test pass | Controller adapts HTTP to the use case |

## RED Phase for E2E Tests

Write the test as if the endpoint exists (wishful thinking), then scaffold just enough for a
**behavioral** failure, not a compilation or 404 one:

```java
// adapters/primary/web/BookAppointmentController.java
@RestController
class BookAppointmentController {

    @PostMapping("/appointments")
    ResponseEntity<Void> book(@RequestBody BookAppointmentRequest request) {
        return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build(); // scaffold
    }
}
```

GREEN wires it to `businesslogic/usecases/bookappointment/BookAppointmentUseCase.java`; the
use case and the fakes it needs are provided through `configuration/`.

## Related Skills

- **`tdd-workflow-engine`** — TDD sequence, violations, test commands
- `tdd-analysis` — test lists, naming, statuses
- `tdd-testing-patterns` — doubles, fixtures, assertions
- `tdd-core-patterns` — unit tests for `businesslogic`
- `tdd-integration-patterns` — `*IT.java` adapter tests with Testcontainers
