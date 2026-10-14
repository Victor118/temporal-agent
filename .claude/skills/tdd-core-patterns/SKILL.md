---
name: tdd-core-patterns
description: Strategic testing philosophy for the WealDoc business logic (businesslogic/) - sociable tests through use cases, outside-in TDD, time as a port, and core testing decisions
---

# Skill: TDD Core Patterns

## Purpose

Strategic testing philosophy for the business logic of the `appointments` module
(`com.wealcome.appointments.businesslogic`), using Martin Fowler's **sociable tests**. It defines
**what to test** and **how to approach** unit tests: through use cases, with real domain
collaborators and fake ports.

- **TDD sequence, violations:** `tdd-workflow-engine` · **TPP, test list, status vocabulary:** `tdd-analysis`.
- **Test doubles, fixtures, snapshot assertions:** `tdd-testing-patterns`.

> **Names below are illustrative, not design commitments.** `appointments` starts empty:
> `BookAppointmentUseCase`, `Appointment`, `AppointmentId`, `Slot`, `ConsultationReason`,
> `PatientId`, `FakeAppointmentRepository`, `PatientRestrictedException`, `DateProvider` only show
> the shape. Let the real names emerge from the tests and the business rules (`RG1`…`RG26`, `README.md`).

## Testing Philosophy: Outside-In, Sociable, Wishful Thinking

**Wishful thinking:** the test is written first, calling classes that do not exist yet — see
`tdd-workflow-engine` for the canonical sequence.

**Outside-In:**
- Start from the use case with a business scenario (one RG example)
- Let aggregates and value objects emerge from what the use case needs
- Begin with simple types, evolve to rich domain objects when a test demands it

**Sociable:**
- The use case works with **real** aggregates, entities and value objects
- Only **ports** are faked (repositories, clock, agenda, documents, notifications, secretariat)
- Tests describe business scenarios, not object structure

**What we avoid:** inside-out design (aggregates before behavior), solitary tests mocking domain
objects, production code before the test.

**Decision rule:** wishful thinking first, outside-in, sociable inside `businesslogic/`, isolated
from infrastructure.

## Core Principles

- < 10 ms per test, memory only — no database, network or file system
- Hand-written fakes, **no mocking framework**
- Fakes live in the production tree (`businesslogic/models/`) so `configuration/` and every test level reuse them
- **Time is a port:** domain and use cases never call `now()`; they read a `DateProvider`, faked by `DeterministicDateProvider`
- Assert through `toSnapshot()` (ENFORCEMENT.md). Aggregate equality (id-only vs full-state) is decided at the first test that needs it — then stay consistent
- Java 25, no `null` (use `Optional`), records for value objects, package-private by default
- **NEVER add defensive code (null checks, validation) or getters/setters unless a failing test demands it**

## Test Categories

### Primary target: use cases
- **Production:** `boundedcontexts/appointments/src/main/java/com/wealcome/appointments/businesslogic/usecases/<featurename>/XxxUseCase.java`
- **Test:** `boundedcontexts/appointments/src/test/java/com/wealcome/appointments/unit/businesslogic/usecases/<featurename>/XxxUseCaseTest.java`
- **Why:** validates complete business scenarios with low maintenance cost

### Secondary target: domain services (when justified)
- **Test:** `unit/businesslogic/models/<aggregate>/XxxTest.java`
- Only for complex isolated logic: calculations, cross-aggregate rules, logic that reads better specified alone

### Not tested directly: aggregates, entities, value objects
Their behavior and invariants are verified through use case scenarios. Isolated tests of
`Appointment` or `Slot` over-specify structure and break on every refactoring.

## Primary Testing Through Use Cases — Example

Test methods are short French `devrait_[résultat]`, with `_quand_[condition]` only when the result
alone does not distinguish two tests; no accents or apostrophes. Given-When-Then helpers are in
French; production code is in English.

```java
// boundedcontexts/appointments/src/test/java/com/wealcome/appointments/unit/businesslogic/usecases/bookappointment/BookAppointmentUseCaseTest.java

class BookAppointmentUseCaseTest {

    private final FakeAppointmentRepository appointmentRepository = new FakeAppointmentRepository();
    private final DeterministicDateProvider dateProvider = new DeterministicDateProvider();
    private final BookAppointmentUseCase bookAppointmentUseCase =
            new BookAppointmentUseCase(appointmentRepository, dateProvider);

    @Test
    void devrait_reserver_trois_creneaux_consecutifs_pour_un_motif_de_30_minutes() { // RG1, RG2, RG4
        dateProvider.currentDateTime = LocalDateTime.parse("2026-09-14T07:00:00");

        quandIlReserve(unPatient(), "2026-09-14T09:10:00", uneConsultationDe(3));

        alorsLeRendezVousEst(new AppointmentSnapshot(
                "rdv-1", "patient-1", "2026-09-14T09:10:00", 30, "CONFIRMED"));
    }

    @Test
    void devrait_refuser_la_reservation_quand_le_patient_est_restreint() { // RG6
        assertThatThrownBy(() -> quandIlReserve(unPatientRestreint(), "2026-09-14T09:10:00", uneConsultationDe(1)))
                .isInstanceOf(PatientRestrictedException.class);

        assertThat(appointmentRepository.allSnapshots()).isEmpty(); // no side effect
    }
}
```

### Time-bound rules: both sides of the threshold

RG14–RG17, RG20–RG22, RG24–RG25 each get **two tests**, one on each side of the edge, driven by
the deterministic clock:

```java
@Test
void devrait_accepter_la_reservation_quand_le_rendez_vous_est_dans_2_heures() { // RG14
    dateProvider.currentDateTime = LocalDateTime.parse("2026-09-14T07:10:00");
    quandIlReserve(unPatient(), "2026-09-14T09:10:00", uneConsultationDe(1));
    alorsLeRendezVousEst(/* ... */);
}

@Test
void devrait_refuser_la_reservation_quand_le_rendez_vous_est_dans_1h59() { // RG14
    dateProvider.currentDateTime = LocalDateTime.parse("2026-09-14T07:11:00");
    assertThatThrownBy(() -> quandIlReserve(unPatient(), "2026-09-14T09:10:00", uneConsultationDe(1)))
            .isInstanceOf(BookingTooLateException.class);
}
```

## Secondary Testing — Domain Service (When Justified)

```java
// boundedcontexts/appointments/src/test/java/com/wealcome/appointments/unit/businesslogic/models/agenda/ConsecutiveSlotsTest.java

class ConsecutiveSlotsTest {

    @Test
    void devrait_trouver_les_creneaux_consecutifs_libres() { // RG3, RG4
        var agenda = unAgendaAvecCreneauxLibres("09:00", "09:10", "09:20");

        var slots = agenda.consecutiveFreeSlotsFrom(Slot.at("09:00"), 3);

        assertThat(slots).map(Slot::startsAt).containsExactly("09:00", "09:10", "09:20");
    }

    @Test
    void devrait_ne_rien_trouver_quand_un_creneau_intermediaire_est_pris() { // RG4
        var agenda = unAgendaAvecCreneauxLibres("09:00", "09:20");

        assertThat(agenda.consecutiveFreeSlotsFrom(Slot.at("09:00"), 2)).isEmpty();
    }
}
```

## Machinery Introduced Only When a Test Demands It

`appointments` has no CQRS split, outbox, transaction performer or base aggregate class. Do not create
them up front; add each one when a failing test needs it.

- **Domain events** (e.g. `AppointmentCancelled` for RG19/RG23): assert through a fake port that
  receives them (publisher or outbox fake), never by bloating the repository fake.
- **Read side / queries** (e.g. a patient's follow-up sheet, RG26): if a query use case appears,
  test it through its public method with the same outside-in discipline, but:
  - it returns a flat immutable record (a view), asserted directly — **no snapshot**;
  - build the expected view from the read shape, not from `Appointment.toSnapshot()`;
  - it is side-effect free: no events, no store mutation;
  - fake its read port in unit tests; real projection query in `integration/*IT.java`; `GET` over HTTP in `e2e/*E2ETest.java`;
  - if views are fed by events, test the **projection** and the **query** as separate units.

## Anti-Patterns

### Solitary tests on domain objects

```java
// BAD - mocking what we own
Appointment appointment = mock(Appointment.class);

// GOOD - real collaboration, fake ports only
quandIlReserve(unPatient(), "2026-09-14T09:10:00", uneConsultationDe(3));
assertThat(appointmentRepository.allSnapshots()).hasSize(1);
```

Full rationale and the test-double catalog: `tdd-testing-patterns` → *Test Doubles*.

### Real infrastructure or real time in unit tests

```java
// BAD
var entityManager = emf.createEntityManager();   // slow, not a unit test
LocalDateTime.now();                             // non-deterministic, threshold untestable

// GOOD
var appointmentRepository = new FakeAppointmentRepository();
var dateProvider = new DeterministicDateProvider();
```

## Test Organization and Commands

```
boundedcontexts/appointments/src/test/java/com/wealcome/appointments/
├── unit/businesslogic/
│   ├── usecases/bookappointment/BookAppointmentUseCaseTest.java   # PRIMARY
│   └── models/agenda/ConsecutiveSlotsTest.java                    # SECONDARY (justified only)
├── integration/   # *IT.java      — see tdd-integration-patterns
└── e2e/           # *E2ETest.java — fixtures in e2e/config/E2ETestConfiguration, see tdd-e2e-patterns
```

- One class: `.claude/scripts/tdd-run.sh BookAppointmentUseCaseTest` · whole unit suite: `--unit` (engine §7)
- Any other file name silently never runs.

## Quick Checklist

- [ ] Testing through a use case (domain service only when justified)
- [ ] French name `devrait_…` (`_quand_…` only if needed), RG cited
- [ ] Real domain objects; fakes only for ports, taken from `businesslogic/models/`
- [ ] No `now()` — `DeterministicDateProvider`; time-bound RG tested on both sides of its threshold
- [ ] Assertions via `toSnapshot()`; no mocking framework
- [ ] < 10 ms, memory only
- [ ] No defensive code, getters or setters without a failing test

## Related Skills

- **`tdd-workflow-engine`** — TDD sequence, clean GREEN, violations
- **`tdd-analysis`** — test list, TPP, naming, status vocabulary
- **`tdd-testing-patterns`** — test doubles, fixtures, snapshot assertions
- **`tdd-integration-patterns`** / **`tdd-e2e-patterns`** — adapter and HTTP levels
