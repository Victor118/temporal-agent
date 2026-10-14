---
name: tdd-testing-patterns
description: Test patterns, doubles, fixtures, and assertions for Clean Architecture
---

# Skill: TDD Testing Patterns

**Focus:** HOW to write test code — naming, doubles, fixtures, assertions — for WealDoc
(context modules `appointments` and `reliability`, packages `com.wealcome.<context>`, rules `RG1`…`RG26`
in `README.md`; examples use `appointments`).
The TEST FIRST sequence, state machine and violations (V0–V8) belong to `tdd-workflow-engine`;
test lists, TPP order and statuses (`⏳ Pending` · `✅ GREEN` · `➕ PROPOSED — <why>` ·
`🧬 PINNED — <mutation>`) belong to `tdd-analysis`. Names below are illustrative.

## Test Naming

Short French `devrait_[resultat]`; add `_quand_[condition]` only when the result alone does not
distinguish two tests. No accents, no apostrophes. Production code stays English.
Given-When-Then intention helpers are French too.

```java
// GOOD
@Test void devrait_reserver_le_creneau() { }
@Test void devrait_refuser_la_reservation_quand_le_creneau_commence_dans_moins_de_2h() { } // RG14

// BAD
@Test void testAppointment() { }   // what behavior?
@Test void bookAppointment() { }   // what outcome?
```

```java
@Test
void devrait_refuser_la_reservation_quand_le_patient_est_restreint() {
    unPatientRestreint();
    quandIlReserve(unCreneauLibre());
    alorsLeRendezVousEst(absent());
}
```

## Test Doubles (No Mocking Frameworks)

```
What dependency?
├── Repository              → Fake (in-memory, snapshot copy-on-read)
├── Clock                   → Deterministic fake (DateProvider)
├── Id generation           → Stub (predictable ids)
├── Collaborator port       → Fake or Spy (agenda, documents, notifications, secretariat)
└── Domain object / VO      → Real object — NEVER mock
```

Fakes live in the **production tree** (`adapters/secondary/persistence/fakes/`; their port in
`businesslogic/models/<aggregate>/ports/`), not the test tree, so
`configuration/` can wire them under `@Profile("fake")` and unit, integration and e2e tests reuse them.
Ports are domain interfaces; `Fake…` implements them for `fake`, `Sql…` (in
`adapters/secondary/persistence/jpa/`) for `jpa`.

## Repository Fake — Snapshot Pattern (Mandatory)

```java
public interface AppointmentRepository {
    void save(Appointment appointment);
    Optional<Appointment> findById(AppointmentId id);
}

public class FakeAppointmentRepository implements AppointmentRepository {
    private final Map<AppointmentId, AppointmentSnapshot> appointments = new HashMap<>();

    @Override
    public void save(Appointment appointment) {
        var snapshot = appointment.toSnapshot();
        appointments.put(new AppointmentId(snapshot.id()), snapshot);
    }

    @Override
    public Optional<Appointment> findById(AppointmentId id) {
        return Optional.ofNullable(appointments.get(id)).map(Appointment::fromSnapshot); // copy on read
    }

    public List<AppointmentSnapshot> allSnapshots() {
        return appointments.values().stream()
                .sorted(Comparator.comparing(AppointmentSnapshot::id))
                .toList();
    }
}
```

**Why:** the use case must call `save()` explicitly; a loaded copy mutated without `save()` changes
nothing; no shared references leak between test steps.

**Reads are deterministic:** whenever an aggregate or fake holds elements in a `HashSet`/`HashMap`,
the snapshot (or read method) sorts them — never assert on hash iteration order.

**No `clear()` or event lists** on a fake unless a test demands it (engine V6).

## Time Is a Port

Domain and use cases never call `now()`. They receive time from a port with a deterministic fake:

```java
public interface DateProvider {
    LocalDateTime now();
}

public class DeterministicDateProvider implements DateProvider {
    private LocalDateTime now;
    public DeterministicDateProvider(LocalDateTime now) { this.now = now; }
    public void setNow(LocalDateTime now) { this.now = now; }
    @Override public LocalDateTime now() { return now; }
}
```

Time-bound rules (RG14–RG17, RG20–RG22, RG24–RG25) are tested **on both sides of the threshold**,
and inclusivity is decided explicitly (and written in the test list) — e.g. RG14:

```java
private static final LocalDateTime NOW = LocalDateTime.of(2026, 9, 14, 8, 0);
private final DeterministicDateProvider dateProvider = new DeterministicDateProvider(NOW);

@Test void devrait_accepter_la_reservation_quand_le_creneau_commence_dans_exactement_2h() { // 10:00
    quandIlReserve(unCreneauA(NOW.plusHours(2)));
    alorsLeRendezVousEst(reserve());
}

@Test void devrait_refuser_la_reservation_quand_le_creneau_commence_dans_1h59() {           // 09:59
    quandIlReserve(unCreneauA(NOW.plusMinutes(119)));
    alorsLaReservationEstRefusee(TooLateToBook.class);
}
```

## Assertions on Snapshots

Per `ENFORCEMENT.md`: assert on `toSnapshot()`, not on the aggregate. Aggregate equality (id-only
+ snapshot assertions, or full-state equality + direct assertions) is **chosen at the first test
that needs it, then kept** — check existing aggregates before picking.

```java
// WRONG (under id-only equality) — passes as soon as ids match
assertThat(actual).isEqualTo(expected);

// RIGHT
assertThat(appointmentRepository.allSnapshots()).containsExactly(
    new AppointmentSnapshot("appointment-1", "patient-1", "2026-09-14T10:00", "FIRST_CONSULTATION", "BOOKED"));
```

Prefer one whole-snapshot comparison over field-by-field assertions. Snapshots carry raw values
(`startsAt: "2026-09-14T10:00:00"`, `status: "RESTRICTED"`) — never formatted French.

## Use Case Results and Refusals

The result shape (exception, sealed result, `Either`) is chosen at the first refusal test, then kept.
Whatever the shape, assert the **specific** refusal and **no side effect**:

```java
sealed interface BookAppointmentError permits RestrictedPatient, SlotUnavailable, TooLateToBook {}
record RestrictedPatient() implements BookAppointmentError {}
```

```java
// exceptions
assertThatThrownBy(() -> bookAppointment.handle(command)).isInstanceOf(TooLateToBookException.class);
// or sealed result
assertThat(result.error()).containsInstanceOf(TooLateToBook.class);

assertThat(appointmentRepository.allSnapshots()).isEmpty();   // nothing booked
```

## Spy and Stub

```java
public class SpyPatientNotifier implements PatientNotifier {        // collaborator port
    private final List<AppointmentConfirmation> sent = new ArrayList<>();
    @Override public void notify(AppointmentConfirmation confirmation) { sent.add(confirmation); }
    public List<AppointmentConfirmation> sent() { return List.copyOf(sent); }
}

public class StubAppointmentIdGenerator implements AppointmentIdGenerator {
    private String nextId = "appointment-1";
    public void setNextId(String nextId) { this.nextId = nextId; }
    @Override public AppointmentId generate() { return new AppointmentId(nextId); }
}
```

Assert on what the spy received (`containsExactly(...)`), not on call counts, unless the count is the rule.

## Domain Events

Nothing exists yet: no aggregate base class, no event publisher, no transaction performer, no outbox.
Introduce event machinery **only when a test asserts an event**, and design it at that moment (the
simplest shape the test needs). Never add event lists to aggregates or repository fakes ahead of that
test (engine V6).

## Fixtures

Build Given data through intention helpers and small factories (Object Mother) returning real
aggregates via `fromSnapshot(...)` or the domain factory. Use a builder only when variations multiply.

```java
private Patient unPatientRestreint() {
    var patient = Patient.fromSnapshot(new PatientSnapshot("patient-1", "RESTRICTED"));
    patientRepository.save(patient);
    return patient;
}

private Slot unCreneauA(LocalDateTime startsAt) {
    return new Slot(startsAt, Duration.ofMinutes(30));
}
```

- Seed date-sensitive fixtures **relative to the fake clock** (`NOW.plusDays(1)`), never absolute
  dates that expire.
- In e2e (shared Spring context, fixtures in `e2e/config/E2ETestConfiguration`), a test asserting
  exact content uses a **dedicated id**.

## Anti-Patterns

- `mock(Appointment.class)` or any mocking framework on domain objects/VOs — use real ones.
- Several behaviors in one test (book + cancel + reschedule) — one behavior per test.
- Calling `LocalDateTime.now()` in domain, use case or test — go through `DateProvider`.
- Testing only one side of a time threshold.
- Asserting on hash iteration order.
- Fakes in `src/test` that `configuration/` cannot wire.

## Quick Checklist

- [ ] Name `devrait_[resultat]` (`_quand_[condition]` only if needed), French helpers, cites `RGn` in the list
- [ ] Unit tests in `unit/businesslogic/...` named `*Test.java`
- [ ] Real domain objects; fakes/spies/stubs from `businesslogic/models/`
- [ ] Snapshot assertions, consistent with the equality choice already made
- [ ] Refusal tests assert the specific refusal and no side effect
- [ ] Time via `DateProvider`, both sides of each threshold
- [ ] JUnit 5 `@Test` + AssertJ, no null (Optional), records for VOs

## Related Skills

- **`tdd-workflow-engine`** — TDD sequence, violations, test commands
- `tdd-analysis` — test lists, TPP ordering, test type detection, statuses
- `tdd-core-patterns` — what to test (sociable tests), performance targets
