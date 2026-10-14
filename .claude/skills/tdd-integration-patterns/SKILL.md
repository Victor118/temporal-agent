---
name: tdd-integration-patterns
description: Integration testing patterns for secondary adapters with testcontainers and real infrastructure
---

# Skill: TDD Integration Patterns

## Purpose

Integration tests validate **secondary adapters** (`Sql…` implementations of domain ports) against
real PostgreSQL via Testcontainers. They prove the mapping and the queries, not business rules:
RG1–RG26 are tested by unit tests through use cases with fakes (`tdd-core-patterns`).

For the TDD sequence, statuses and test-list rules, see `tdd-workflow-engine` and `tdd-analysis`.

## Scope

| Integration test | Not integration test |
|------------------|----------------------|
| `SqlAppointmentRepository`: save/byId round-trip, not found, overwrite by id, queries (e.g. a practitioner's appointments of a day) | Business rules (RG…) → unit tests with fakes |
| Column mapping of every snapshot field (`startsAt`, `durationMinutes`, `status`) | HTTP contract → `tdd-e2e-patterns` |
| Flyway migration matching the `…JpaEntity` (`ddl-auto=validate`) | Domain invariants, cross-aggregate rules |

## Conventions

- **Contexts:** examples use `appointments`; for `reliability`, substitute the module, package and schema.
- **Location & naming:** `boundedcontexts/<context>/src/test/java/com/wealcome/<context>/integration/`, mirroring the
  adapter's package (`integration/adapters/secondary/persistence/jpa/SqlAppointmentRepositoryIT`), class name
  `…IT.java` (Failsafe `**/*IT.java`). Any other name silently never runs; `mvn test` skips ITs.
- **Production layout:**
  ```
  boundedcontexts/appointments/src/main/java/com/wealcome/appointments/
  ├── businesslogic/models/appointment/ports/  AppointmentRepository (port)
  ├── adapters/secondary/persistence/fakes/   FakeAppointmentRepository
  ├── adapters/secondary/persistence/jpa/  SqlAppointmentRepository, AppointmentJpaEntity, AppointmentJpaRepository
  └── configuration/                  fakes declared under @Profile("fake")
  boundedcontexts/appointments/src/main/resources/db/migration/appointments/  V1__create_appointments.sql (schema only)
  ```
- **Adapters:** `@Repository @Profile("jpa")`, named `Sql…`. The persistence model (`…JpaEntity` +
  Spring Data `…JpaRepository`) is separate from the aggregate; JPA annotations never enter
  `businesslogic/`.
- **One SQL schema per bounded context, named after it** (`appointments`). The context owns it end to
  end in its `JpaPersistenceConfiguration`: its own `Flyway` bean (`schemas(SCHEMA)`,
  `locations("classpath:db/migration/" + SCHEMA)`, history table inside the schema) wrapped in a
  `FlywayMigrationInitializer`, and every `@Table` / `@CollectionTable` carries
  `schema = JpaPersistenceConfiguration.SCHEMA`. Migrations stay unqualified (Flyway sets the schema);
  raw SQL in ITs is qualified (`DELETE FROM appointments.daily_agendas`). No global `spring.flyway.*`.
- **Validation:** `spring.jpa.hibernate.ddl-auto=validate` (see
  `src/test/resources/application-jpa.properties`). Migrations never insert demo rows — demo seeds
  live in `wealdoc-app/.../configuration/`.
- **Command** (needs Docker):
  `.claude/scripts/tdd-run.sh SqlAppointmentRepositoryIT` (engine §7; `INFRA_ERROR` = Docker, not RED)
  — `fake`/`jpa` are Spring profiles (`@ActiveProfiles`), never Maven `-P`.
- **PIT runs unit tests only:** surviving mutants in `adapters.*` are expected; ITs cannot kill them.

## Boot Setup

`appointments` has no `@SpringBootApplication`, so a test-scoped one sits in the test's own package
(`integration/`, the level root — never deeper), scanning adapters and configuration only:

```java
// boundedcontexts/appointments/src/test/java/com/wealcome/appointments/integration/IntegrationTestApplication.java
@SpringBootApplication(scanBasePackages = {
        "com.wealcome.appointments.adapters",
        "com.wealcome.appointments.configuration"})
class IntegrationTestApplication {
}
```

One container per JVM (started once, reused by every IT class — `@Container` would restart it per
class):

```java
// boundedcontexts/appointments/src/test/java/com/wealcome/appointments/integration/PostgresIntegrationTest.java
@SpringBootTest(classes = IntegrationTestApplication.class)
@ActiveProfiles({"test", "jpa"})
public abstract class PostgresIntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }
}
```

Because the container and the Spring context are shared, **each IT cleans its own tables in
`@BeforeEach`** (prefer this over `@Transactional` rollback, which hides flush/mapping errors the
test is meant to catch).

## Repository Adapter Test

```java
// boundedcontexts/appointments/src/test/java/com/wealcome/appointments/integration/adapters/secondary/persistence/jpa/SqlAppointmentRepositoryIT.java
class SqlAppointmentRepositoryIT extends PostgresIntegrationTest {

    @Autowired
    private AppointmentRepository appointmentRepository; // the port; SqlAppointmentRepository under "jpa"

    @Autowired
    private JdbcClient jdbcClient;

    @BeforeEach
    void cleanTables() {
        jdbcClient.sql("DELETE FROM appointments").update();
    }

    @Test
    void devrait_retrouver_le_rendez_vous_enregistre() {
        var appointment = Appointment.book(
                new AppointmentId("a1"),
                new PractitionerId("p1"),
                new PatientId("pat1"),
                LocalDateTime.parse("2026-09-14T09:10:00")); // a 10-minute slot (RG1)

        appointmentRepository.save(appointment);

        assertThat(appointmentRepository.byId(new AppointmentId("a1")))
                .map(Appointment::toSnapshot)
                .contains(appointment.toSnapshot());
    }

    @Test
    void devrait_ne_rien_retourner_quand_le_rendez_vous_est_inconnu() {
        assertThat(appointmentRepository.byId(new AppointmentId("unknown"))).isEmpty();
    }

    @Test
    void devrait_ecraser_le_rendez_vous_quand_il_est_reenregistre() {
        var booked = Appointment.book(new AppointmentId("a1"), new PractitionerId("p1"),
                new PatientId("pat1"), LocalDateTime.parse("2026-09-14T09:10:00"));
        appointmentRepository.save(booked);
        var cancelled = booked.cancel(); // immutable: a new instance with the same id

        appointmentRepository.save(cancelled);

        assertThat(appointmentRepository.byId(new AppointmentId("a1")))
                .map(Appointment::toSnapshot)
                .contains(cancelled.toSnapshot());
    }
}
```

Names (`book`, `cancel`, statuses) are illustrative: `appointments` starts empty; use whatever the unit
tests have already driven. Aim for one round-trip per aggregate state that maps differently.

### Principles

1. **Round-trip through the port** — `save` then `byId`, assert the full `toSnapshot()`.
2. **Real domain objects, real database** — never a `Fake…` here.
3. **Seed reference data with raw SQL, read back through the port.** When the adapter under test
   only reads (e.g. a practitioner agenda), insert rows with `JdbcClient`, then query through the
   domain port. Seeding with the adapter's own `save` makes the test tautological: a symmetric
   mapping bug passes both ways.
   ```java
   jdbcClient.sql("INSERT INTO practitioners (id, name) VALUES (:id, :name)")
           .param("id", "p1").param("name", "Dr Martin").update();
   ```
4. **Deterministic reads** — if the aggregate holds elements in a `Set`, its snapshot sorts them;
   otherwise the IT flickers on row order. Queries returning lists declare an `ORDER BY`.
5. **Time is data** — timestamps come from literals or a `DeterministicDateProvider`, never `now()`.

## RED Phase: Scaffold Batch

Written with wishful thinking, the IT fails at context startup unless the scaffold batch includes
**everything the context needs**:

1. `SqlAppointmentRepository` (`@Repository @Profile("jpa")`) with behavioral-failure bodies
2. `AppointmentJpaEntity` + `AppointmentJpaRepository`
3. **The Flyway migration for the new table** — with `ddl-auto=validate`, a missing table kills
   the context before any assertion (a compilation/startup RED, not a behavioral RED)

```java
// boundedcontexts/appointments/src/main/java/com/wealcome/appointments/adapters/secondary/persistence/jpa/SqlAppointmentRepository.java
@Repository
@Profile("jpa")
class SqlAppointmentRepository implements AppointmentRepository {

    private final AppointmentJpaRepository appointmentJpaRepository;

    SqlAppointmentRepository(AppointmentJpaRepository appointmentJpaRepository) {
        this.appointmentJpaRepository = appointmentJpaRepository;
    }

    @Override
    public void save(Appointment appointment) {
        // scaffold: behavioral failure
    }

    @Override
    public Optional<Appointment> byId(AppointmentId id) {
        return Optional.empty(); // scaffold: behavioral failure
    }
}
```

```sql
-- boundedcontexts/appointments/src/main/resources/db/migration/appointments/V1__create_appointments.sql
CREATE TABLE appointments (
    id               VARCHAR(255) PRIMARY KEY,
    practitioner_id  VARCHAR(255) NOT NULL,
    patient_id       VARCHAR(255) NOT NULL,
    starts_at        TIMESTAMP    NOT NULL,
    duration_minutes INTEGER      NOT NULL,
    status           VARCHAR(50)  NOT NULL
);
```

The fake counterpart (`FakeAppointmentRepository` in `businesslogic/models/appointment/`) is declared in
`configuration/` under `@Profile("fake")`, so both profiles always have exactly one bean.

## Anti-Patterns

| ❌ | ✅ |
|----|----|
| Asserting a business rule (e.g. "slot already taken", RG…) in an IT | Unit test through the use case with fakes |
| `new FakeAppointmentRepository()` in an IT | Autowire the port, backed by PostgreSQL |
| Seeding with the adapter under test, then reading with it | Raw SQL seed via `JdbcClient`, read via the port |
| Relying on data left by another IT class (shared container) | `DELETE` own tables in `@BeforeEach` |
| Asserting one snapshot field after a round-trip | Assert the whole `toSnapshot()` |
| Demo rows in a Flyway migration | Seeds in `wealdoc-app/.../configuration/` |
| `ddl-auto=create-drop` to dodge a missing migration | Migration in the scaffold batch, `validate` |
| Tables in `public`, or `spring.flyway.*` shared by every context | The context's own schema and `Flyway` bean |
| `IntegrationTestApplication` scanning `com.wealcome.appointments` whole or in a subpackage | Test's own package, scanning `adapters` + `configuration` |

## Related Skills

- **`tdd-workflow-engine`** — TDD sequence, enforcement, test commands
- `tdd-analysis` — test lists, naming, statuses
- `tdd-testing-patterns` — test doubles, fixtures, assertions
- `tdd-core-patterns` — unit tests of the business logic (fakes)
- `tdd-e2e-patterns` — HTTP-boundary tests
