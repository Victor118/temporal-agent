# Clean Architecture Rules

## The Dependency Rule

**THE OVERRIDING RULE**: Dependencies can ONLY point inward.

```
External World (Infrastructure)
    ↓
Adapters (Primary & Secondary)
    ↓
Application (Use Cases)
    ↓
Domain (Core Business)
```

### Non-Negotiable Principles

1. **Domain has ZERO external dependencies**
   - No framework imports
   - No infrastructure concepts
   - No external libraries
   - Pure business logic only

2. **Dependencies point inward ALWAYS**
   - Outer layers depend on inner layers
   - Inner layers know nothing about outer layers
   - No circular dependencies

3. **Infrastructure adapts to domain**
   - Domain defines ports (interfaces)
   - Infrastructure implements adapters
   - Never the reverse

4. **Use cases orchestrate, don't contain business logic**
   - Business rules live in domain
   - Use cases coordinate domain objects
   - Transaction boundaries at use case level

## Layer Responsibilities

### Domain Layer (Innermost)
- Pure business logic
- Aggregates, Entities, Value Objects
- Domain Services
- Domain Events
- Repository Interfaces (Ports)

### Application Layer
- Use Cases / Application Services
- Commands and Queries (CQRS)
- Input/Output Ports
- Orchestration logic
- Transaction management

### Infrastructure Layer (Outermost)
- Repository Implementations
- Web Controllers
- External Service Clients
- Message Publishers/Consumers
- Framework-specific code

## Boundary Crossing Rules

### Data Crossing Boundaries
✅ **DO**: Use simple DTOs, Value Objects, or primitives
❌ **DON'T**: Pass entities, framework objects, or database rows

### Dependency Inversion
- Inner layers define interfaces
- Outer layers implement them
- Flow of control can oppose dependency direction

### Port and Adapter Pattern
- **Ports**: Interfaces defined by domain/application
- **Adapters**: Implementations in infrastructure
- Primary Adapters: Drive the application (Controllers, CLI)
- Secondary Adapters: Driven by application (Repositories, External APIs)

## Testing Implications

- **Domain tests**: No external dependencies, pure unit tests
- **Application tests**: Mock only ports (repository interfaces)
- **Infrastructure tests**: Test with real infrastructure (TestContainers)

## Common Violations to Reject

❌ JPA annotations in domain entities
❌ Spring dependencies in use cases
❌ HTTP concepts in application layer
❌ Database queries in domain services
❌ Business logic in controllers
❌ Framework exceptions in domain

## The Litmus Test

Before writing any code, ask:
- Can I test this without a framework?
- Can I swap the database without changing domain?
- Can I change the web framework without touching use cases?
- Is my domain expressing business concepts only?

If any answer is "no", the architecture is compromised.