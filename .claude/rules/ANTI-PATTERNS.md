# Anti-Patterns to Reject

## Domain Anti-Patterns

### Anemic Domain Model
❌ Data classes with only getters/setters
❌ Business logic in services outside entities
❌ Entities as DTOs
✅ Rich objects with behavior and invariants

### Large Aggregates
❌ Including entire object graphs
❌ Loading collections when only count needed
❌ Parent tracking all children
✅ Small aggregates, reference by ID

### Aggregate References
❌ `private Customer customer;`
❌ Direct object navigation between aggregates
✅ `private CustomerId customerId;`

### Bidirectional Associations
❌ Parent knows children, children know parent
❌ `@OneToMany` for navigation convenience
✅ Unidirectional, query when needed

## Architecture Anti-Patterns

### Smart Controllers
❌ Business logic in REST controllers
❌ Validation beyond input format in adapters
✅ Controllers only adapt HTTP to use cases

### Infrastructure Leak
❌ JPA annotations in domain entities
❌ Domain depending on Spring
❌ Database concepts in domain
✅ Pure domain, infrastructure adapts

### Shared Mutable State
❌ Static mutable fields
❌ Entities modifying other entities directly
✅ Immutable values, isolated aggregates

## Testing Anti-Patterns

See `tdd-testing-patterns` skill for comprehensive testing patterns and anti-patterns.

## Code Quality Anti-Patterns

### Primitive Obsession
❌ `String email`, `BigDecimal amount`
❌ Validation scattered everywhere
✅ Value objects with encapsulated validation

### Feature Envy
❌ Method uses another object's data extensively
❌ Reaching through objects for data
✅ Move behavior to where data lives

### God Classes
❌ One class doing everything
❌ 500+ line classes
✅ Single responsibility, cohesive classes

### Long Parameter Lists
❌ `createOrder(id, customer, date, items, discount, tax, shipping)`
✅ Parameter objects, builders for complex creation

## Integration Anti-Patterns

### Distributed Monolith
❌ Synchronous calls between bounded contexts
❌ Shared database between contexts
✅ Events, eventual consistency

### Missing ACL
❌ External model leaking into domain
❌ Direct use of third-party types
✅ Anti-corruption layer protects domain

### Chatty Integration
❌ Multiple calls to fulfill one use case
❌ N+1 query problems across contexts
✅ Aggregate data, design better boundaries