# Code Style Rules

## Naming Conventions

### Domain Objects
- **Aggregates**: Noun (Order, Customer, Subscription)
- **Value Objects**: Descriptive noun (Money, EmailAddress, OrderId)
- **Domain Services**: Verb phrase or policy (PricingPolicy, InventoryChecker)
- **Domain Events**: Past tense (OrderPlaced, PaymentReceived)

### Application Layer
- **Use Cases**: Imperative verb (PlaceOrder, CancelSubscription); a write use case is a command handler
- **Commands**: Action + "Command" (CreateOrderCommand)
- **Command handlers**: Command + "Handler" (CreateOrderCommandHandler), `handle(command)`
- **Queries**: Query + "Query" (GetOrderByIdQuery)

### Tests
- **Test methods**: short French `devrait_[résultat]`, `_quand_[condition]` only when needed (no accents/apostrophes); intention helpers in French, production code in English — see `tdd-analysis` skill §1
- **Test classes**: [ClassUnderTest]Test

## Java Principles

### Null Safety
- **NEVER** use null
- Use Optional for maybe-present values
- Use domain-specific null objects when appropriate
- Validate in constructors with Objects.requireNonNull(), only when a test proves necessity

### Immutability
- Immutable by default
- Use records for value objects
- Collections are immutable (List.of(), List.copyOf())
- State changes return new instances

### Visibility
- Package-private by default
- Public only when crossing package boundaries
- Private for internal implementation details
- Protected rarely (only for controlled extension)

### Object Construction
- Constructor validation ensures objects are always valid
- Use static factory methods for complex creation
- Builder pattern only when truly needed

## Code Organization

### Package Structure
- One aggregate per package
- Package by feature, not by layer within bounded context
- Clear boundaries between contexts

### Class Size
- Single Responsibility Principle
- Maximum ~100 lines per class (excluding imports/package)
- Extract when cohesion drops

### Method Size
- Maximum 10-15 lines
- Single level of abstraction
- Extract method if you need comments to explain a block

## Domain Specific Rules

### No Anemic Models
- Behavior lives with data
- Rich domain objects with business methods
- No public setters

### Tell, Don't Ask
- Objects do things, they don't expose data
- No getter chains
- Encapsulate decisions

### No Primitive Obsession
- Wrap primitives in value objects
- Type safety over convenience
