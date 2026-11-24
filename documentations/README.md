# Event Store Library Documentation

Welcome to the comprehensive documentation for the `logicblocks.event.store` library. This documentation provides detailed information about building event-sourced applications using this powerful and flexible event store implementation.

## 📚 Documentation Structure

### Getting Started
- **[Overview](./overview.md)** - High-level introduction with architecture diagrams and core concepts
  - Library architecture and components
  - Event sourcing fundamentals
  - Key features and capabilities
  - Usage patterns

### Core Concepts
- **[Events and Event Types](./events.md)** - Deep dive into event modeling
  - Event types (NewEvent, StoredEvent)
  - Event design patterns
  - Best practices for event modeling
  - Event versioning strategies

- **[Storage Adapters](./storage-adapters.md)** - Persistence layer documentation
  - In-memory adapter for testing
  - PostgreSQL adapter for production
  - Creating custom adapters
  - Performance optimization

- **[Projections](./projections.md)** - State derivation from events
  - Creating projectors
  - Projection patterns
  - Projection stores
  - Performance optimization

- **[Query System](./query-system.md)** - Finding and filtering data
  - Query syntax and operations
  - Advanced filtering
  - Performance considerations
  - Query patterns

### Advanced Topics
- **[Transactions](./transactions.md)** - Coordination and consistency
  - Transaction management
  - Error handling strategies
  - Retry patterns
  - Distributed transactions

- **[Testing](./testing.md)** - Testing strategies and utilities
  - Test builders and generators
  - Unit and integration testing
  - Performance testing
  - Best practices

### Reference
- **[API Reference](./api-reference.md)** - Complete API documentation
  - All classes and methods
  - Type definitions
  - Code examples
  - Parameter details

## 🚀 Quick Start

### Installation
```bash
pip install logicblocks-event-store
```

### Basic Example
```python
import asyncio
from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import NewEvent

async def main():
    # Create store with in-memory adapter
    adapter = adapters.InMemoryEventStorageAdapter()
    store = EventStore(adapter)
    
    # Get a stream reference
    stream = store.stream(category="users", stream="user-123")
    
    # Publish an event
    await stream.publish(events=[
        NewEvent(
            name="user-registered",
            payload={"email": "user@example.com"}
        )
    ])
    
    # Read events
    async for event in stream.scan():
        print(f"{event.name}: {event.payload}")

asyncio.run(main())
```

## 📖 How to Use This Documentation

1. **New to Event Sourcing?** Start with the [Overview](./overview.md) to understand the core concepts and architecture.

2. **Building an Application?** Follow this path:
   - [Events](./events.md) → [Storage Adapters](./storage-adapters.md) → [Projections](./projections.md)

3. **Need Specific Information?** Use the [API Reference](./api-reference.md) for detailed class and method documentation.

4. **Testing Your Application?** Check out [Testing](./testing.md) for utilities and strategies.

## 🔍 Common Use Cases

### Event Sourcing
Build applications where all changes are captured as events:
- [Event modeling patterns](./events.md#event-design-patterns)
- [Projection strategies](./projections.md#projection-patterns)
- [Query patterns](./query-system.md#query-patterns)

### CQRS Implementation
Separate read and write models:
- [Event store for writes](./events.md#working-with-events)
- [Projection store for reads](./projections.md#projection-store)
- [Query system](./query-system.md)

### Audit Logging
Track all changes with complete history:
- [Event metadata](./events.md#event-metadata)
- [Bi-temporal data](./events.md#event-types)
- [Stream scanning](./api-reference.md#eventstream)

## 💡 Best Practices Summary

1. **Event Design**
   - Keep events immutable
   - Use descriptive event names
   - Include only relevant data in payloads
   - Version events for evolution

2. **Performance**
   - Use appropriate storage adapters
   - Implement efficient projections
   - Optimize queries with indexes
   - Consider caching strategies

3. **Testing**
   - Use in-memory adapter for unit tests
   - Test projections independently
   - Verify event ordering
   - Performance test with realistic loads

4. **Production**
   - Use PostgreSQL adapter
   - Implement monitoring
   - Plan for event versioning
   - Consider backup strategies

## 🤝 Contributing

This library is open source. For contribution guidelines and development setup, see the main project README.

## 📄 License

This documentation is part of the logicblocks.event.store project, distributed under the MIT License.

---

*For the latest updates and additional resources, visit the [project repository](https://github.com/logicblocks/event.store).* 