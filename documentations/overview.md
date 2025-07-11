# Event Store Library Overview

## Introduction

The `logicblocks.event.store` library is a comprehensive eventing infrastructure designed for building event-sourced architectures. It provides a robust foundation for implementing event-driven systems with support for event storage, projections, querying, and more.

## Core Concepts

### Architecture Overview

```mermaid
graph TB
    %% Core Components
    ES[Event Store]
    EA[Event Adapter]
    EL[Event Log]
    EC[Event Category]
    EST[Event Stream]
    
    %% Storage Layer
    MA[Memory Adapter]
    PA[PostgreSQL Adapter]
    SA[Storage Adapter Interface]
    
    %% Processing Layer
    PR[Projector]
    PS[Projection Store]
    PSA[Projection Storage Adapter]
    
    %% Query Layer
    QS[Query System]
    
    %% Events
    E[Event]
    NE[New Event]
    SE[Stored Event]
    
    %% Relationships
    ES --> SA
    SA --> MA
    SA --> PA
    
    ES --> EL
    EL --> EC
    EC --> EST
    
    EST --> E
    E --> NE
    E --> SE
    
    PR --> EST
    PR --> PS
    PS --> PSA
    
    QS --> ES
    QS --> PS
    
    style ES fill:#f9f,stroke:#333,stroke-width:4px
    style PR fill:#9ff,stroke:#333,stroke-width:2px
    style QS fill:#ff9,stroke:#333,stroke-width:2px
```

### Event Model

The library uses a hierarchical event model:

```mermaid
graph TD
    L[Log] --> C1[Category: profiles]
    L --> C2[Category: orders]
    L --> C3[Category: payments]
    
    C1 --> S1[Stream: user-123]
    C1 --> S2[Stream: user-456]
    
    C2 --> S3[Stream: order-789]
    C2 --> S4[Stream: order-012]
    
    S1 --> E1[Event: profile-created]
    S1 --> E2[Event: profile-updated]
    S1 --> E3[Event: email-changed]
    
    S3 --> E4[Event: order-placed]
    S3 --> E5[Event: order-shipped]
```

### Event Lifecycle

```mermaid
sequenceDiagram
    participant App as Application
    participant ES as Event Store
    participant SA as Storage Adapter
    participant DB as Database
    
    App->>ES: Create NewEvent
    ES->>ES: Validate Event
    ES->>ES: Apply Write Conditions
    ES->>SA: Store Event
    SA->>DB: Persist Event
    DB-->>SA: Confirmation
    SA-->>ES: StoredEvent
    ES-->>App: Event Published
```

## Key Features

### 1. Event Storage
- **Immutable and Append-Only**: Events are never modified once stored
- **Bi-temporal Support**: Tracks both occurrence time and recording time
- **Consistency Guarantees**: Optimistic concurrency control for stream updates
- **Write Conditions**: Extensible pre-condition evaluation system
- **Ordering Guarantees**: Serialized writes ensure consistent event ordering

### 2. Storage Adapters
- **Pluggable Architecture**: Abstract base class for implementing custom adapters
- **In-Memory Adapter**: For testing and development
- **PostgreSQL Adapter**: Production-ready persistent storage
- **Extensible**: Easy to add support for other databases

### 3. Projections
- **Event Reduction**: Transform event sequences into meaningful state
- **Metadata Tracking**: Automatic version and timestamp management
- **Projection Store**: Built-in storage and querying for projections
- **Custom Projectors**: Define your own projection logic

### 4. Query System
- **Flexible Querying**: Rich query language for events and projections
- **Filtering**: Support for various filter operations
- **Ordering**: Control result ordering
- **Pagination**: Built-in support for paginated results

### 5. Transaction Support
- **Retry Logic**: Automatic retry on transient failures
- **Error Handling**: Configurable error handling strategies
- **Batch Operations**: Support for atomic multi-event operations

## Component Interaction

```mermaid
graph LR
    %% User interaction
    U[User Code] --> ES[Event Store]
    U --> PR[Projector]
    
    %% Event flow
    ES --> |publish| EST[Event Stream]
    EST --> |scan| E[Events]
    
    %% Projection flow
    PR --> |project| EST
    PR --> |store| PS[Projection Store]
    
    %% Query flow
    U --> |query| QS[Query System]
    QS --> ES
    QS --> PS
    
    %% Storage
    ES --> SA[Storage Adapter]
    PS --> PSA[Projection Adapter]
    
    style U fill:#ffd,stroke:#333,stroke-width:2px
    style ES fill:#f9f,stroke:#333,stroke-width:2px
    style PR fill:#9ff,stroke:#333,stroke-width:2px
```

## Usage Patterns

### 1. Event Sourcing Pattern
```
Application → NewEvent → EventStore → StoredEvent → Projector → Projection → Query
```

### 2. CQRS Pattern
```
Commands → EventStore (Write Model)
Queries → ProjectionStore (Read Model)
```

### 3. Event-Driven Architecture
```
Service A → Event → EventStore → Event Handler → Service B
```

## Next Steps

For detailed information on specific topics, see:

- [Events and Event Types](./events.md) - Deep dive into event modeling
- [Storage Adapters](./storage-adapters.md) - Working with different storage backends
- [Projections](./projections.md) - Building and managing projections
- [Query System](./query-system.md) - Querying events and projections
- [Transactions](./transactions.md) - Transaction support and error handling
- [Testing](./testing.md) - Testing utilities and best practices
- [API Reference](./api-reference.md) - Complete API documentation 