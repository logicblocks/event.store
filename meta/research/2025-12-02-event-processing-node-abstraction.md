---
date: 2025-12-02T01:39:06+00:00
researcher: Claude Code
git_commit: 2af43d9b064e568e635e157f18679509bae8e4f3
branch: event-processing-node-abstraction
repository: event.store
topic: "Event Processing Infrastructure and EventProcessingNode Abstraction Research"
tags: [ research, codebase, event-processing, event-broker, service-manager, subscribers, coordination, observation ]
status: complete
last_updated: 2025-12-02
last_updated_by: Claude Code
---

# Research: Event Processing Infrastructure and EventProcessingNode Abstraction

**Date**: 2025-12-02T01:39:06+00:00
**Researcher**: Claude Code
**Git Commit**: 2af43d9b064e568e635e157f18679509bae8e4f3
**Branch**: event-processing-node-abstraction
**Repository**: event.store

## Research Question

Perform deep research of the event processing infrastructure to understand:

1. All components relevant to subscribing and consuming from event sources
2. Patterns used for wiring components together
3. Key abstractions involved (EventBroker, ServiceManager, subscribers, etc.)
4. The coordinator vs observer patterns
5. Testing strategy employed

The goal is to inform the design of an `EventProcessingNode` abstraction that:

- Sits over the top of the event broker and service manager
- Makes it simpler to register subscribers
- Encapsulates the broker instance
- Supports three node modes: coordinator-only, observer-only, or both

## Summary

The codebase implements a sophisticated distributed event processing system with
clear separation of concerns:

1. **EventBroker**: Coordinates work allocation among subscribers using two
   strategies:
  - `SingletonEventBroker`: For single-process scenarios
  - `DistributedEventBroker`: For multi-node distributed scenarios with leader
    election

2. **ServiceManager**: Orchestrates service lifecycle with execution modes (
   foreground/background) and isolation modes (main thread, shared thread,
   dedicated thread)

3. **Subscriber/Consumer Abstractions**: Two-tier consumer model with
   `EventSubscriptionConsumer` (broker-mediated) delegating to
   `EventSourceConsumer` (direct source consumption)

4. **Coordinator/Observer Pattern**: The distributed broker separates:
  - **Coordination**: Single leader allocates work via distributed lock
  - **Observation**: All nodes apply allocation changes locally

5. **Testing Strategy**: Three-tier approach (unit, integration, component) with
   extensive use of mocks, test doubles, and real PostgreSQL for integration
   tests

## Detailed Findings

### EventBroker Component

The EventBroker is the central coordination mechanism for event distribution.

#### Base Interface

**File**: `src/logicblocks/event/processing/broker/base.py:11-14`

```python
class EventBroker[E](Service[NoneType], Process, ABC):
    @abstractmethod
    async def register(self, subscriber: EventSubscriber[E]) -> None:
        raise NotImplementedError
```

#### Strategy Implementations

**SingletonEventBroker** (`broker/strategies/singleton/broker.py:26`):

- Simple in-process distribution
- Maintains in-memory subscriber store
- Periodically iterates subscribers and distributes event sources
- Configuration: `distribution_interval` (default 60 seconds)

**DistributedEventBroker** (`broker/strategies/distributed/broker.py:19`):

- Three concurrent components:
  1. `EventSubscriberManager`: Manages subscriber lifecycle and heartbeats
  2. `EventSubscriptionCoordinator`: Leader-elected work allocator
  3. `EventSubscriptionObserver`: Applies allocation changes locally

#### Coordinator vs Observer Architecture

| Aspect             | Coordinator                           | Observer                |
|--------------------|---------------------------------------|-------------------------|
| **File**           | `coordinator.py:112`                  | `observer.py:28`        |
| **Concurrency**    | One active (leader)                   | All nodes active        |
| **State Access**   | Read subscribers, write subscriptions | Read subscriptions only |
| **Responsibility** | Decide work allocation                | Apply work allocation   |
| **Uses Lock**      | Yes (leader election)                 | No                      |
| **Operates On**    | All subscribers globally              | Local subscribers only  |

**Coordinator Work Distribution** (`coordinator.py:199-364`):

1. Fetches existing subscriptions and active subscribers
2. Groups subscribers by `subscriber_group`
3. For each group, identifies eligible subscribers (matching subscription
   requests)
4. Chunks new event sources across eligible subscribers using round-robin
5. Creates ADD/REMOVE/REPLACE changes and applies atomically

**Observer Synchronisation** (`observer.py:77-113`):

1. Fetches updated subscriptions from state store
2. Computes diff against cached subscriptions
3. Calls `subscriber.withdraw(source)` for revocations
4. Calls `subscriber.accept(source)` for allocations

#### Configuration Options

**DistributedEventBrokerSettings** (`distributed/builder.py:22-33`):

- `subscriber_manager_heartbeat_interval`: 10 seconds
- `subscriber_manager_purge_interval`: 1 minute
- `subscriber_manager_subscriber_max_age`: 10 minutes
- `coordinator_distribution_interval`: 20 seconds
- `coordinator_leadership_max_duration`: 15 minutes
- `observer_synchronisation_interval`: 20 seconds

---

### ServiceManager Component

**File**: `src/logicblocks/event/processing/services/manager.py:174`

The ServiceManager orchestrates service lifecycle with configurable execution
and isolation modes.

#### Service Protocol

```python
class Service[T = Any](ABC):
    @abstractmethod
    async def execute(self) -> T:
        raise NotImplementedError()
```

#### Execution Modes (`manager.py:14-16`)

- **FOREGROUND**: `ServiceManager.start()` blocks until service completes
- **BACKGROUND**: Service runs concurrently without blocking

#### Isolation Modes (`manager.py:19-22`)

- **MAIN_THREAD**: Runs on main event loop thread via `asyncio.create_task()`
- **SHARED_THREAD**: Multiple services share a background thread with `uvloop`
- **DEDICATED_THREAD**: Each service gets its own background thread

#### Service Lifecycle

1. **Registration**:
   `manager.register(service, execution_mode, isolation_mode)` - stores service
   definition
2. **Start**: `await manager.start()` - starts executor, schedules all services,
   waits for foreground
3. **Stop**: `await manager.stop()` - cancels tasks, shuts down thread pools

#### Signal Handling

```python
manager.stop_on([signal.SIGINT, signal.SIGTERM])
```

Configures graceful shutdown on OS signals.

---

### Subscriber/Consumer Abstractions

#### Two-Tier Consumer Model

**EventSubscriptionConsumer** (`consumers/subscription.py:95`):

- Receives event sources from broker via `accept(source)` / `withdraw(source)`
- Maintains dict of delegates keyed by source identifier
- `consume_all()` iterates all delegates and calls their `consume_all()`

**EventSourceConsumer** (`consumers/source.py:139`):

- Consumes events from a single `EventSource`
- Tracks state via `EventConsumerStateStore`
- Supports three processor types:
  - `EventProcessor`: Simple callback-based
  - `AutoCommitEventIteratorProcessor`: Auto-acknowledges each event
  - `ManagedEventIteratorProcessor`: Manual acknowledgment control

#### Factory Pattern: `make_subscriber`

**File**: `consumers/subscription.py:54-92`

Creates a fully-configured subscription consumer:

1. Generates subscriber ID (UUID)
2. Creates `EventConsumerStateStore` with persistence interval
3. Defines delegate factory closure capturing shared state
4. Returns `EventSubscriptionConsumer` with factory

#### Subscription Request Types

**CategoryIdentifier** (`types/identifier.py:84-98`):

```python
CategoryIdentifier(category="orders")
```

**StreamIdentifier** (`types/identifier.py:127-150`):

```python
StreamIdentifier(category="orders", stream="order-123")
```

**LogIdentifier** (`types/identifier.py:49-61`):

```python
LogIdentifier()  # Entire event log
```

#### State Persistence

**EventConsumerStateStore** (`consumers/state/base.py:76`):

- `record_processed(event)`: Records event processing with lag counter
- `save_if_needed()`: Saves when lag >= persistence_interval
- `save()`: Forces immediate save with optimistic concurrency
- `load_to_query_constraint()`: Returns resumption constraint for iteration

---

### Error Handling and Polling Services

#### Error Handler Protocol

```python
class ErrorHandler[T](ABC):
    @abstractmethod
    def handle(self, exception: BaseException) -> ErrorHandlerDecision[T]:
        raise NotImplementedError
```

#### Concrete Handlers

| Handler                   | Behaviour                                       |
|---------------------------|-------------------------------------------------|
| `ExitErrorHandler`        | Raises `SystemExit` with configurable exit code |
| `RaiseErrorHandler`       | Re-raises exception (optionally transformed)    |
| `ContinueErrorHandler`    | Returns default value, continues execution      |
| `RetryErrorHandler`       | Retries operation in loop                       |
| `TypeMappingErrorHandler` | Maps exception types to specific handlers       |

#### ErrorHandlingService (`services/error.py:475-486`)

Wraps any callable with error handling:

```python
error_service = ErrorHandlingService(
    callable=subscriber.consume_all,
    error_handler=ContinueErrorHandler(),
)
```

The `execute()` method implements retry loop with decision pattern matching:

- `RetryErrorHandlerDecision` → `continue` (retry)
- `ContinueErrorHandlerDecision` → return value
- `RaiseErrorHandlerDecision` → re-raise
- `ExitErrorHandlerDecision` → raise `SystemExit`

#### PollingService (`services/polling.py:9-24`)

Continuously executes a callable at intervals:

```python
polling_service = PollingService(
    callable=error_handling_service.execute,
    poll_interval=timedelta(seconds=1),
)
```

#### Composition Pattern (from examples)

```python
# 1. Create subscriber
subscriber = make_subscriber(...)

# 2. Wrap with error handling
error_service = ErrorHandlingService(
    callable=subscriber.consume_all,
    error_handler=ContinueErrorHandler(),
)

# 3. Wrap with polling
polling_service = PollingService(
    callable=error_service.execute,
    poll_interval=timedelta(seconds=1),
)

# 4. Register with broker and service manager
await event_broker.register(subscriber)
service_manager.register(
    polling_service,
    execution_mode=ExecutionMode.BACKGROUND,
)
```

---

### Wiring Patterns from Examples

The `meta/examples/subscriber_registration/` directory shows several patterns:

#### Pattern 1: All-in-One Function (`all_in_one_function.py`)

Single function that creates subscriber, wraps with error handling and polling,
registers with broker and service manager.

#### Pattern 2: Consumer Registrar (`consumer_registrar.py`)

Abstract base class with:

- Class-level configuration (`subscriber_group`, `category`)
- Instance-level dependencies (`event_store`, `event_processor`)
- `register_as_service()` method encapsulating wiring

#### Pattern 3: Register Many Function (`register_many_function.py`)

Batch registration with `init_event_services()` function that:

- Creates broker and service manager
- Collects all subscription consumers from domain modules
- Registers each with error handling and polling

#### Pattern 4: Subsystem Creator (`subsytem_creator.py`)

`ProviderService` class that creates multiple related services:

- Projection services
- Ingestion services
- Optional enable/disable per service

---

### Testing Strategy

#### Three-Tier Approach

**Unit Tests** (`tests/unit/`):

- Mock all dependencies with `Mock(spec=Interface)`
- Test single component behaviour
- Fast execution, no external dependencies
- Example: `test_broker.py` uses mocked coordinator/observer/manager

**Integration Tests** (`tests/integration/`):

- Use real PostgreSQL via `AsyncConnectionPool`
- Test interactions between components
- Database fixtures reset schema between tests
- Example: `test_broker.py` tests 50+ subscribers across multiple nodes

**Component Tests** (`tests/component/`):

- Combine multiple real components
- Validate end-to-end workflows
- Use `ServiceManager` to orchestrate lifecycle
- Example: `test_asynchronous_projections.py` tests full projection pipeline

#### Test Patterns

**Capturing Test Doubles**:

```python
class CapturingEventProcessor(EventProcessor):
    def __init__(self):
        self.events = []

    async def process_event(self, event):
        self.events.append(event)
```

**NodeSet Pattern** (integration tests):
Manages multiple broker nodes for distributed testing with dynamic
start/stop/replacement.

**Builder Pattern**:

```python
NewEventBuilder().with_category("orders").with_payload({...}).build()
```

**Shared Test Cases**:
Abstract base test classes extended by adapter-specific tests (e.g.,
`LockManagerCases`).

---

## Architecture Insights

### Key Design Patterns

1. **Strategy Pattern**: Two broker implementations (Singleton vs Distributed)
   behind common interface

2. **Factory Pattern**: `make_subscriber()`, `make_event_broker()` construct
   complex object graphs

3. **Builder Pattern**: `DistributedEventBrokerBuilder` for fluent construction

4. **Observer Pattern**: Distributed observer watches subscription state and
   propagates to local subscribers

5. **Decorator/Wrapper Pattern**: `ErrorHandlingService` wraps callable,
   `PollingService` wraps service

6. **Leader Election**: Coordinator uses distributed advisory lock for mutual
   exclusion

7. **Heartbeat Pattern**: Subscriber manager sends periodic heartbeats for
   liveness tracking

### node_id Concept

A string identifier representing a physical instance/host in the distributed
system. Used by:

- Coordinator: For logging which node is coordinating
- Observer: For logging which node is observing
- Subscriber Manager: For tracking which node owns subscribers
- State stores: For associating subscriptions with nodes

### Process Abstraction

All long-running components implement `Process` with:

- `status` property returning `ProcessStatus` (INITIALISED, STARTING, RUNNING,
  STOPPED, ERRORED)
- Lifecycle tracking through status transitions

---

## Implications for EventProcessingNode

Based on this research, an `EventProcessingNode` abstraction could:

### Encapsulation

- Wrap `EventBroker`, `ServiceManager`, and related infrastructure
- Own the `node_id` concept
- Manage lifecycle of all node-scoped components

### Node Modes

| Mode                 | Coordinator | Observer | Subscribers |
|----------------------|-------------|----------|-------------|
| **Coordinator-only** | Yes         | No       | No          |
| **Observer-only**    | No          | Yes      | Yes         |
| **Both**             | Yes         | Yes      | Yes         |

For coordinator-only nodes:

- Run only `EventSubscriberManager` (for heartbeats) and
  `EventSubscriptionCoordinator`
- No local subscribers registered
- Could use `SingletonEventBroker` if no distribution needed

For observer-only nodes:

- Run `EventSubscriberManager` and `EventSubscriptionObserver`
- No `EventSubscriptionCoordinator`
- Register and process subscribers locally

For both:

- Current `DistributedEventBroker` behaviour
- Full participation in coordination and observation

### Registration Simplification

Current pattern:

```python
subscriber = make_subscriber(...)
error_service = ErrorHandlingService(callable=subscriber.consume_all, ...)
polling_service = PollingService(callable=error_service.execute, ...)
await event_broker.register(subscriber)
service_manager.register(polling_service, ...)
```

Simplified with `EventProcessingNode`:

```python
node = EventProcessingNode(mode=NodeMode.BOTH, ...)
node.register_subscriber(
    group="projections",
    category="orders",
    processor=my_processor,
    error_handler=ContinueErrorHandler(),
    poll_interval=timedelta(seconds=1),
)
await node.start()
```

### Configuration

Could consolidate:

- Broker settings (distribution interval, leadership duration, etc.)
- Service manager settings (stop signals, isolation modes)
- Subscriber defaults (persistence interval, poll interval, error handling)

---

## Code References

### Core Components

- `src/logicblocks/event/processing/broker/base.py:11-14` - EventBroker
  interface
-
`src/logicblocks/event/processing/broker/strategies/distributed/broker.py:19-57` -
DistributedEventBroker
-
`src/logicblocks/event/processing/broker/strategies/singleton/broker.py:26-84` -
SingletonEventBroker
- `src/logicblocks/event/processing/services/manager.py:174-224` -
  ServiceManager
- `src/logicblocks/event/processing/consumers/subscription.py:54-192` -
  EventSubscriptionConsumer & make_subscriber
- `src/logicblocks/event/processing/consumers/source.py:139-239` -
  EventSourceConsumer

### Coordinator/Observer

-
`src/logicblocks/event/processing/broker/strategies/distributed/coordinator.py:112-364` -
EventSubscriptionCoordinator
-
`src/logicblocks/event/processing/broker/strategies/distributed/observer.py:28-113` -
EventSubscriptionObserver
-
`src/logicblocks/event/processing/broker/strategies/distributed/subscribers/manager.py:39-148` -
EventSubscriberManager

### Error Handling & Polling

- `src/logicblocks/event/processing/services/error.py:52-486` - Error handlers
  and ErrorHandlingService
- `src/logicblocks/event/processing/services/polling.py:9-24` - PollingService

### State Management

- `src/logicblocks/event/processing/consumers/state/base.py:76-198` -
  EventConsumerStateStore
-
`src/logicblocks/event/processing/broker/strategies/distributed/subscribers/stores/state/base.py:14-60` -
EventSubscriberStateStore
-
`src/logicblocks/event/processing/broker/strategies/distributed/subscriptions/stores/state/base.py:17-72` -
EventSubscriptionStateStore

### Tests

- `tests/unit/logicblocks/event/processing/services/test_manager.py:1-730` -
  ServiceManager tests
-
`tests/unit/logicblocks/event/processing/brokers/strategies/distributed/test_broker.py` -
Broker unit tests
-
`tests/integration/logicblocks/event/processing/broker/strategies/distributed/test_broker.py` -
Broker integration tests
- `tests/component/test_processing.py` - End-to-end processing tests

---

## Open Questions

1. **Node Identity**: Should `EventProcessingNode` generate its own `node_id` or
   accept one? How does this relate to deployment topology?

2. **Dynamic Mode Switching**: Can a node switch between coordinator-only and
   both modes at runtime, or is this fixed at construction?

3. **Subscriber Hot-Swap**: Should the node support adding/removing subscribers
   while running?

4. **Health Reporting**: Should the node expose aggregate health status from all
   components?

5. **Graceful Shutdown**: How should shutdown coordinate between service manager
   stop and broker unregistration?

6. **Configuration Hierarchy**: Should node-level defaults be overridable
   per-subscriber?
