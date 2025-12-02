# EventRuntime Abstraction Implementation Plan

## Overview

Introduce an `EventRuntime` abstraction that sits over the event broker and
service manager, simplifying subscriber registration and providing a clean
lifecycle API. Additionally, add `BrokerRole` support to
`DistributedEventBroker`
to enable coordinator-only, observer-only, or full participation modes.

## Current State Analysis

### Problem

Registering subscribers currently requires 5+ steps across multiple
abstractions:

```python
# 1. Create subscriber
subscriber = make_subscriber(
    subscriber_group="projections",
    subscription_request=CategoryIdentifier(category="orders"),
    subscriber_state_category=event_store.category(...),
    event_processor=processor,
    ...
)

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

# 4. Register with broker
await event_broker.register(subscriber)

# 5. Register with service manager
service_manager.register(polling_service,
                         execution_mode=ExecutionMode.BACKGROUND)
```

Each consumer in the codebase solves this differently (see examples in
`meta/examples/subscriber_registration/`), leading to inconsistency and
boilerplate.

Additionally, the `DistributedEventBroker` currently always runs all three
components (coordinator, observer, subscriber manager) together. There's no way
to run a coordinator-only node (for work allocation without local processing)
or an observer-only node (for processing without coordination).

### Key Discoveries

- `DistributedEventBroker` tightly couples coordinator, observer, and subscriber
  manager in `_do_execute()` at `broker/strategies/distributed/broker.py:44-57`
- `EventSubscriber` interface is for work allocation (accept/withdraw sources)
- `EventConsumer` interface is for event consumption (consume_all)
- `EventSubscriptionConsumer` implements both interfaces
- Error handling and polling wrapping applies to consumption, not subscription
- `node_id` is already injected into broker at construction time

## Desired End State

### BrokerRole Support

`DistributedEventBroker` supports three roles:

```python
class BrokerRole(StrEnum):
    COORDINATOR = "coordinator"  # Work allocation only, no local subscribers
    OBSERVER = "observer"  # Local processing only, no coordination
    FULL = "full"  # Both coordination and observation
```

### EventRuntime Abstraction

A simplified API for event processing setup:

```python
@dataclass
class EventConsumerSettings:
    error_handler: ErrorHandler = field(
        default_factory=lambda: ContinueErrorHandler()
    )
    poll_interval: timedelta = timedelta(seconds=1)
    isolation_mode: IsolationMode = IsolationMode.MAIN_THREAD


runtime = EventRuntime(
    node_id=node_id,
    event_broker=event_broker,
    service_manager=service_manager,
    default_consumer_settings=EventConsumerSettings(...),  # Optional defaults
)

# Register subscription consumer (common case)
await runtime.register_subscription_consumer(
    subscription_consumer,
    settings=EventConsumerSettings(poll_interval=timedelta(milliseconds=100)),
)

# Or use defaults
await runtime.register_subscription_consumer(subscription_consumer)

# Start processing
await runtime.start()

# ... running ...

await runtime.stop()
```

### Verification

- Unit tests verify each `BrokerRole` runs correct components
- Unit tests verify `EventRuntime` registration and lifecycle
- Integration tests verify coordinator-only and observer-only nodes work
  together
- Component tests verify end-to-end event processing through `EventRuntime`

## What We're NOT Doing

- Dynamic registration while running (future enhancement)
- Factory functions or builder pattern for `EventRuntime` (can add later)
- Changes to `SingletonEventBroker` (no coordinator/observer concept)
- Renaming `node_id` (discussed but deferred)
- Adding mode property to `EventBroker` base interface

## Implementation Approach

Split into two pull requests:

1. **PR 1**: Add `BrokerRole` support to `DistributedEventBroker`
2. **PR 2**: Introduce `EventRuntime` with lifecycle and registration API

---

## Phase 1: BrokerRole Support for DistributedEventBroker

### Overview

Introduce `BrokerRole` enum and refactor `DistributedEventBroker` to
conditionally run coordinator and/or observer based on the configured role.

### Changes Required

#### 1. Add BrokerRole Enum

**File**: `src/logicblocks/event/processing/broker/types.py` (new file or add to
existing)

```python
from enum import StrEnum


class BrokerRole(StrEnum):
    COORDINATOR = "coordinator"
    OBSERVER = "observer"
    FULL = "full"
```

#### 2. Update DistributedEventBroker

**File**:
`src/logicblocks/event/processing/broker/strategies/distributed/broker.py`

**Changes**:

- Add `role: BrokerRole` parameter to constructor
- Store role as instance variable
- Modify `_do_execute()` to conditionally create tasks based on role
- Modify `register()` to raise if role is `COORDINATOR`
- Update `status` property to handle missing coordinator/observer

```python
class DistributedEventBroker[E: Event](
    EventBroker[E], ErrorHandlingServiceMixin[NoneType]
):
    def __init__(
        self,
        event_subscriber_manager: EventSubscriberManager[E],
        event_subscription_coordinator: EventSubscriptionCoordinator,
        event_subscription_observer: EventSubscriptionObserver[E],
        role: BrokerRole = BrokerRole.FULL,
        error_handler: ErrorHandler[NoneType] = RetryErrorHandler(),
    ):
        super().__init__(error_handler)
        self._event_subscriber_manager = event_subscriber_manager
        self._event_subscription_coordinator = event_subscription_coordinator
        self._event_subscription_observer = event_subscription_observer
        self._role = role

    @property
    def role(self) -> BrokerRole:
        return self._role

    async def register(self, subscriber: EventSubscriber[E]) -> None:
        if self._role == BrokerRole.COORDINATOR:
            raise RuntimeError(
                "Cannot register subscribers on a coordinator-only broker"
            )
        await self._event_subscriber_manager.add(subscriber)

    @property
    def status(self) -> ProcessStatus:
        match self._role:
            case BrokerRole.COORDINATOR:
                return self._event_subscription_coordinator.status
            case BrokerRole.OBSERVER:
                return self._event_subscription_observer.status
            case BrokerRole.FULL:
                return determine_multi_process_status(
                    self._event_subscription_coordinator.status,
                    self._event_subscription_observer.status,
                )

    async def _do_execute(self) -> None:
        subscriber_manager = self._event_subscriber_manager
        coordinator = self._event_subscription_coordinator
        observer = self._event_subscription_observer

        try:
            await subscriber_manager.start()

            async with asyncio.TaskGroup() as tg:
                tg.create_task(subscriber_manager.maintain())

                if self._role in (BrokerRole.COORDINATOR, BrokerRole.FULL):
                    tg.create_task(coordinator.coordinate())

                if self._role in (BrokerRole.OBSERVER, BrokerRole.FULL):
                    tg.create_task(observer.observe())
        finally:
            await subscriber_manager.stop()
```

#### 3. Update Builder

**File**:
`src/logicblocks/event/processing/broker/strategies/distributed/builder.py`

**Changes**:

- Add `role: BrokerRole` to `DistributedEventBrokerSettings`
- Pass role through to broker construction in `build()`

```python
@dataclass(frozen=True)
class DistributedEventBrokerSettings:
    role: BrokerRole = BrokerRole.FULL
    subscriber_manager_heartbeat_interval: timedelta = timedelta(seconds=10)
    # ... rest unchanged
```

#### 4. Update Factories

**File**: `src/logicblocks/event/processing/broker/factories.py`

**Changes**:

- Ensure `role` flows through factory functions via settings

#### 5. Export New Types

**File**: `src/logicblocks/event/processing/__init__.py`

**Changes**:

- Export `BrokerRole` from public API

### Testing Strategy

#### Unit Tests

**File**:
`tests/unit/logicblocks/event/processing/brokers/strategies/distributed/test_broker.py`

**New Test Cases**:

```python
class TestDistributedEventBrokerRoles:
    async def test_full_role_runs_coordinator_and_observer(self):
        # Verify both coordinator.coordinate() and observer.observe() are called
        pass

    async def test_coordinator_role_runs_only_coordinator(self):
        # Verify coordinator.coordinate() called, observer.observe() not called
        pass

    async def test_observer_role_runs_only_observer(self):
        # Verify observer.observe() called, coordinator.coordinate() not called
        pass

    async def test_coordinator_role_rejects_subscriber_registration(self):
        # Verify register() raises RuntimeError
        pass

    async def test_observer_role_accepts_subscriber_registration(self):
        # Verify register() succeeds
        pass

    async def test_full_role_accepts_subscriber_registration(self):
        # Verify register() succeeds
        pass

    async def test_status_reflects_coordinator_only_when_coordinator_role(self):
        # Verify status property returns coordinator status only
        pass

    async def test_status_reflects_observer_only_when_observer_role(self):
        # Verify status property returns observer status only
        pass
```

**Pattern**: Use existing `MockEventSubscriptionCoordinator` and
`MockEventSubscriptionObserver` to track whether `coordinate()` and `observe()`
are called.

#### Integration Tests

**File**:
`tests/integration/logicblocks/event/processing/broker/strategies/distributed/test_broker.py`

**New Test Cases**:

```python
class TestDistributedEventBrokerRoleIntegration:
    async def test_coordinator_and_observer_nodes_work_together(self):
        # Start coordinator-only node
        # Start observer-only node with subscribers
        # Publish events
        # Verify observer node processes events
        # Verify coordinator allocated work to observer
        pass

    async def test_multiple_observer_nodes_with_single_coordinator(self):
        # Start one coordinator-only node
        # Start multiple observer-only nodes
        # Verify work distributed across observers
        pass
```

**Pattern**: Extend existing `NodeSet` pattern to support mixed-role node sets.

### Success Criteria

#### Automated Verification

- [ ] All existing broker tests pass: `mise run test:unit`
- [ ] New role-specific unit tests pass
- [ ] Integration tests pass: `mise run test:integration`
- [ ] Type checking passes: `mise run type:check`
- [ ] Linting passes: `mise run lint:fix`
- [ ] Formatting passes: `mise run format:fix`

#### Manual Verification

- [ ] Coordinator-only broker starts without error
- [ ] Observer-only broker starts without error
- [ ] Coordinator-only broker raises on `register()` call

---

## Phase 2: EventRuntime Abstraction

### Overview

Introduce `EventRuntime` class that encapsulates subscriber/consumer
registration, error handling wrapping, polling service creation, and lifecycle
management.

### Changes Required

#### 1. Add EventConsumerSettings

**File**: `src/logicblocks/event/processing/runtime/settings.py` (new file)

```python
from dataclasses import dataclass, field
from datetime import timedelta

from logicblocks.event.processing.services import IsolationMode
from logicblocks.event.processing.services.error import (
    ContinueErrorHandler,
    ErrorHandler,
)


@dataclass(frozen=True)
class EventConsumerSettings:
    error_handler: ErrorHandler = field(
        default_factory=lambda: ContinueErrorHandler()
    )
    poll_interval: timedelta = timedelta(seconds=1)
    isolation_mode: IsolationMode = IsolationMode.MAIN_THREAD
```

#### 2. Add EventRuntime Class

**File**: `src/logicblocks/event/processing/runtime/runtime.py` (new file)

```python
from logicblocks.event.processing.broker import EventBroker
from logicblocks.event.processing.broker.types import EventSubscriber
from logicblocks.event.processing.consumers import (
    EventConsumer,
    EventSubscriptionConsumer,
)
from logicblocks.event.processing.services import (
    ErrorHandlingService,
    ExecutionMode,
    PollingService,
    ServiceManager,
)

from .settings import EventConsumerSettings


class EventRuntime:
    def __init__(
        self,
        node_id: str,
        event_broker: EventBroker,
        service_manager: ServiceManager,
        default_consumer_settings: EventConsumerSettings | None = None,
    ):
        self._node_id = node_id
        self._event_broker = event_broker
        self._service_manager = service_manager
        self._default_consumer_settings = (
            default_consumer_settings or EventConsumerSettings()
        )
        self._started = False

    @property
    def node_id(self) -> str:
        return self._node_id

    def _resolve_settings(
        self, settings: EventConsumerSettings | None
    ) -> EventConsumerSettings:
        return settings or self._default_consumer_settings

    def _check_not_started(self) -> None:
        if self._started:
            raise RuntimeError(
                "Cannot register after runtime has started"
            )

    async def register_subscriber(
        self,
        subscriber: EventSubscriber,
    ) -> None:
        """Register a subscriber for work allocation only.

        The subscriber will receive event source allocations from the broker
        but no consumption wrapping (error handling, polling) is applied.
        """
        self._check_not_started()
        await self._event_broker.register(subscriber)

    async def register_consumer(
        self,
        consumer: EventConsumer,
        settings: EventConsumerSettings | None = None,
    ) -> None:
        """Register a consumer for continuous, error-resilient consumption.

        Wraps the consumer with error handling and polling services.
        Does not register with the broker for work allocation.
        """
        self._check_not_started()
        resolved_settings = self._resolve_settings(settings)

        error_handling_service = ErrorHandlingService(
            callable=consumer.consume_all,
            error_handler=resolved_settings.error_handler,
        )

        polling_service = PollingService(
            callable=error_handling_service.execute,
            poll_interval=resolved_settings.poll_interval,
        )

        self._service_manager.register(
            polling_service,
            execution_mode=ExecutionMode.BACKGROUND,
            isolation_mode=resolved_settings.isolation_mode,
        )

    async def register_subscription_consumer(
        self,
        subscription_consumer: EventSubscriptionConsumer,
        settings: EventConsumerSettings | None = None,
    ) -> None:
        """Register a subscription consumer for both allocation and consumption.

        Registers with the broker for work allocation and wraps with error
        handling and polling for continuous consumption.
        """
        self._check_not_started()
        await self._event_broker.register(subscription_consumer)
        await self.register_consumer(subscription_consumer, settings)

    async def start(self) -> None:
        """Start the runtime.

        Registers the broker with the service manager and starts all services.
        No further registrations are allowed after calling start().
        """
        self._check_not_started()
        self._service_manager.register(self._event_broker)
        self._started = True
        await self._service_manager.start()

    async def stop(self) -> None:
        """Stop the runtime.

        Stops all services including the broker.
        """
        await self._service_manager.stop()
```

#### 3. Add Module Structure

**File**: `src/logicblocks/event/processing/runtime/__init__.py` (new file)

```python
from .runtime import EventRuntime
from .settings import EventConsumerSettings

__all__ = [
    "EventRuntime",
    "EventConsumerSettings",
]
```

#### 4. Export from Processing Package

**File**: `src/logicblocks/event/processing/__init__.py`

**Changes**:

- Export `EventRuntime` and `EventConsumerSettings`

### Testing Strategy

#### Unit Tests

**File**: `tests/unit/logicblocks/event/processing/runtime/test_runtime.py` (
new)

```python
class TestEventRuntimeRegistration:
    async def test_register_subscriber_delegates_to_broker(self):
        # Mock broker, verify register() called
        pass

    async def test_register_consumer_creates_error_handling_service(self):
        # Verify consumer wrapped with ErrorHandlingService
        pass

    async def test_register_consumer_creates_polling_service(self):
        # Verify error handling service wrapped with PollingService
        pass

    async def test_register_consumer_registers_with_service_manager(self):
        # Verify polling service registered as BACKGROUND
        pass

    async def test_register_consumer_uses_provided_settings(self):
        # Verify custom error_handler, poll_interval, isolation_mode used
        pass

    async def test_register_consumer_uses_default_settings_when_none_provided(
        self):
        # Verify runtime defaults used
        pass

    async def test_register_subscription_consumer_registers_with_broker(self):
        # Verify broker.register() called
        pass

    async def test_register_subscription_consumer_wraps_for_consumption(self):
        # Verify error handling + polling applied
        pass

    async def test_registration_raises_after_start(self):
        # Verify RuntimeError raised if register called after start()
        pass


class TestEventRuntimeLifecycle:
    async def test_start_registers_broker_with_service_manager(self):
        # Verify broker registered before start
        pass

    async def test_start_starts_service_manager(self):
        # Verify service_manager.start() called
        pass

    async def test_stop_stops_service_manager(self):
        # Verify service_manager.stop() called
        pass


class TestEventRuntimeWithCoordinatorBroker:
    async def test_register_subscriber_propagates_broker_error(self):
        # Broker configured as COORDINATOR raises on register()
        # Verify EventRuntime propagates the error
        pass
```

**File**: `tests/unit/logicblocks/event/processing/runtime/test_settings.py` (
new)

```python
class TestEventConsumerSettings:
    def test_default_error_handler_is_continue(self):
        pass

    def test_default_poll_interval_is_one_second(self):
        pass

    def test_default_isolation_mode_is_main_thread(self):
        pass

    def test_settings_are_immutable(self):
        # Verify frozen=True
        pass
```

#### Integration Tests

**File**:
`tests/integration/logicblocks/event/processing/runtime/test_runtime.py` (new)

```python
class TestEventRuntimeIntegration:
    async def test_processes_events_through_registered_subscription_consumer(
        self):
        # Create real broker, service manager, runtime
        # Register subscription consumer
        # Publish events
        # Verify events processed
        pass

    async def test_multiple_subscription_consumers_process_concurrently(self):
        # Register multiple subscription consumers
        # Verify all receive events
        pass

    async def test_error_handling_continues_on_processor_exception(self):
        # Configure ContinueErrorHandler
        # Processor throws exception
        # Verify processing continues
        pass
```

#### Component Tests

**File**: `tests/component/test_event_runtime.py` (new)

```python
class TestEventRuntimeComponent:
    async def test_end_to_end_event_processing(self):
        # Full setup with PostgreSQL
        # Create runtime with subscription consumers
        # Publish events
        # Verify processing and state persistence
        pass
```

### Success Criteria

#### Automated Verification

- [ ] All existing tests pass: `mise run test:unit`
- [ ] New runtime unit tests pass
- [ ] Integration tests pass: `mise run test:integration`
- [ ] Component tests pass: `mise run test:component`
- [ ] Type checking passes: `mise run type:check`
- [ ] Linting passes: `mise run lint:fix`
- [ ] Formatting passes: `mise run format:fix`

#### Manual Verification

- [ ] EventRuntime can be constructed with all required dependencies
- [ ] Registration methods work with real subscribers/consumers
- [ ] Start/stop lifecycle works correctly
- [ ] Error handling wrapper catches and handles exceptions
- [ ] Polling continuously calls consume_all at configured interval

---

## Future Enhancements

The following are explicitly out of scope for this implementation but are
documented for future consideration:

1. **Dynamic Registration**: Allow `register_*` methods to be called after
   `start()`. Requires changes to broker and service manager.

2. **Factory Functions**: `make_event_runtime()` factory with overloads for
   type-safe construction similar to `make_event_broker()`.

3. **Builder Pattern**: `EventRuntimeBuilder` for fluent construction if
   configuration becomes more complex.

4. **Health/Status API**: Expose aggregate health status from broker and
   registered consumers.

5. **Graceful Shutdown**: Coordinate shutdown to drain in-flight events before
   stopping.

---

## References

- Research document:
  `meta/research/2025-12-02-event-processing-node-abstraction.md`
- Example patterns: `meta/examples/subscriber_registration/`
- EventBroker interface: `src/logicblocks/event/processing/broker/base.py:11-14`
- DistributedEventBroker:
  `src/logicblocks/event/processing/broker/strategies/distributed/broker.py:19-57`
- ServiceManager: `src/logicblocks/event/processing/services/manager.py:174-224`
- EventSubscriptionConsumer:
  `src/logicblocks/event/processing/consumers/subscription.py:95`
