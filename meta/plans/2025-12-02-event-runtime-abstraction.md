# EventRuntime Abstraction

## Overview

Introduce an `EventRuntime` abstraction that sits over the event broker and
service manager, simplifying subscriber and consumer registration while
providing a clean lifecycle API.

## Current State Analysis

### Problem

Registering subscribers currently requires 5+ steps across multiple abstractions:

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
service_manager.register(polling_service, execution_mode=ExecutionMode.BACKGROUND)
```

Each consumer in the codebase solves this differently (see examples in
`meta/examples/subscriber_registration/`), leading to inconsistency and
boilerplate.

### Key Discoveries

- `EventSubscriber` interface is for work allocation (accept/withdraw sources)
- `EventConsumer` interface is for event consumption (consume_all)
- `EventSubscriptionConsumer` implements both interfaces
- Error handling and polling wrapping applies to consumption, not subscription
- `node_id` is injected into broker at construction time

## Desired End State

A simplified API for event processing setup:

```python
@dataclass
class EventConsumerSettings:
    error_handler: ErrorHandler = field(
        default_factory=lambda: ContinueErrorHandler()
    )
    poll_interval: timedelta = timedelta(seconds=1)
    isolation_mode: IsolationMode = IsolationMode.MAIN_THREAD


# Create runtime with injected dependencies
runtime = EventRuntime(
    node_id=node_id,
    event_broker=event_broker,
    service_manager=service_manager,
    default_consumer_settings=EventConsumerSettings(...),  # Optional defaults
)

# Register subscription consumer (common case) - handles both broker
# registration and consumption wrapping
await runtime.register_subscription_consumer(
    subscription_consumer,
    settings=EventConsumerSettings(poll_interval=timedelta(milliseconds=100)),
)

# Or register subscriber only (work allocation, no consumption wrapping)
await runtime.register_subscriber(subscriber)

# Or register consumer only (consumption wrapping, no broker registration)
await runtime.register_consumer(consumer, settings=EventConsumerSettings(...))

# Start processing
await runtime.start()

# ... running ...

await runtime.stop()
```

### Verification

- Unit tests verify registration methods delegate correctly
- Unit tests verify error handling and polling wrapping
- Unit tests verify lifecycle methods
- Integration tests verify end-to-end event processing
- Component tests verify full PostgreSQL-backed processing

## What We're NOT Doing

- Dynamic registration while running (future enhancement)
- Factory functions or builder pattern for `EventRuntime` (can add later)
- Health/status aggregation API (future enhancement)

## Implementation Approach

Follow TDD: write failing tests first, then implement minimum code to pass,
then refactor.

---

## Changes Required

### 1. Add EventConsumerSettings

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
    """Settings for consumer error handling and polling behaviour.

    Attributes:
        error_handler: Handler for exceptions during consumption.
            Defaults to ContinueErrorHandler which logs and continues.
        poll_interval: Interval between consume_all() calls.
            Defaults to 1 second.
        isolation_mode: Thread isolation for the polling service.
            Defaults to MAIN_THREAD.
    """

    error_handler: ErrorHandler = field(
        default_factory=lambda: ContinueErrorHandler()
    )
    poll_interval: timedelta = timedelta(seconds=1)
    isolation_mode: IsolationMode = IsolationMode.MAIN_THREAD
```

### 2. Add EventRuntime Class

**File**: `src/logicblocks/event/processing/runtime/runtime.py` (new file)

```python
from typing import Any

from logicblocks.event.processing.broker import EventBroker
from logicblocks.event.processing.broker.types import EventSubscriber
from logicblocks.event.processing.consumers import EventConsumer
from logicblocks.event.processing.consumers.subscription import (
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
    """Runtime for event processing that simplifies registration and lifecycle.

    Encapsulates the event broker and service manager, providing a simplified
    API for registering subscribers and consumers with appropriate error
    handling and polling.

    Attributes:
        node_id: Identifier for this deployed instance.
    """

    def __init__(
        self,
        node_id: str,
        event_broker: EventBroker[Any],
        service_manager: ServiceManager,
        default_consumer_settings: EventConsumerSettings | None = None,
    ):
        """Initialise the runtime.

        Args:
            node_id: Identifier for this deployed instance.
            event_broker: The event broker for subscriber registration.
            service_manager: The service manager for service lifecycle.
            default_consumer_settings: Default settings for consumers.
                Used when no settings provided to registration methods.
        """
        self._node_id = node_id
        self._event_broker = event_broker
        self._service_manager = service_manager
        self._default_consumer_settings = (
            default_consumer_settings or EventConsumerSettings()
        )
        self._started = False

    @property
    def node_id(self) -> str:
        """The identifier for this deployed instance."""
        return self._node_id

    def _resolve_settings(
        self, settings: EventConsumerSettings | None
    ) -> EventConsumerSettings:
        """Resolve settings, using defaults if none provided."""
        return settings or self._default_consumer_settings

    def _check_not_started(self) -> None:
        """Raise if runtime has already started."""
        if self._started:
            raise RuntimeError("Cannot register after runtime has started")

    async def register_subscriber(
        self,
        subscriber: EventSubscriber[Any],
    ) -> None:
        """Register a subscriber for work allocation only.

        The subscriber will receive event source allocations from the broker
        but no consumption wrapping (error handling, polling) is applied.

        Use this when:
        - You have a custom subscriber that manages its own consumption
        - You need broker registration but handle polling yourself

        Args:
            subscriber: The subscriber to register with the broker.

        Raises:
            RuntimeError: If called after start().
            RuntimeError: If broker does not accept subscribers (e.g.,
                coordinator-only broker).
        """
        self._check_not_started()
        await self._event_broker.register(subscriber)

    async def register_consumer(
        self,
        consumer: EventConsumer[Any],
        settings: EventConsumerSettings | None = None,
    ) -> None:
        """Register a consumer for continuous, error-resilient consumption.

        Wraps the consumer with error handling and polling services and
        registers with the service manager. Does not register with the broker
        for work allocation.

        Use this when:
        - You have a consumer that doesn't need broker work allocation
        - You're consuming from a known source directly

        Args:
            consumer: The consumer to wrap and register.
            settings: Consumer settings. Uses runtime defaults if not provided.

        Raises:
            RuntimeError: If called after start().
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
        subscription_consumer: EventSubscriptionConsumer[Any],
        settings: EventConsumerSettings | None = None,
    ) -> None:
        """Register a subscription consumer for allocation and consumption.

        Registers with the broker for work allocation and wraps with error
        handling and polling for continuous consumption. This is the common
        case for event processing.

        Args:
            subscription_consumer: The subscription consumer to register.
            settings: Consumer settings. Uses runtime defaults if not provided.

        Raises:
            RuntimeError: If called after start().
            RuntimeError: If broker does not accept subscribers.
        """
        self._check_not_started()
        await self._event_broker.register(subscription_consumer)
        await self.register_consumer(subscription_consumer, settings)

    async def start(self) -> None:
        """Start the runtime.

        Registers the broker with the service manager and starts all services.
        No further registrations are allowed after calling start().

        Raises:
            RuntimeError: If already started.
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

### 3. Add Module Structure

**File**: `src/logicblocks/event/processing/runtime/__init__.py` (new file)

```python
from .runtime import EventRuntime
from .settings import EventConsumerSettings

__all__ = [
    "EventRuntime",
    "EventConsumerSettings",
]
```

### 4. Export from Processing Package

**File**: `src/logicblocks/event/processing/__init__.py`

**Changes**:

- Add imports for `EventRuntime` and `EventConsumerSettings`
- Add to `__all__` list

---

## Testing Strategy

### Unit Tests

**File**: `tests/unit/logicblocks/event/processing/runtime/test_settings.py` (new)

```python
from datetime import timedelta

import pytest

from logicblocks.event.processing.runtime import EventConsumerSettings
from logicblocks.event.processing.services import IsolationMode
from logicblocks.event.processing.services.error import ContinueErrorHandler


class TestEventConsumerSettings:
    def test_default_error_handler_is_continue_error_handler(self):
        settings = EventConsumerSettings()
        assert isinstance(settings.error_handler, ContinueErrorHandler)

    def test_default_poll_interval_is_one_second(self):
        settings = EventConsumerSettings()
        assert settings.poll_interval == timedelta(seconds=1)

    def test_default_isolation_mode_is_main_thread(self):
        settings = EventConsumerSettings()
        assert settings.isolation_mode == IsolationMode.MAIN_THREAD

    def test_custom_values_are_preserved(self):
        from logicblocks.event.processing.services.error import RaiseErrorHandler

        settings = EventConsumerSettings(
            error_handler=RaiseErrorHandler(),
            poll_interval=timedelta(milliseconds=500),
            isolation_mode=IsolationMode.DEDICATED_THREAD,
        )

        assert isinstance(settings.error_handler, RaiseErrorHandler)
        assert settings.poll_interval == timedelta(milliseconds=500)
        assert settings.isolation_mode == IsolationMode.DEDICATED_THREAD

    def test_settings_are_immutable(self):
        settings = EventConsumerSettings()

        with pytest.raises(AttributeError):
            settings.poll_interval = timedelta(seconds=5)
```

**File**: `tests/unit/logicblocks/event/processing/runtime/test_runtime.py` (new)

```python
from datetime import timedelta
from unittest.mock import AsyncMock, Mock

import pytest

from logicblocks.event.processing.broker import EventBroker
from logicblocks.event.processing.consumers import EventConsumer
from logicblocks.event.processing.consumers.subscription import (
    EventSubscriptionConsumer,
)
from logicblocks.event.processing.runtime import (
    EventConsumerSettings,
    EventRuntime,
)
from logicblocks.event.processing.services import (
    ExecutionMode,
    IsolationMode,
    ServiceManager,
)
from logicblocks.event.processing.services.error import RaiseErrorHandler


class TestEventRuntimeConstruction:
    def test_stores_node_id(self):
        runtime = EventRuntime(
            node_id="test-node",
            event_broker=Mock(spec=EventBroker),
            service_manager=Mock(spec=ServiceManager),
        )

        assert runtime.node_id == "test-node"

    def test_uses_provided_default_settings(self):
        custom_settings = EventConsumerSettings(
            poll_interval=timedelta(milliseconds=100)
        )

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=Mock(spec=EventBroker),
            service_manager=Mock(spec=ServiceManager),
            default_consumer_settings=custom_settings,
        )

        assert runtime._default_consumer_settings == custom_settings

    def test_creates_default_settings_when_none_provided(self):
        runtime = EventRuntime(
            node_id="test-node",
            event_broker=Mock(spec=EventBroker),
            service_manager=Mock(spec=ServiceManager),
        )

        assert runtime._default_consumer_settings is not None
        assert runtime._default_consumer_settings.poll_interval == timedelta(
            seconds=1
        )


class TestEventRuntimeRegisterSubscriber:
    async def test_delegates_to_broker_register(self):
        broker = Mock(spec=EventBroker)
        broker.register = AsyncMock()
        subscriber = Mock(spec=EventSubscriptionConsumer)

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=broker,
            service_manager=Mock(spec=ServiceManager),
        )

        await runtime.register_subscriber(subscriber)

        broker.register.assert_called_once_with(subscriber)

    async def test_raises_after_start(self):
        broker = Mock(spec=EventBroker)
        broker.register = AsyncMock()
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        service_manager.start = AsyncMock()

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=broker,
            service_manager=service_manager,
        )

        await runtime.start()

        with pytest.raises(RuntimeError, match="after runtime has started"):
            await runtime.register_subscriber(Mock(spec=EventSubscriptionConsumer))

    async def test_propagates_broker_registration_error(self):
        broker = Mock(spec=EventBroker)
        broker.register = AsyncMock(
            side_effect=RuntimeError("coordinator-only broker")
        )

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=broker,
            service_manager=Mock(spec=ServiceManager),
        )

        with pytest.raises(RuntimeError, match="coordinator-only"):
            await runtime.register_subscriber(Mock(spec=EventSubscriptionConsumer))


class TestEventRuntimeRegisterConsumer:
    async def test_registers_polling_service_with_service_manager(self):
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        consumer = Mock(spec=EventConsumer)
        consumer.consume_all = AsyncMock()

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=Mock(spec=EventBroker),
            service_manager=service_manager,
        )

        await runtime.register_consumer(consumer)

        service_manager.register.assert_called_once()
        call_args = service_manager.register.call_args
        assert call_args.kwargs["execution_mode"] == ExecutionMode.BACKGROUND

    async def test_uses_provided_settings(self):
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        consumer = Mock(spec=EventConsumer)
        consumer.consume_all = AsyncMock()

        custom_settings = EventConsumerSettings(
            isolation_mode=IsolationMode.DEDICATED_THREAD
        )

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=Mock(spec=EventBroker),
            service_manager=service_manager,
        )

        await runtime.register_consumer(consumer, settings=custom_settings)

        call_args = service_manager.register.call_args
        assert call_args.kwargs["isolation_mode"] == IsolationMode.DEDICATED_THREAD

    async def test_uses_default_settings_when_none_provided(self):
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        consumer = Mock(spec=EventConsumer)
        consumer.consume_all = AsyncMock()

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=Mock(spec=EventBroker),
            service_manager=service_manager,
        )

        await runtime.register_consumer(consumer)

        call_args = service_manager.register.call_args
        assert call_args.kwargs["isolation_mode"] == IsolationMode.MAIN_THREAD

    async def test_raises_after_start(self):
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        service_manager.start = AsyncMock()

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=Mock(spec=EventBroker),
            service_manager=service_manager,
        )

        await runtime.start()

        with pytest.raises(RuntimeError, match="after runtime has started"):
            await runtime.register_consumer(Mock(spec=EventConsumer))


class TestEventRuntimeRegisterSubscriptionConsumer:
    async def test_registers_with_broker(self):
        broker = Mock(spec=EventBroker)
        broker.register = AsyncMock()
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        subscription_consumer = Mock(spec=EventSubscriptionConsumer)
        subscription_consumer.consume_all = AsyncMock()

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=broker,
            service_manager=service_manager,
        )

        await runtime.register_subscription_consumer(subscription_consumer)

        broker.register.assert_called_once_with(subscription_consumer)

    async def test_registers_as_consumer_for_polling(self):
        broker = Mock(spec=EventBroker)
        broker.register = AsyncMock()
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        subscription_consumer = Mock(spec=EventSubscriptionConsumer)
        subscription_consumer.consume_all = AsyncMock()

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=broker,
            service_manager=service_manager,
        )

        await runtime.register_subscription_consumer(subscription_consumer)

        service_manager.register.assert_called_once()

    async def test_uses_provided_settings(self):
        broker = Mock(spec=EventBroker)
        broker.register = AsyncMock()
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        subscription_consumer = Mock(spec=EventSubscriptionConsumer)
        subscription_consumer.consume_all = AsyncMock()

        custom_settings = EventConsumerSettings(
            isolation_mode=IsolationMode.SHARED_THREAD
        )

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=broker,
            service_manager=service_manager,
        )

        await runtime.register_subscription_consumer(
            subscription_consumer, settings=custom_settings
        )

        call_args = service_manager.register.call_args
        assert call_args.kwargs["isolation_mode"] == IsolationMode.SHARED_THREAD


class TestEventRuntimeLifecycle:
    async def test_start_registers_broker_with_service_manager(self):
        broker = Mock(spec=EventBroker)
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        service_manager.start = AsyncMock()

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=broker,
            service_manager=service_manager,
        )

        await runtime.start()

        # Broker should be registered
        service_manager.register.assert_called_with(broker)

    async def test_start_starts_service_manager(self):
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        service_manager.start = AsyncMock()

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=Mock(spec=EventBroker),
            service_manager=service_manager,
        )

        await runtime.start()

        service_manager.start.assert_called_once()

    async def test_start_raises_if_already_started(self):
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        service_manager.start = AsyncMock()

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=Mock(spec=EventBroker),
            service_manager=service_manager,
        )

        await runtime.start()

        with pytest.raises(RuntimeError, match="after runtime has started"):
            await runtime.start()

    async def test_stop_stops_service_manager(self):
        service_manager = Mock(spec=ServiceManager)
        service_manager.register = Mock(return_value=service_manager)
        service_manager.start = AsyncMock()
        service_manager.stop = AsyncMock()

        runtime = EventRuntime(
            node_id="test-node",
            event_broker=Mock(spec=EventBroker),
            service_manager=service_manager,
        )

        await runtime.start()
        await runtime.stop()

        service_manager.stop.assert_called_once()
```

### Integration Tests

**File**: `tests/integration/logicblocks/event/processing/runtime/test_runtime.py` (new)

```python
class TestEventRuntimeIntegration:
    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "events")
        await drop_table(open_connection_pool, "subscribers")
        await drop_table(open_connection_pool, "subscriptions")
        await create_table(open_connection_pool, "events")
        await create_table(open_connection_pool, "subscribers")
        await create_table(open_connection_pool, "subscriptions")

    async def test_processes_events_through_subscription_consumer(
        self, open_connection_pool
    ):
        """Verify events are processed when using register_subscription_consumer."""
        adapter = PostgresEventStorageAdapter(connection_source=open_connection_pool)
        event_store = EventStore(adapter=adapter)
        event_processor = CapturingEventProcessor()

        node_id = random_node_id()
        category = random_category_name()

        event_broker = make_event_broker(
            node_id=node_id,
            broker_type=EventBrokerType.Distributed,
            storage_type=EventBrokerStorageType.Postgres,
            settings=DistributedEventBrokerSettings(
                observer_synchronisation_interval=timedelta(milliseconds=100),
                coordinator_distribution_interval=timedelta(milliseconds=100),
            ),
            connection_settings=connection_settings,
            connection_pool=open_connection_pool,
            adapter=adapter,
        )

        service_manager = ServiceManager()

        runtime = EventRuntime(
            node_id=node_id,
            event_broker=event_broker,
            service_manager=service_manager,
            default_consumer_settings=EventConsumerSettings(
                poll_interval=timedelta(milliseconds=50),
            ),
        )

        subscription_consumer = make_subscriber(
            subscriber_group=f"test-{category}",
            subscription_request=CategoryIdentifier(category=category),
            subscriber_state_category=event_store.category(
                category=f"state-{category}"
            ),
            event_processor=event_processor,
            ...
        )

        await runtime.register_subscription_consumer(subscription_consumer)

        # Publish event before starting
        await event_store.category(category=category).publish(
            NewEvent(name="test-event", payload={"key": "value"})
        )

        # Start runtime
        start_task = asyncio.create_task(runtime.start())

        try:
            # Wait for event to be processed
            await asyncio.wait_for(
                wait_for_event_count(event_processor, 1),
                timeout=10.0,
            )

            assert len(event_processor.events) == 1
        finally:
            await runtime.stop()
            start_task.cancel()
            await asyncio.gather(start_task, return_exceptions=True)

    async def test_error_handling_continues_on_exception(
        self, open_connection_pool
    ):
        """Verify processing continues after exception with ContinueErrorHandler."""
        # Similar setup with a processor that throws once then succeeds
        pass
```

### Component Tests

**File**: `tests/component/test_event_runtime.py` (new)

```python
class TestEventRuntimeComponent:
    async def test_end_to_end_event_processing_with_postgres(self):
        """Full integration test with PostgreSQL storage."""
        # Complete setup with real storage
        # Multiple subscription consumers
        # Verify all events processed
        pass
```

---

## Success Criteria

### Automated Verification

- [ ] All existing tests pass: `mise run test:unit`
- [ ] New runtime unit tests pass
- [ ] New settings unit tests pass
- [ ] Integration tests pass: `mise run test:integration`
- [ ] Component tests pass: `mise run test:component`
- [ ] Type checking passes: `mise run type:check`
- [ ] Linting passes: `mise run lint:fix`
- [ ] Formatting passes: `mise run format:fix`

### Manual Verification

- [ ] EventRuntime can be constructed with all required dependencies
- [ ] Registration methods work with real subscribers/consumers
- [ ] Start/stop lifecycle works correctly
- [ ] Error handling wrapper catches and handles exceptions
- [ ] Polling continuously calls consume_all at configured interval
- [ ] Registration after start raises clear error

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

- Research document: `meta/research/2025-12-02-event-processing-node-abstraction.md`
- Example patterns: `meta/examples/subscriber_registration/`
- EventBroker interface: `src/logicblocks/event/processing/broker/base.py:11-14`
- ServiceManager: `src/logicblocks/event/processing/services/manager.py:174-224`
- EventSubscriptionConsumer: `src/logicblocks/event/processing/consumers/subscription.py:95`
- ErrorHandlingService: `src/logicblocks/event/processing/services/error.py:475-486`
- PollingService: `src/logicblocks/event/processing/services/polling.py:9-24`
