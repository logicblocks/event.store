# BrokerRole Support for DistributedEventBroker

## Overview

Add `BrokerRole` support to `DistributedEventBroker` to enable coordinator-only,
observer-only, or full participation modes. This allows deploying nodes that
only coordinate work allocation (without local processing) or only observe and
process (without coordination).

## Current State Analysis

### Problem

The `DistributedEventBroker` currently always runs all three components
(coordinator, observer, subscriber manager) together in `_do_execute()` at
`broker/strategies/distributed/broker.py:44-57`. There's no way to:

- Run a coordinator-only node for work allocation without local processing
- Run an observer-only node for processing without coordination overhead

### Key Discoveries

- `_do_execute()` unconditionally creates tasks for all three components
- The `asyncio.TaskGroup` pattern requires all tasks to be created together
- Status property aggregates coordinator and observer statuses
- Builder and factories don't support mode configuration

## Desired End State

`DistributedEventBroker` supports three roles:

```python
class BrokerRole(StrEnum):
    COORDINATOR = "coordinator"  # Work allocation only, no local subscribers
    OBSERVER = "observer"        # Local processing only, no coordination
    FULL = "full"                # Both coordination and observation
```

Usage:

```python
# Coordinator-only node
coordinator_broker = make_event_broker(
    node_id=node_id,
    broker_type=EventBrokerType.Distributed,
    storage_type=EventBrokerStorageType.Postgres,
    settings=DistributedEventBrokerSettings(role=BrokerRole.COORDINATOR),
    ...
)

# Observer-only node
observer_broker = make_event_broker(
    node_id=node_id,
    broker_type=EventBrokerType.Distributed,
    storage_type=EventBrokerStorageType.Postgres,
    settings=DistributedEventBrokerSettings(role=BrokerRole.OBSERVER),
    ...
)

# Full participation (default, current behaviour)
full_broker = make_event_broker(
    node_id=node_id,
    broker_type=EventBrokerType.Distributed,
    storage_type=EventBrokerStorageType.Postgres,
    settings=DistributedEventBrokerSettings(role=BrokerRole.FULL),
    ...
)
```

### Verification

- Unit tests verify each `BrokerRole` runs correct components
- Unit tests verify `register()` raises for coordinator-only brokers
- Integration tests verify coordinator-only and observer-only nodes work together

## What We're NOT Doing

- Changes to `SingletonEventBroker` (no coordinator/observer concept)
- Adding mode property to `EventBroker` base interface
- Dynamic role switching at runtime

## Implementation Approach

Follow TDD: write failing tests first, then implement minimum code to pass,
then refactor.

---

## Changes Required

### 1. Add BrokerRole Enum

**File**: `src/logicblocks/event/processing/broker/types.py`

Add to existing file or create new:

```python
from enum import StrEnum


class BrokerRole(StrEnum):
    COORDINATOR = "coordinator"
    OBSERVER = "observer"
    FULL = "full"
```

### 2. Update DistributedEventBroker

**File**: `src/logicblocks/event/processing/broker/strategies/distributed/broker.py`

**Changes**:

- Add `role: BrokerRole` parameter to constructor (default `BrokerRole.FULL`)
- Store role as instance variable
- Add `role` property
- Modify `register()` to raise if role is `COORDINATOR`
- Modify `status` property to handle single-component roles
- Modify `_do_execute()` to conditionally create tasks based on role

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

### 3. Update Builder

**File**: `src/logicblocks/event/processing/broker/strategies/distributed/builder.py`

**Changes**:

- Add `role: BrokerRole` to `DistributedEventBrokerSettings` with default `FULL`
- Pass role through to broker construction in `build()`

```python
@dataclass(frozen=True)
class DistributedEventBrokerSettings:
    role: BrokerRole = BrokerRole.FULL
    subscriber_manager_heartbeat_interval: timedelta = timedelta(seconds=10)
    subscriber_manager_purge_interval: timedelta = timedelta(minutes=1)
    subscriber_manager_subscriber_max_age: timedelta = timedelta(minutes=10)
    coordinator_subscriber_max_time_since_last_seen: timedelta = timedelta(
        seconds=60
    )
    coordinator_distribution_interval: timedelta = timedelta(seconds=20)
    coordinator_leadership_max_duration: timedelta = timedelta(minutes=15)
    coordinator_leadership_attempt_interval: timedelta = timedelta(seconds=5)
    observer_synchronisation_interval: timedelta = timedelta(seconds=20)
```

Update `build()` method to pass `settings.role` to `DistributedEventBroker`.

### 4. Update Factories

**File**: `src/logicblocks/event/processing/broker/strategies/distributed/factories.py`

**Changes**:

- Ensure `role` flows through `make_in_memory_distributed_event_broker()` and
  `make_postgres_distributed_event_broker()` via settings

### 5. Export New Types

**File**: `src/logicblocks/event/processing/__init__.py`

**Changes**:

- Export `BrokerRole` from public API

---

## Testing Strategy

### Unit Tests

**File**: `tests/unit/logicblocks/event/processing/brokers/strategies/distributed/test_broker.py`

**TDD Approach**: Write each test first, verify it fails, then implement.

**New Test Class**:

```python
class TestDistributedEventBrokerRoles:
    async def test_full_role_runs_coordinator_and_observer(self):
        """Verify both coordinator.coordinate() and observer.observe() are called."""
        coordinator = MockEventSubscriptionCoordinator()
        observer = MockEventSubscriptionObserver()
        manager = MockEventSubscriberManager()

        broker = DistributedEventBroker(
            event_subscriber_manager=manager,
            event_subscription_coordinator=coordinator,
            event_subscription_observer=observer,
            role=BrokerRole.FULL,
        )

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_start_count_eventually(coordinator, 1)
            await assert_start_count_eventually(observer, 1)

    async def test_coordinator_role_runs_only_coordinator(self):
        """Verify only coordinator.coordinate() called, not observer.observe()."""
        coordinator = MockEventSubscriptionCoordinator()
        observer = MockEventSubscriptionObserver()
        manager = MockEventSubscriberManager()

        broker = DistributedEventBroker(
            event_subscriber_manager=manager,
            event_subscription_coordinator=coordinator,
            event_subscription_observer=observer,
            role=BrokerRole.COORDINATOR,
        )

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_start_count_eventually(coordinator, 1)
            # Give time for observer to start if it was going to
            await asyncio.sleep(0.1)
            assert observer.start_count == 0

    async def test_observer_role_runs_only_observer(self):
        """Verify only observer.observe() called, not coordinator.coordinate()."""
        coordinator = MockEventSubscriptionCoordinator()
        observer = MockEventSubscriptionObserver()
        manager = MockEventSubscriberManager()

        broker = DistributedEventBroker(
            event_subscriber_manager=manager,
            event_subscription_coordinator=coordinator,
            event_subscription_observer=observer,
            role=BrokerRole.OBSERVER,
        )

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_start_count_eventually(observer, 1)
            # Give time for coordinator to start if it was going to
            await asyncio.sleep(0.1)
            assert coordinator.start_count == 0

    async def test_coordinator_role_rejects_subscriber_registration(self):
        """Verify register() raises RuntimeError for coordinator-only broker."""
        broker = DistributedEventBroker(
            event_subscriber_manager=Mock(spec=DefaultEventSubscriberManager),
            event_subscription_coordinator=Mock(spec=EventSubscriptionCoordinator),
            event_subscription_observer=Mock(spec=EventSubscriptionObserver),
            role=BrokerRole.COORDINATOR,
        )

        subscriber = DummyEventSubscriber()

        with pytest.raises(RuntimeError, match="coordinator-only"):
            await broker.register(subscriber)

    async def test_observer_role_accepts_subscriber_registration(self):
        """Verify register() succeeds for observer-only broker."""
        manager = Mock(spec=DefaultEventSubscriberManager)
        broker = DistributedEventBroker(
            event_subscriber_manager=manager,
            event_subscription_coordinator=Mock(spec=EventSubscriptionCoordinator),
            event_subscription_observer=Mock(spec=EventSubscriptionObserver),
            role=BrokerRole.OBSERVER,
        )

        subscriber = DummyEventSubscriber()
        await broker.register(subscriber)

        manager.add.assert_called_once_with(subscriber)

    async def test_full_role_accepts_subscriber_registration(self):
        """Verify register() succeeds for full broker."""
        manager = Mock(spec=DefaultEventSubscriberManager)
        broker = DistributedEventBroker(
            event_subscriber_manager=manager,
            event_subscription_coordinator=Mock(spec=EventSubscriptionCoordinator),
            event_subscription_observer=Mock(spec=EventSubscriptionObserver),
            role=BrokerRole.FULL,
        )

        subscriber = DummyEventSubscriber()
        await broker.register(subscriber)

        manager.add.assert_called_once_with(subscriber)

    async def test_status_reflects_coordinator_only_when_coordinator_role(self):
        """Verify status property returns only coordinator status."""
        coordinator = Mock(spec=EventSubscriptionCoordinator)
        coordinator.status = ProcessStatus.RUNNING

        broker = DistributedEventBroker(
            event_subscriber_manager=Mock(spec=DefaultEventSubscriberManager),
            event_subscription_coordinator=coordinator,
            event_subscription_observer=Mock(spec=EventSubscriptionObserver),
            role=BrokerRole.COORDINATOR,
        )

        assert broker.status == ProcessStatus.RUNNING

    async def test_status_reflects_observer_only_when_observer_role(self):
        """Verify status property returns only observer status."""
        observer = Mock(spec=EventSubscriptionObserver)
        observer.status = ProcessStatus.RUNNING

        broker = DistributedEventBroker(
            event_subscriber_manager=Mock(spec=DefaultEventSubscriberManager),
            event_subscription_coordinator=Mock(spec=EventSubscriptionCoordinator),
            event_subscription_observer=observer,
            role=BrokerRole.OBSERVER,
        )

        assert broker.status == ProcessStatus.RUNNING

    def test_role_property_returns_configured_role(self):
        """Verify role property returns the configured role."""
        broker = DistributedEventBroker(
            event_subscriber_manager=Mock(spec=DefaultEventSubscriberManager),
            event_subscription_coordinator=Mock(spec=EventSubscriptionCoordinator),
            event_subscription_observer=Mock(spec=EventSubscriptionObserver),
            role=BrokerRole.OBSERVER,
        )

        assert broker.role == BrokerRole.OBSERVER

    def test_default_role_is_full(self):
        """Verify default role is FULL for backwards compatibility."""
        broker = DistributedEventBroker(
            event_subscriber_manager=Mock(spec=DefaultEventSubscriberManager),
            event_subscription_coordinator=Mock(spec=EventSubscriptionCoordinator),
            event_subscription_observer=Mock(spec=EventSubscriptionObserver),
        )

        assert broker.role == BrokerRole.FULL
```

### Integration Tests

**File**: `tests/integration/logicblocks/event/processing/broker/strategies/distributed/test_broker.py`

**New Test Class**:

```python
class TestDistributedEventBrokerRoleIntegration:
    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "events")
        await drop_table(open_connection_pool, "subscribers")
        await drop_table(open_connection_pool, "subscriptions")
        await create_table(open_connection_pool, "events")
        await create_table(open_connection_pool, "subscribers")
        await create_table(open_connection_pool, "subscriptions")

    async def test_coordinator_and_observer_nodes_work_together(
        self, open_connection_pool
    ):
        """Verify coordinator allocates work to observer node."""
        event_processor = CapturingEventProcessor()
        adapter = PostgresEventStorageAdapter(connection_source=open_connection_pool)
        event_store = EventStore(adapter=adapter)

        category = random_category_name()
        node_id_coordinator = random_node_id()
        node_id_observer = random_node_id()

        # Create coordinator-only broker
        coordinator_broker = make_postgres_distributed_event_broker(
            node_id=node_id_coordinator,
            connection_settings=connection_settings,
            connection_pool=open_connection_pool,
            adapter=adapter,
            settings=DistributedEventBrokerSettings(
                role=BrokerRole.COORDINATOR,
                coordinator_distribution_interval=timedelta(milliseconds=200),
            ),
        )

        # Create observer-only broker with subscriber
        observer_broker = make_postgres_distributed_event_broker(
            node_id=node_id_observer,
            connection_settings=connection_settings,
            connection_pool=open_connection_pool,
            adapter=adapter,
            settings=DistributedEventBrokerSettings(
                role=BrokerRole.OBSERVER,
                observer_synchronisation_interval=timedelta(milliseconds=200),
            ),
        )

        subscriber = make_subscriber(
            subscriber_group=f"test-group-{category}",
            subscription_request=CategoryIdentifier(category=category),
            subscriber_state_category=event_store.category(
                category=f"test-state-{category}"
            ),
            event_processor=event_processor,
            ...
        )

        await observer_broker.register(subscriber)

        # Publish event
        await publish_event_to_category(event_store, category)

        # Start both brokers
        coordinator_task = asyncio.create_task(coordinator_broker.execute())
        observer_task = asyncio.create_task(observer_broker.execute())

        try:
            # Wait for event to be processed
            await asyncio.wait_for(
                consume_until_event_count(event_processor, [subscriber], 1),
                timeout=10.0,
            )

            assert len(event_processor.events) == 1
        finally:
            coordinator_task.cancel()
            observer_task.cancel()
            await asyncio.gather(
                coordinator_task, observer_task, return_exceptions=True
            )

    async def test_multiple_observer_nodes_with_single_coordinator(
        self, open_connection_pool
    ):
        """Verify coordinator distributes work across multiple observers."""
        # Similar pattern with multiple observer nodes
        pass
```

---

## Success Criteria

### Automated Verification

- [ ] All existing broker tests pass: `mise run test:unit`
- [ ] New role-specific unit tests pass
- [ ] Integration tests pass: `mise run test:integration`
- [ ] Type checking passes: `mise run type:check`
- [ ] Linting passes: `mise run lint:fix`
- [ ] Formatting passes: `mise run format:fix`

### Manual Verification

- [ ] Coordinator-only broker starts without error
- [ ] Observer-only broker starts without error
- [ ] Full broker behaves same as before (backwards compatible)
- [ ] Coordinator-only broker raises on `register()` call

---

## References

- Research document: `meta/research/2025-12-02-event-processing-node-abstraction.md`
- DistributedEventBroker: `src/logicblocks/event/processing/broker/strategies/distributed/broker.py:19-57`
- Builder: `src/logicblocks/event/processing/broker/strategies/distributed/builder.py`
- Existing broker tests: `tests/unit/logicblocks/event/processing/brokers/strategies/distributed/test_broker.py`
