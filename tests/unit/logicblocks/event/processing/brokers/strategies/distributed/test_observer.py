import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import NotRequired, Protocol, Sequence, TypedDict

from logicblocks.event.testlogging.logger import CapturingLogger, LogLevel
from logicblocks.event.testsupport import CapturingEventSubscriber
from structlog.typing import FilteringBoundLogger

from logicblocks.event.processing import (
    ProcessStatus,
)
from logicblocks.event.processing.broker.strategies.distributed import (
    DefaultEventSubscriptionObserver,
    EventSubscriptionDifference,
    EventSubscriptionKey,
    EventSubscriptionState,
    EventSubscriptionStateStore,
    InMemoryEventSubscriptionStateStore,
)
from logicblocks.event.processing.broker.subscribers import (
    EventSubscriberStore,
    InMemoryEventSubscriberStore,
)
from logicblocks.event.sources import EventSourceFactory
from logicblocks.event.store import EventStoreEventSourceFactory
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.store.adapters.base import EventStorageAdapter
from logicblocks.event.store.store import EventCategory
from logicblocks.event.testing import data
from logicblocks.event.types import (
    CategoryIdentifier,
    Event,
    EventSourceIdentifier,
    StoredEvent,
)


class ThrowingEventSubscriptionStateStore(InMemoryEventSubscriptionStateStore):
    async def list(self) -> Sequence[EventSubscriptionState]:
        raise RuntimeError


class GeneratingEventSubscriptionStateStore(
    InMemoryEventSubscriptionStateStore
):
    def __init__(
        self, node_id: str, subscriber_group: str, subscriber_id: str
    ):
        super().__init__(node_id)
        self._node_id = node_id
        self._subscriber_group = subscriber_group
        self._subscriber_id = subscriber_id
        self.sources: list[EventSourceIdentifier] = []

    async def list(self) -> Sequence[EventSubscriptionState]:
        extra_source = CategoryIdentifier(
            category=data.random_event_category_name()
        )
        existing = await self.get(
            EventSubscriptionKey(
                group=self._subscriber_group, id=self._subscriber_id
            )
        )
        if existing is None:
            await self.add(
                EventSubscriptionState(
                    group=self._subscriber_group,
                    id=self._subscriber_id,
                    node_id=self._node_id,
                    event_sources=[extra_source],
                )
            )
        else:
            await self.replace(
                EventSubscriptionState(
                    group=self._subscriber_group,
                    id=self._subscriber_id,
                    node_id=self._node_id,
                    event_sources=[*existing.event_sources, extra_source],
                )
            )

        self.sources.append(extra_source)

        return await super().list()


@dataclass(frozen=True)
class Context:
    observer: DefaultEventSubscriptionObserver
    node_id: str
    subscriber_store: EventSubscriberStore
    subscription_state_store: EventSubscriptionStateStore
    logger: CapturingLogger
    event_storage_adapter: EventStorageAdapter


class NodeAwareEventSubscriptionStateStoreClass(Protocol):
    def __call__(self, node_id: str) -> EventSubscriptionStateStore: ...


class DefaultEventSubscriptionObserverParams[E: Event](TypedDict):
    node_id: str
    subscriber_store: EventSubscriberStore[E]
    subscription_state_store: EventSubscriptionStateStore
    event_source_factory: EventSourceFactory[E]
    subscription_difference: NotRequired[EventSubscriptionDifference]
    logger: NotRequired[FilteringBoundLogger]
    synchronisation_interval: NotRequired[timedelta]


def make_observer(
    node_id: str | None = None,
    subscription_state_store: EventSubscriptionStateStore | None = None,
    synchronisation_interval: timedelta | None = None,
) -> Context:
    if node_id is None:
        node_id = data.random_node_id()
    subscriber_store = InMemoryEventSubscriberStore()
    if subscription_state_store is None:
        subscription_state_store = InMemoryEventSubscriptionStateStore(
            node_id=node_id
        )
    logger = CapturingLogger.create()
    subscription_difference = EventSubscriptionDifference()
    event_storage_adapter = InMemoryEventStorageAdapter()
    event_source_factory = EventStoreEventSourceFactory(
        adapter=event_storage_adapter
    )

    kwargs: DefaultEventSubscriptionObserverParams[StoredEvent] = {
        "node_id": node_id,
        "subscriber_store": subscriber_store,
        "subscription_state_store": subscription_state_store,
        "subscription_difference": subscription_difference,
        "logger": logger,
        "event_source_factory": event_source_factory,
    }
    if synchronisation_interval is not None:
        kwargs["synchronisation_interval"] = synchronisation_interval

    observer = DefaultEventSubscriptionObserver(**kwargs)

    return Context(
        observer,
        node_id,
        subscriber_store,
        subscription_state_store,
        logger,
        event_storage_adapter,
    )


class TestSynchroniseSynchronisation:
    async def test_applies_new_subscription_to_subscriber(self):
        context = make_observer()
        observer = context.observer
        node_id = context.node_id
        subscriber_store = context.subscriber_store
        subscription_state_store = context.subscription_state_store
        event_storage_adapter = context.event_storage_adapter

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_identifier = CategoryIdentifier(
            category=data.random_event_category_name()
        )

        subscriber = CapturingEventSubscriber(
            group=subscriber_group,
            id=subscriber_id,
        )

        await subscriber_store.add(subscriber)

        subscription_state = EventSubscriptionState(
            group=subscriber_group,
            id=subscriber_id,
            node_id=node_id,
            event_sources=[category_identifier],
        )

        await subscription_state_store.add(subscription_state)

        await observer.synchronise()

        assert subscriber.sources == [
            EventCategory(
                adapter=event_storage_adapter, category=category_identifier
            )
        ]

    async def test_removes_old_subscription_from_subscriber(self):
        context = make_observer()
        observer = context.observer
        node_id = context.node_id
        subscriber_store = context.subscriber_store
        subscription_state_store = context.subscription_state_store

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_identifier = CategoryIdentifier(
            category=data.random_event_category_name()
        )

        subscriber = CapturingEventSubscriber(
            group=subscriber_group,
            id=subscriber_id,
        )

        await subscriber_store.add(subscriber)

        subscription_state = EventSubscriptionState(
            group=subscriber_group,
            id=subscriber_id,
            node_id=node_id,
            event_sources=[category_identifier],
        )

        await subscription_state_store.add(subscription_state)

        await observer.synchronise()

        await subscription_state_store.remove(subscription_state)

        await observer.synchronise()

        assert subscriber.sources == []

    async def test_ignores_new_subscription_if_no_subscriber_in_store(self):
        context = make_observer()
        observer = context.observer
        node_id = context.node_id
        subscription_state_store = context.subscription_state_store

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_identifier = CategoryIdentifier(
            category=data.random_event_category_name()
        )

        subscriber = CapturingEventSubscriber(
            group=subscriber_group,
            id=subscriber_id,
        )

        subscription_state = EventSubscriptionState(
            group=subscriber_group,
            id=subscriber_id,
            node_id=node_id,
            event_sources=[category_identifier],
        )

        await subscription_state_store.add(subscription_state)

        await observer.synchronise()

        assert subscriber.sources == []

    async def test_ignores_old_subscription_if_no_subscriber_in_store(self):
        context = make_observer()
        observer = context.observer
        node_id = context.node_id
        subscriber_store = context.subscriber_store
        subscription_state_store = context.subscription_state_store
        event_storage_adapter = context.event_storage_adapter

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_identifier = CategoryIdentifier(
            category=data.random_event_category_name()
        )

        subscriber = CapturingEventSubscriber(
            group=subscriber_group,
            id=subscriber_id,
        )

        await subscriber_store.add(subscriber)

        subscription_state = EventSubscriptionState(
            group=subscriber_group,
            id=subscriber_id,
            node_id=node_id,
            event_sources=[category_identifier],
        )

        await subscription_state_store.add(subscription_state)

        await observer.synchronise()

        await subscriber_store.remove(subscriber)

        await subscription_state_store.remove(subscription_state)

        await observer.synchronise()

        assert subscriber.sources == [
            EventCategory(
                adapter=event_storage_adapter, category=category_identifier
            )
        ]


class TestSynchroniseLogging:
    async def test_logs_on_starting_synchronisation(self):
        context = make_observer()
        observer = context.observer
        logger = context.logger
        node_id = context.node_id

        await observer.synchronise()

        startup_log_event = logger.find_event(
            "event.processing.broker.observer.synchronisation.starting",
        )

        assert startup_log_event is not None
        assert startup_log_event.level == LogLevel.DEBUG
        assert startup_log_event.is_async is True
        assert startup_log_event.context == {"node": node_id}

    async def test_logs_on_completing_synchronisation(self):
        context = make_observer()
        observer = context.observer
        logger = context.logger
        node_id = context.node_id
        subscriber_store = context.subscriber_store
        subscription_state_store = context.subscription_state_store

        old_subscriber_group = data.random_subscriber_group()
        old_subscriber_id = data.random_subscriber_id()

        old_category_identifier = CategoryIdentifier(
            category=data.random_event_category_name()
        )

        old_subscriber = CapturingEventSubscriber(
            group=old_subscriber_group,
            id=old_subscriber_id,
        )

        await subscriber_store.add(old_subscriber)

        old_subscription_state = EventSubscriptionState(
            group=old_subscriber_group,
            id=old_subscriber_id,
            node_id=node_id,
            event_sources=[old_category_identifier],
        )

        await subscription_state_store.add(old_subscription_state)

        await observer.synchronise()

        await subscription_state_store.remove(old_subscription_state)

        new_subscriber_group = data.random_subscriber_group()
        new_subscriber_id = data.random_subscriber_id()

        new_category_identifier = CategoryIdentifier(
            category=data.random_event_category_name()
        )

        new_subscriber = CapturingEventSubscriber(
            group=new_subscriber_group,
            id=new_subscriber_id,
        )

        await subscriber_store.add(new_subscriber)

        new_subscription_state = EventSubscriptionState(
            group=new_subscriber_group,
            id=new_subscriber_id,
            node_id=node_id,
            event_sources=[new_category_identifier],
        )

        await subscription_state_store.add(new_subscription_state)

        await observer.synchronise()

        completion_log_events = logger.find_events(
            "event.processing.broker.observer.synchronisation.complete",
        )

        assert len(completion_log_events) == 2

        last_completion_log_event = completion_log_events[-1]

        assert last_completion_log_event.level == LogLevel.DEBUG
        assert last_completion_log_event.is_async is True
        assert last_completion_log_event.context == {
            "node": node_id,
            "changes": {
                "allocation_count": 1,
                "revocation_count": 1,
            },
        }


class TestObserveStatus:
    async def test_has_a_status_of_initialised_before_running_observe(self):
        context = make_observer()
        observer = context.observer

        assert observer.status == ProcessStatus.INITIALISED

    async def test_sets_status_to_running_while_observe_running(self):
        context = make_observer()
        observer = context.observer

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = observer.status
                if status == ProcessStatus.RUNNING:
                    return

        task = asyncio.create_task(observer.observe())
        try:
            await asyncio.wait_for(
                wait_until_running(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            assert observer.status == ProcessStatus.RUNNING
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_sets_status_to_stopped_when_observe_cancelled(self):
        context = make_observer()
        observer = context.observer

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = observer.status
                if status == ProcessStatus.RUNNING:
                    return

        task = asyncio.create_task(observer.observe())

        await asyncio.wait_for(
            wait_until_running(),
            timeout=timedelta(milliseconds=100).total_seconds(),
        )

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        assert observer.status == ProcessStatus.STOPPED

    async def test_sets_status_to_errored_when_observe_encounters_error(self):
        node_id = data.random_node_id()
        context = make_observer(
            subscription_state_store=ThrowingEventSubscriptionStateStore(
                node_id=node_id
            )
        )
        observer = context.observer

        await asyncio.gather(observer.observe(), return_exceptions=True)

        assert observer.status == ProcessStatus.ERRORED


class TestObserveLogging:
    async def test_logs_on_startup(self):
        context = make_observer(synchronisation_interval=timedelta(seconds=5))
        observer = context.observer
        node_id = context.node_id
        logger = context.logger

        async def observer_running():
            while True:
                await asyncio.sleep(0)
                status = observer.status
                if status == ProcessStatus.RUNNING:
                    return

        task = asyncio.create_task(observer.observe())

        try:
            await asyncio.wait_for(
                observer_running(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            startup_log_event = logger.find_event(
                "event.processing.broker.observer.starting",
            )

            assert startup_log_event is not None
            assert startup_log_event.level == LogLevel.INFO
            assert startup_log_event.is_async is True
            assert startup_log_event.context == {
                "node": node_id,
                "synchronisation_interval_seconds": 5.0,
            }
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_logs_on_shutdown(self):
        context = make_observer()
        observer = context.observer
        logger = context.logger
        node_id = context.node_id

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = observer.status
                if status == ProcessStatus.RUNNING:
                    return

        task = asyncio.create_task(observer.observe())

        await asyncio.wait_for(
            wait_until_running(),
            timeout=timedelta(milliseconds=100).total_seconds(),
        )

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        shutdown_log_event = logger.find_event(
            "event.processing.broker.observer.stopped"
        )

        assert shutdown_log_event is not None
        assert shutdown_log_event.level == LogLevel.INFO
        assert shutdown_log_event.is_async is True
        assert shutdown_log_event.context == {
            "node": node_id,
        }

    async def test_logs_on_error(self):
        node_id = data.random_node_id()
        context = make_observer(
            node_id=node_id,
            subscription_state_store=ThrowingEventSubscriptionStateStore(
                node_id=node_id
            ),
        )
        observer = context.observer
        logger = context.logger

        await asyncio.gather(observer.observe(), return_exceptions=True)

        failed_log_event = logger.find_event(
            "event.processing.broker.observer.failed"
        )

        assert failed_log_event is not None
        assert failed_log_event.level == LogLevel.ERROR
        assert failed_log_event.is_async is True
        assert failed_log_event.context == {"node": node_id}
        assert failed_log_event.exc_info is not None
        assert failed_log_event.exc_info[0] is RuntimeError


class TestObserveSynchronisation:
    async def test_synchronises_subscriptions(self):
        context = make_observer()
        observer = context.observer
        node_id = context.node_id
        subscriber_store = context.subscriber_store
        subscription_state_store = context.subscription_state_store
        event_storage_adapter = context.event_storage_adapter

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_identifier = CategoryIdentifier(
            category=data.random_event_category_name()
        )

        subscriber = CapturingEventSubscriber(
            group=subscriber_group,
            id=subscriber_id,
        )

        await subscriber_store.add(subscriber)

        subscription_state = EventSubscriptionState(
            group=subscriber_group,
            id=subscriber_id,
            node_id=node_id,
            event_sources=[category_identifier],
        )

        await subscription_state_store.add(subscription_state)

        task = asyncio.create_task(observer.observe())

        try:

            async def wait_for_event_sources():
                while True:
                    await asyncio.sleep(0)
                    if len(subscriber.sources) > 0:
                        return subscriber.sources

            event_sources = await asyncio.wait_for(
                wait_for_event_sources(),
                timeout=timedelta(milliseconds=50).total_seconds(),
            )

            assert event_sources == [
                EventCategory(
                    adapter=event_storage_adapter, category=category_identifier
                )
            ]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_synchronises_event_synchronisation_interval(self):
        node_id = data.random_node_id()
        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        subscription_state_store = GeneratingEventSubscriptionStateStore(
            node_id=node_id,
            subscriber_group=subscriber_group,
            subscriber_id=subscriber_id,
        )
        context = make_observer(
            subscription_state_store=subscription_state_store,
            synchronisation_interval=timedelta(milliseconds=20),
        )
        observer = context.observer
        subscriber_store = context.subscriber_store

        subscriber = CapturingEventSubscriber(
            group=subscriber_group,
            id=subscriber_id,
        )

        await subscriber_store.add(subscriber)

        task = asyncio.create_task(observer.observe())

        try:

            async def wait_until_running():
                while True:
                    await asyncio.sleep(0)
                    status = observer.status
                    if status == ProcessStatus.RUNNING:
                        return

            await wait_until_running()
            await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

            task.cancel()

            await asyncio.gather(task, return_exceptions=True)

            assert len(subscriber.sources) <= len(
                subscription_state_store.sources
            )
            assert 2 <= len(subscriber.sources) <= 3
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
