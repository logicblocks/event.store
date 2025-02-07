import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol, Sequence

from logicblocks.event.processing.broker import (
    EventSubscriber,
    EventSubscriberHealth,
    EventSubscriberStore,
    EventSubscriptionDifference,
    EventSubscriptionKey,
    EventSubscriptionObserver,
    EventSubscriptionObserverStatus,
    EventSubscriptionState,
    EventSubscriptionStateStore,
    InMemoryEventStoreEventSourceFactory,
    InMemoryEventSubscriberStore,
    InMemoryEventSubscriptionStateStore,
)
from logicblocks.event.store import EventSource
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.store.adapters.base import EventStorageAdapter
from logicblocks.event.store.store import EventCategory
from logicblocks.event.testing import data
from logicblocks.event.types import EventSequenceIdentifier
from logicblocks.event.types.identifier import (
    CategoryIdentifier,
    EventSourceIdentifier,
)


class CapturingEventSubscriber(EventSubscriber):
    sources: list[EventSource]

    def __init__(self, group: str, id: str):
        self.sources = []
        self._group = group
        self._id = id

    @property
    def group(self) -> str:
        return self._group

    @property
    def id(self) -> str:
        return self._id

    @property
    def sequences(self) -> Sequence[EventSequenceIdentifier]:
        return []

    def health(self) -> EventSubscriberHealth:
        return EventSubscriberHealth.HEALTHY

    async def accept(self, source: EventSource) -> None:
        self.sources.append(source)

    async def withdraw(self, source: EventSource) -> None:
        self.sources.remove(source)


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
    observer: EventSubscriptionObserver
    node_id: str
    subscriber_store: EventSubscriberStore
    subscription_state_store: EventSubscriptionStateStore
    event_storage_adapter: EventStorageAdapter


class NodeAwareEventSubscriptionStateStoreClass(Protocol):
    def __call__(self, node_id: str) -> EventSubscriptionStateStore: ...


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
    subscription_difference = EventSubscriptionDifference()
    event_storage_adapter = InMemoryEventStorageAdapter()
    event_source_factory = InMemoryEventStoreEventSourceFactory(
        adapter=event_storage_adapter
    )

    kwargs = {
        "subscriber_store": subscriber_store,
        "subscription_state_store": subscription_state_store,
        "subscription_difference": subscription_difference,
        "event_source_factory": event_source_factory,
    }
    if synchronisation_interval is not None:
        kwargs["synchronisation_interval"] = synchronisation_interval

    observer = EventSubscriptionObserver(**kwargs)

    return Context(
        observer,
        node_id,
        subscriber_store,
        subscription_state_store,
        event_storage_adapter,
    )


class TestSynchronise:
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


class TestObserveStatus:
    async def test_has_a_status_of_stopped_before_running_observe(self):
        context = make_observer()
        observer = context.observer

        assert observer.status == EventSubscriptionObserverStatus.STOPPED

    async def test_sets_status_to_running_while_observe_running(self):
        context = make_observer()
        observer = context.observer

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = observer.status
                if status == EventSubscriptionObserverStatus.RUNNING:
                    return

        task = asyncio.create_task(observer.observe())
        try:
            await asyncio.wait_for(
                wait_until_running(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            assert observer.status == EventSubscriptionObserverStatus.RUNNING
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
                if status == EventSubscriptionObserverStatus.RUNNING:
                    return

        task = asyncio.create_task(observer.observe())

        await asyncio.wait_for(
            wait_until_running(),
            timeout=timedelta(milliseconds=100).total_seconds(),
        )

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        assert observer.status == EventSubscriptionObserverStatus.STOPPED

    async def test_sets_status_to_errored_when_observe_encounters_error(self):
        node_id = data.random_node_id()
        context = make_observer(
            subscription_state_store=ThrowingEventSubscriptionStateStore(
                node_id=node_id
            )
        )
        observer = context.observer

        await asyncio.gather(observer.observe(), return_exceptions=True)

        assert observer.status == EventSubscriptionObserverStatus.ERRORED


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
                    if status == EventSubscriptionObserverStatus.RUNNING:
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
