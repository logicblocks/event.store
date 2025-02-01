from logicblocks.event.processing.broker import (
    EventSubscriber,
    EventSubscriberStore,
    EventSubscriptionObserver,
    EventSubscriptionState,
    EventSubscriptionStateStore,
    InMemoryEventStoreEventSourceFactory,
    InMemoryEventSubscriberStore,
    InMemoryEventSubscriptionStateStore,
)
from logicblocks.event.processing.broker.difference import (
    EventSubscriptionDifference,
)
from logicblocks.event.store import EventSource
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.store.adapters.base import EventStorageAdapter
from logicblocks.event.store.store import EventCategory
from logicblocks.event.testing import data
from logicblocks.event.types.identifier import (
    CategoryIdentifier,
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

    async def accept(self, source: EventSource) -> None:
        self.sources.append(source)

    async def withdraw(self, source: EventSource) -> None:
        self.sources.remove(source)


def make_observer() -> tuple[
    EventSubscriptionObserver,
    EventSubscriberStore,
    EventSubscriptionStateStore,
    EventStorageAdapter,
]:
    subscriber_store = InMemoryEventSubscriberStore()
    subscription_state_store = InMemoryEventSubscriptionStateStore()
    subscription_difference = EventSubscriptionDifference()
    event_storage_adapter = InMemoryEventStorageAdapter()
    event_source_factory = InMemoryEventStoreEventSourceFactory(
        adapter=event_storage_adapter
    )

    observer = EventSubscriptionObserver(
        subscriber_store=subscriber_store,
        subscription_state_store=subscription_state_store,
        subscription_difference=subscription_difference,
        event_source_factory=event_source_factory,
    )

    return (
        observer,
        subscriber_store,
        subscription_state_store,
        event_storage_adapter,
    )


class TestEventSubscriptionObserver:
    async def test_applies_new_subscription_to_subscriber(self):
        (
            observer,
            subscriber_store,
            subscription_state_store,
            event_storage_adapter,
        ) = make_observer()

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
        (observer, subscriber_store, subscription_state_store, _) = (
            make_observer()
        )

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
            event_sources=[category_identifier],
        )

        await subscription_state_store.add(subscription_state)

        await observer.synchronise()

        await subscription_state_store.remove(subscription_state)

        await observer.synchronise()

        assert subscriber.sources == []

    async def test_ignores_new_subscription_if_no_subscriber_in_store(self):
        (observer, _, subscription_state_store, event_storage_adapter) = (
            make_observer()
        )

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
            event_sources=[category_identifier],
        )

        await subscription_state_store.add(subscription_state)

        await observer.synchronise()

        assert subscriber.sources == []

    async def test_ignores_old_subscription_if_no_subscriber_in_store(self):
        (
            observer,
            subscriber_store,
            subscription_state_store,
            event_storage_adapter,
        ) = make_observer()

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
