import asyncio
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol, cast

from logicblocks.event.processing.broker import (
    COORDINATOR_LOCK_NAME,
    EventSubscriber,
    EventSubscriberHealth,
    EventSubscriberState,
    EventSubscriberStateStore,
    EventSubscriptionCoordinator,
    EventSubscriptionCoordinatorStatus,
    EventSubscriptionKey,
    EventSubscriptionSourceMappingStore,
    EventSubscriptionState,
    EventSubscriptionStateChange,
    EventSubscriptionStateStore,
    InMemoryEventSubscriberStateStore,
    InMemoryEventSubscriptionSourceMappingStore,
    InMemoryEventSubscriptionStateStore,
    InMemoryLockManager,
    LockManager,
)
from logicblocks.event.processing.broker.types import EventSubscriberKey
from logicblocks.event.store import EventSource
from logicblocks.event.testing import data
from logicblocks.event.testlogging.logger import CapturingLogger, LogLevel
from logicblocks.event.types import (
    EventSequenceIdentifier,
    EventSourceIdentifier,
    StreamIdentifier,
)
from logicblocks.event.types.identifier import CategoryIdentifier


class CapturingEventSubscriber(EventSubscriber):
    sources: list[EventSource]

    def __init__(self, group: str, id: str):
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


class ThrowingEventSubscriberStateStore(EventSubscriberStateStore):
    def __init__(self, node_id: str):
        self._node_id = node_id

    async def add(self, subscriber: EventSubscriberKey) -> None:
        raise RuntimeError

    async def remove(self, subscriber: EventSubscriberKey) -> None:
        raise RuntimeError

    async def list(
        self,
        subscriber_group: str | None = None,
        max_time_since_last_seen: timedelta | None = None,
    ) -> Sequence[EventSubscriberState]:
        raise RuntimeError

    async def heartbeat(self, subscriber: EventSubscriberKey) -> None:
        raise RuntimeError

    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(seconds=360)
    ) -> None:
        raise RuntimeError


class CountingEventSubscriptionStateStore(EventSubscriptionStateStore):
    def __init__(self, node_id: str):
        self._delegate = InMemoryEventSubscriptionStateStore(node_id=node_id)
        self.counts: dict[str, int] = defaultdict(lambda: 0)

    async def list(self) -> Sequence[EventSubscriptionState]:
        self.counts["list"] += 1
        return await self._delegate.list()

    async def get(
        self, key: EventSubscriptionKey
    ) -> EventSubscriptionState | None:
        self.counts["get"] += 1
        return await self._delegate.get(key)

    async def add(self, subscription: EventSubscriptionState) -> None:
        self.counts["add"] += 1
        await self._delegate.add(subscription)

    async def remove(self, subscription: EventSubscriptionState) -> None:
        self.counts["remove"] += 1
        await self._delegate.remove(subscription)

    async def replace(self, subscription: EventSubscriptionState) -> None:
        self.counts["replace"] += 1
        await self._delegate.replace(subscription)

    async def apply(
        self, changes: Sequence[EventSubscriptionStateChange]
    ) -> None:
        self.counts["apply"] += 1
        await self._delegate.apply(changes)


def random_subscriber(
    subscriber_group: str | None = None,
) -> CapturingEventSubscriber:
    if subscriber_group is None:
        subscriber_group = data.random_subscriber_group()
    subscriber_id = data.random_subscriber_id()

    return CapturingEventSubscriber(group=subscriber_group, id=subscriber_id)


def random_event_source_identifier(
    category_name: str | None = None,
) -> EventSourceIdentifier:
    if category_name is None:
        category_name = data.random_event_category_name()
    stream_name = data.random_event_stream_name()

    return StreamIdentifier(category=category_name, stream=stream_name)


@dataclass(frozen=True)
class Context:
    coordinator: EventSubscriptionCoordinator
    node_id: str
    lock_manager: LockManager
    logger: CapturingLogger
    subscriber_state_store: EventSubscriberStateStore
    subscription_state_store: EventSubscriptionStateStore
    subscription_source_mapping_store: EventSubscriptionSourceMappingStore


class NodeAwareEventSubscriberStateStoreClass(Protocol):
    def __call__(self, node_id: str) -> EventSubscriberStateStore: ...


class NodeAwareEventSubscriptionStateStoreClass(Protocol):
    def __call__(self, node_id: str) -> EventSubscriptionStateStore: ...


def make_coordinator(
    subscriber_state_store_class: NodeAwareEventSubscriberStateStoreClass = InMemoryEventSubscriberStateStore,
    subscription_state_store_class: NodeAwareEventSubscriptionStateStoreClass = InMemoryEventSubscriptionStateStore,
    subscriber_max_time_since_last_seen: timedelta | None = None,
    distribution_interval: timedelta | None = None,
) -> Context:
    node_id = data.random_node_id()

    logger = CapturingLogger.create()
    subscriber_state_store = subscriber_state_store_class(node_id=node_id)
    subscription_state_store = subscription_state_store_class(node_id=node_id)
    subscription_source_mapping_store = (
        InMemoryEventSubscriptionSourceMappingStore()
    )
    lock_manager = InMemoryLockManager()

    kwargs = {
        "node_id": node_id,
        "lock_manager": lock_manager,
        "logger": logger,
        "subscriber_state_store": subscriber_state_store,
        "subscription_state_store": subscription_state_store,
        "subscription_source_mapping_store": subscription_source_mapping_store,
    }
    if subscriber_max_time_since_last_seen is not None:
        kwargs["subscriber_max_time_since_last_seen"] = (
            subscriber_max_time_since_last_seen
        )
    if distribution_interval is not None:
        kwargs["distribution_interval"] = distribution_interval

    coordinator = EventSubscriptionCoordinator(**kwargs)

    return Context(
        coordinator=coordinator,
        node_id=node_id,
        lock_manager=lock_manager,
        logger=logger,
        subscriber_state_store=subscriber_state_store,
        subscription_state_store=subscription_state_store,
        subscription_source_mapping_store=subscription_source_mapping_store,
    )


def subscriptions_with_event_source_count(
    subscriptions: Sequence[EventSubscriptionState], count: int
) -> Sequence[EventSubscriptionState]:
    return [
        subscription
        for subscription in subscriptions
        if len(subscription.event_sources) == count
    ]


def subscriptions_for_subscriber_group(
    subscriptions: Sequence[EventSubscriptionState], subscriber_group: str
):
    return [
        subscription
        for subscription in subscriptions
        if subscription.group == subscriber_group
    ]


def subscription_for_subscriber_key(
    subscriptions: Sequence[EventSubscriptionState],
    subscriber_group: str,
    subscriber_id: str,
) -> EventSubscriptionState | None:
    return next(
        (
            subscription
            for subscription in subscriptions
            if subscription.group == subscriber_group
            and subscription.id == subscriber_id
        ),
        None,
    )


def subscription_event_sources(
    subscriptions: Sequence[EventSubscriptionState],
) -> Sequence[EventSourceIdentifier]:
    return [
        event_source
        for subscription in subscriptions
        for event_source in subscription.event_sources
    ]


class TestDistributeNoSubscriptions:
    async def test_distributes_single_source_for_single_subscriber_instance(
        self,
    ):
        subscriber = random_subscriber()
        event_sequence_identifier = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber.key)
        await subscription_source_mapping_store.add(
            subscriber_group=subscriber.group,
            event_sources=[event_sequence_identifier],
        )

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        assert subscriptions == [
            EventSubscriptionState(
                group=subscriber.group,
                id=subscriber.id,
                node_id=node_id,
                event_sources=[event_sequence_identifier],
            )
        ]

    async def test_distributes_many_sources_for_single_subscriber_instance(
        self,
    ):
        subscriber = random_subscriber()

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber.key)
        await subscription_source_mapping_store.add(
            subscriber_group=subscriber.group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscription = subscription_for_subscriber_key(
            subscriptions, subscriber.group, subscriber.id
        )

        assert subscription is not None
        assert set(subscription.event_sources) == {
            event_sequence_identifier_1,
            event_sequence_identifier_2,
            event_sequence_identifier_3,
        }

    async def test_distributes_single_source_when_multiple_subscriber_instances(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_2 = random_subscriber(subscriber_group=subscriber_group)

        event_sequence_identifier = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_1.key)
        await subscriber_state_store.add(subscriber_2.key)

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group,
            event_sources=[event_sequence_identifier],
        )

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscriptions_without_source = subscriptions_with_event_source_count(
            subscriptions, 0
        )
        subscriptions_with_source = subscriptions_with_event_source_count(
            subscriptions, 1
        )

        assert len(subscriptions_without_source) == 1
        assert len(subscriptions_with_source) == 1
        assert subscriptions_with_source[0].event_sources == [
            event_sequence_identifier
        ]

    async def test_distributes_multiple_source_across_multiple_subscriber_instances(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_2 = random_subscriber(subscriber_group=subscriber_group)

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_1.key)
        await subscriber_state_store.add(subscriber_2.key)

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscriptions_with_one_source = subscriptions_with_event_source_count(
            subscriptions, 1
        )
        subscriptions_with_two_sources = subscriptions_with_event_source_count(
            subscriptions, 2
        )

        assert len(subscriptions_with_one_source) == 1
        assert len(subscriptions_with_two_sources) == 1

        event_sources = subscription_event_sources(subscriptions)

        assert set(event_sources) == {
            event_sequence_identifier_1,
            event_sequence_identifier_2,
            event_sequence_identifier_3,
        }

    async def test_distributes_across_multiple_subscriber_groups(self):
        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        subscriber_1 = random_subscriber(subscriber_group=subscriber_group_1)
        subscriber_2 = random_subscriber(subscriber_group=subscriber_group_2)
        subscriber_3 = random_subscriber(subscriber_group=subscriber_group_2)

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_1.key)
        await subscriber_state_store.add(subscriber_2.key)
        await subscriber_state_store.add(subscriber_3.key)

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group_1,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group_2,
            event_sources=[
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscriber_group_1_subscriptions = subscriptions_for_subscriber_group(
            subscriptions, subscriber_group_1
        )
        subscriber_group_2_subscriptions = subscriptions_for_subscriber_group(
            subscriptions, subscriber_group_2
        )

        assert len(subscriber_group_1_subscriptions) == 1
        assert subscriber_group_1_subscriptions[0].id == subscriber_1.id
        assert set(subscriber_group_1_subscriptions[0].event_sources) == {
            event_sequence_identifier_1,
            event_sequence_identifier_2,
        }

        assert len(subscriber_group_2_subscriptions[0].event_sources) == 1
        assert len(subscriber_group_2_subscriptions[1].event_sources) == 1

        event_sources = subscription_event_sources(
            subscriber_group_2_subscriptions
        )

        assert set(event_sources) == {
            event_sequence_identifier_3,
            event_sequence_identifier_4,
        }


class TestDistributeExistingSubscriptionsSourceChanges:
    async def test_distributes_additional_source_to_single_subscriber_instance(
        self,
    ):
        subscriber = random_subscriber()

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber.key)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber.group,
                id=subscriber.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ],
            ),
        )

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber.group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscription = subscription_for_subscriber_key(
            subscriptions, subscriber.group, subscriber.id
        )

        assert len(subscriptions) == 1
        assert subscription is not None
        assert set(subscription.event_sources) == {
            event_sequence_identifier_1,
            event_sequence_identifier_2,
            event_sequence_identifier_3,
            event_sequence_identifier_4,
        }

    async def test_distributes_additional_sources_to_multiple_subscriber_instances(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_2 = random_subscriber(subscriber_group=subscriber_group)

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_1.key)
        await subscriber_state_store.add(subscriber_2.key)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                ],
            ),
        )
        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_2.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_2,
                ],
            ),
        )

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_4,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_1,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscriber_1_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_1.id
        )
        subscriber_2_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_2.id
        )

        assert subscriber_1_subscription is not None
        assert subscriber_2_subscription is not None

        assert len(subscriber_1_subscription.event_sources) == 2
        assert len(subscriber_2_subscription.event_sources) == 2

        assert (
            event_sequence_identifier_1
            in subscriber_1_subscription.event_sources
        )
        assert (
            event_sequence_identifier_2
            in subscriber_2_subscription.event_sources
        )

        sources = subscription_event_sources(subscriptions)

        assert set(sources) == {
            event_sequence_identifier_1,
            event_sequence_identifier_2,
            event_sequence_identifier_3,
            event_sequence_identifier_4,
        }

    async def test_removes_sources_no_longer_registered_for_subscriber_group_from_single_instance(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        subscriber = random_subscriber(subscriber_group=subscriber_group)

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber.key)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                    event_sequence_identifier_3,
                    event_sequence_identifier_4,
                ],
            ),
        )

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_4,
                event_sequence_identifier_2,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber.id
        )

        assert len(subscriptions) == 1
        assert subscription is not None
        assert set(subscription.event_sources) == {
            event_sequence_identifier_2,
            event_sequence_identifier_4,
        }

    async def test_removes_sources_no_longer_registered_for_subscriber_group_from_multiple_instances(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_2 = random_subscriber(subscriber_group=subscriber_group)

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_1.key)
        await subscriber_state_store.add(subscriber_2.key)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ],
            ),
        )
        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_2.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_3,
                    event_sequence_identifier_4,
                ],
            ),
        )

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_3,
                event_sequence_identifier_2,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscriber_1_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_1.id
        )
        subscriber_2_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_2.id
        )

        assert len(subscriptions) == 2
        assert subscriber_1_subscription is not None
        assert subscriber_2_subscription is not None

        assert (
            event_sequence_identifier_2
            in subscriber_1_subscription.event_sources
        )
        assert (
            event_sequence_identifier_3
            in subscriber_2_subscription.event_sources
        )

        sources = subscription_event_sources(subscriptions)

        assert set(sources) == {
            event_sequence_identifier_2,
            event_sequence_identifier_3,
        }


class TestDistributeExistingSubscriptionSubscriberChanges:
    async def test_distributes_to_single_additional_subscriber_instance(self):
        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_2 = random_subscriber(subscriber_group=subscriber_group)

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_1.key)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ],
            ),
        )

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        await subscriber_state_store.add(subscriber_2.key)

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscriber_1_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_1.id
        )
        subscriber_2_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_2.id
        )

        assert len(subscriptions) == 2
        assert subscriber_1_subscription is not None
        assert subscriber_2_subscription is not None

        assert set(subscriber_1_subscription.event_sources) == {
            event_sequence_identifier_1,
            event_sequence_identifier_2,
        }
        assert set(subscriber_2_subscription.event_sources) == set()

    async def test_distributes_to_multiple_additional_subscriber_instances(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_2 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_3 = random_subscriber(subscriber_group=subscriber_group)

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_1.key)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ],
            ),
        )

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        await subscriber_state_store.add(subscriber_2.key)
        await subscriber_state_store.add(subscriber_3.key)

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscriber_1_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_1.id
        )
        subscriber_2_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_2.id
        )
        subscriber_3_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_3.id
        )

        assert len(subscriptions) == 3
        assert subscriber_1_subscription is not None
        assert subscriber_2_subscription is not None
        assert subscriber_3_subscription is not None

        assert set(subscriber_1_subscription.event_sources) == {
            event_sequence_identifier_1,
            event_sequence_identifier_2,
        }
        assert set(subscriber_2_subscription.event_sources) == set()
        assert set(subscriber_3_subscription.event_sources) == set()

    async def test_redistributes_subscriptions_from_removed_subscriber_instances(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_2 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_3 = random_subscriber(subscriber_group=subscriber_group)

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_1.key)
        await subscriber_state_store.add(subscriber_2.key)
        await subscriber_state_store.add(subscriber_3.key)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                node_id=node_id,
                event_sources=[event_sequence_identifier_1],
            ),
        )
        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_2.id,
                node_id=node_id,
                event_sources=[event_sequence_identifier_2],
            )
        )
        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_3.id,
                node_id=node_id,
                event_sources=[event_sequence_identifier_3],
            )
        )

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ],
        )

        await subscriber_state_store.remove(subscriber_2.key)
        await subscriber_state_store.remove(subscriber_3.key)

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscriber_1_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_1.id
        )
        subscriber_2_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_2.id
        )
        subscriber_3_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group, subscriber_3.id
        )

        assert subscriber_1_subscription is not None
        assert subscriber_2_subscription is None
        assert subscriber_3_subscription is None

        assert set(subscriber_1_subscription.event_sources) == {
            event_sequence_identifier_1,
            event_sequence_identifier_2,
            event_sequence_identifier_3,
        }

    async def test_removes_subscriptions_for_a_subscriber_group_without_any_subscriber_instances(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_2 = random_subscriber(subscriber_group=subscriber_group)

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_1.key)
        await subscriber_state_store.add(subscriber_2.key)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                node_id=node_id,
                event_sources=[event_sequence_identifier_1],
            ),
        )
        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_2.id,
                node_id=node_id,
                event_sources=[event_sequence_identifier_2],
            )
        )

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        await subscriber_state_store.remove(subscriber_1.key)
        await subscriber_state_store.remove(subscriber_2.key)

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        assert len(subscriptions) == 0

    async def test_removes_subscription_sources_for_a_subscriber_group_without_any_subscriber_instances(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_2 = random_subscriber(subscriber_group=subscriber_group)

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_1.key)
        await subscriber_state_store.add(subscriber_2.key)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                node_id=node_id,
                event_sources=[event_sequence_identifier_1],
            ),
        )
        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_2.id,
                node_id=node_id,
                event_sources=[event_sequence_identifier_2],
            )
        )

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        await subscriber_state_store.remove(subscriber_1.key)
        await subscriber_state_store.remove(subscriber_2.key)

        await coordinator.distribute()

        subscription_sources = await subscription_source_mapping_store.list()

        assert len(subscription_sources) == 0

    async def test_distributes_to_new_subscriber_group_instances(self):
        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        subscriber_1_group_1 = random_subscriber(
            subscriber_group=subscriber_group_1
        )
        subscriber_2_group_1 = random_subscriber(
            subscriber_group=subscriber_group_1
        )
        subscriber_1_group_2 = random_subscriber(
            subscriber_group=subscriber_group_2
        )
        subscriber_2_group_2 = random_subscriber(
            subscriber_group=subscriber_group_2
        )

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_1_group_1.key)
        await subscriber_state_store.add(subscriber_2_group_1.key)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group_1,
                id=subscriber_1_group_1.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                ],
            ),
        )
        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group_1,
                id=subscriber_2_group_1.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_2,
                ],
            ),
        )

        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group_1,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group_2,
            event_sources=[
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )

        await subscriber_state_store.add(subscriber_1_group_2.key)
        await subscriber_state_store.add(subscriber_2_group_2.key)

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        subscriber_1_group_1_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group_1, subscriber_1_group_1.id
        )
        subscriber_2_group_1_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group_1, subscriber_2_group_1.id
        )
        subscriber_1_group_2_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group_2, subscriber_1_group_2.id
        )
        subscriber_2_group_2_subscription = subscription_for_subscriber_key(
            subscriptions, subscriber_group_2, subscriber_2_group_2.id
        )

        assert len(subscriptions) == 4
        assert subscriber_1_group_1_subscription is not None
        assert subscriber_2_group_1_subscription is not None
        assert subscriber_1_group_2_subscription is not None
        assert subscriber_2_group_2_subscription is not None

        assert set(subscriber_1_group_1_subscription.event_sources) == {
            event_sequence_identifier_1,
        }
        assert set(subscriber_2_group_1_subscription.event_sources) == {
            event_sequence_identifier_2,
        }

        subscriber_group_2_event_sources = {
            event_source
            for subscriber in [
                subscriber_1_group_2_subscription,
                subscriber_2_group_2_subscription,
            ]
            for event_source in subscriber.event_sources
        }

        assert len(subscriber_1_group_2_subscription.event_sources) == 1
        assert len(subscriber_2_group_2_subscription.event_sources) == 1

        assert subscriber_group_2_event_sources == {
            event_sequence_identifier_3,
            event_sequence_identifier_4,
        }


class TestCoordinateLocking:
    async def test_takes_lock_while_coordinate_running(self):
        context = make_coordinator()
        coordinator = context.coordinator
        lock_manager = context.lock_manager

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = coordinator.status
                if status == EventSubscriptionCoordinatorStatus.RUNNING:
                    return

        task = asyncio.create_task(coordinator.coordinate())

        await wait_until_running()

        async with lock_manager.try_lock(COORDINATOR_LOCK_NAME) as lock:
            task.cancel()
            assert lock.locked is False

    async def test_releases_lock_after_coordinate_stopped(self):
        context = make_coordinator()
        coordinator = context.coordinator
        lock_manager = context.lock_manager

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = coordinator.status
                if status == EventSubscriptionCoordinatorStatus.RUNNING:
                    return

        task = asyncio.create_task(coordinator.coordinate())

        await wait_until_running()

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        assert coordinator.status == EventSubscriptionCoordinatorStatus.STOPPED

        async with lock_manager.try_lock(COORDINATOR_LOCK_NAME) as lock:
            assert lock.locked is True

    async def test_releases_lock_if_coordinate_encounters_error(self):
        context = make_coordinator(
            subscriber_state_store_class=ThrowingEventSubscriberStateStore
        )
        coordinator = context.coordinator
        lock_manager = context.lock_manager

        await asyncio.gather(coordinator.coordinate(), return_exceptions=True)

        assert coordinator.status == EventSubscriptionCoordinatorStatus.ERRORED
        async with lock_manager.try_lock(COORDINATOR_LOCK_NAME) as lock:
            assert lock.locked is True


class TestCoordinateDistribution:
    async def test_distributes_subscriptions(self):
        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()
        subscriber_key = EventSubscriberKey(
            group=subscriber_group, id=subscriber_id
        )

        event_source_identifier = CategoryIdentifier(
            category=data.random_event_category_name()
        )

        context = make_coordinator()
        coordinator = context.coordinator
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store
        subscription_source_mapping_store = (
            context.subscription_source_mapping_store
        )

        await subscriber_state_store.add(subscriber_key)
        await subscription_source_mapping_store.add(
            subscriber_group=subscriber_group,
            event_sources=[event_source_identifier],
        )

        task = asyncio.create_task(coordinator.coordinate())

        async def wait_for_subscription():
            while True:
                await asyncio.sleep(0)
                state = await subscription_state_store.list()
                if len(state) > 0:
                    return state

        subscriptions = await asyncio.wait_for(
            wait_for_subscription(),
            timeout=timedelta(milliseconds=50).total_seconds(),
        )

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        assert len(subscriptions) == 1
        assert subscriptions[0].group == subscriber_group
        assert subscriptions[0].id == subscriber_id
        assert subscriptions[0].event_sources == [event_source_identifier]

    async def test_distributes_every_distribution_interval(self):
        context = make_coordinator(
            subscription_state_store_class=CountingEventSubscriptionStateStore,
            distribution_interval=timedelta(milliseconds=20),
        )
        coordinator = context.coordinator
        subscription_state_store = cast(
            CountingEventSubscriptionStateStore,
            context.subscription_state_store,
        )

        task = asyncio.create_task(coordinator.coordinate())

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = coordinator.status
                if status == EventSubscriptionCoordinatorStatus.RUNNING:
                    return

        await wait_until_running()
        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        assert subscription_state_store.counts["apply"] == 3


class TestCoordinateLogging:
    async def test_logs_on_startup(self):
        context = make_coordinator(
            distribution_interval=timedelta(seconds=5),
            subscriber_max_time_since_last_seen=timedelta(minutes=5),
        )
        node_id = context.node_id
        coordinator = context.coordinator
        logger = context.logger

        task = asyncio.create_task(coordinator.coordinate())

        try:

            async def coordinator_starting():
                while True:
                    await asyncio.sleep(0)
                    status = coordinator.status
                    if status == EventSubscriptionCoordinatorStatus.STARTING:
                        return

            await asyncio.wait_for(
                coordinator_starting(),
                timeout=timedelta(milliseconds=50).total_seconds(),
            )

            startup_event = logger.find_event(
                "event.processing.broker.coordinator.starting"
            )

            assert startup_event is not None
            assert startup_event.level == LogLevel.INFO
            assert startup_event.is_async is True
            assert startup_event.context == {
                "node": node_id,
                "distribution_interval_seconds": 5.0,
                "subscriber_max_time_since_last_seen_seconds": 300.0,
            }
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_logs_on_running(self):
        context = make_coordinator(
            distribution_interval=timedelta(seconds=5),
            subscriber_max_time_since_last_seen=timedelta(minutes=5),
        )
        node_id = context.node_id
        coordinator = context.coordinator
        logger = context.logger

        task = asyncio.create_task(coordinator.coordinate())

        try:

            async def coordinator_running():
                while True:
                    await asyncio.sleep(0)
                    status = coordinator.status
                    if status == EventSubscriptionCoordinatorStatus.RUNNING:
                        return

            await asyncio.wait_for(
                coordinator_running(),
                timeout=timedelta(milliseconds=50).total_seconds(),
            )

            running_event = logger.find_event(
                "event.processing.broker.coordinator.running"
            )

            assert running_event is not None
            assert running_event.level == LogLevel.INFO
            assert running_event.is_async is True
            assert running_event.context == {
                "node": node_id,
            }
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_logs_on_shutdown(self):
        context = make_coordinator(
            distribution_interval=timedelta(seconds=5),
            subscriber_max_time_since_last_seen=timedelta(minutes=5),
        )
        node_id = context.node_id
        coordinator = context.coordinator
        logger = context.logger

        task = asyncio.create_task(coordinator.coordinate())

        try:

            async def coordinator_running():
                while True:
                    await asyncio.sleep(0)
                    status = coordinator.status
                    if status == EventSubscriptionCoordinatorStatus.RUNNING:
                        return

            await asyncio.wait_for(
                coordinator_running(),
                timeout=timedelta(milliseconds=50).total_seconds(),
            )

            task.cancel()

            await asyncio.gather(task, return_exceptions=True)

            shutdown_event = logger.find_event(
                "event.processing.broker.coordinator.stopped"
            )

            assert shutdown_event is not None
            assert shutdown_event.level == LogLevel.INFO
            assert shutdown_event.is_async is True
            assert shutdown_event.context == {"node": node_id}
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_logs_on_error(self):
        context = make_coordinator(
            subscriber_state_store_class=ThrowingEventSubscriberStateStore
        )
        node_id = context.node_id
        coordinator = context.coordinator
        logger = context.logger

        task = asyncio.create_task(coordinator.coordinate())

        await asyncio.gather(task, return_exceptions=True)

        failed_event = logger.find_event(
            "event.processing.broker.coordinator.failed"
        )

        assert failed_event is not None
        assert failed_event.level == LogLevel.ERROR
        assert failed_event.is_async is True
        assert failed_event.context == {"node": node_id}
        assert failed_event.exc_info is not None
        assert failed_event.exc_info[0] is RuntimeError
