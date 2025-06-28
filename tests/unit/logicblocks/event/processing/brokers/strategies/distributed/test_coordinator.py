import asyncio
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol, cast

from pytest_unordered import unordered

from logicblocks.event.processing import EventSubscriber
from logicblocks.event.processing.broker.strategies.distributed import (
    COORDINATOR_LOCK_NAME,
    DefaultEventSubscriptionCoordinator,
    EventSubscriberState,
    EventSubscriberStateStore,
    EventSubscriptionKey,
    EventSubscriptionState,
    EventSubscriptionStateChange,
    EventSubscriptionStateStore,
    InMemoryEventSubscriberStateStore,
    InMemoryEventSubscriptionStateStore,
)
from logicblocks.event.processing.locks import (
    InMemoryLockManager,
    Lock,
    LockManager,
)
from logicblocks.event.processing.process import ProcessStatus
from logicblocks.event.sources import NoOpEventSourcePartitioner
from logicblocks.event.testing import data
from logicblocks.event.testlogging.logger import CapturingLogger, LogLevel
from logicblocks.event.testsupport import (
    CapturingEventSubscriber,
    random_capturing_subscriber,
)
from logicblocks.event.types import (
    CategoryIdentifier,
    EventSourceIdentifier,
    StreamIdentifier,
)


class ThrowingEventSubscriberStateStore(EventSubscriberStateStore):
    def __init__(self, node_id: str):
        self._node_id = node_id

    async def add(self, subscriber: EventSubscriber) -> None:
        raise RuntimeError

    async def remove(self, subscriber: EventSubscriber) -> None:
        raise RuntimeError

    async def list(
        self,
        subscriber_group: str | None = None,
        max_time_since_last_seen: timedelta | None = None,
    ) -> Sequence[EventSubscriberState]:
        raise RuntimeError

    async def heartbeat(self, subscriber: EventSubscriber) -> None:
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


def random_event_source_identifier(
    category_name: str | None = None,
) -> EventSourceIdentifier:
    if category_name is None:
        category_name = data.random_event_category_name()
    stream_name = data.random_event_stream_name()

    return StreamIdentifier(category=category_name, stream=stream_name)


@dataclass(frozen=True)
class Context:
    coordinator: DefaultEventSubscriptionCoordinator
    node_id: str
    lock_manager: LockManager
    logger: CapturingLogger
    subscriber_state_store: EventSubscriberStateStore
    subscription_state_store: EventSubscriptionStateStore


class NodeAwareEventSubscriberStateStoreClass(Protocol):
    def __call__(self, node_id: str) -> EventSubscriberStateStore: ...


class NodeAwareEventSubscriptionStateStoreClass(Protocol):
    def __call__(self, node_id: str) -> EventSubscriptionStateStore: ...


def make_coordinator(
    subscriber_state_store_class: NodeAwareEventSubscriberStateStoreClass = InMemoryEventSubscriberStateStore,
    subscription_state_store_class: NodeAwareEventSubscriptionStateStoreClass = InMemoryEventSubscriptionStateStore,
    subscriber_max_time_since_last_seen: timedelta | None = None,
    distribution_interval: timedelta | None = None,
    leadership_max_duration: timedelta | None = None,
    leadership_attempt_interval: timedelta | None = None,
) -> Context:
    node_id = data.random_node_id()

    logger = CapturingLogger.create()
    subscriber_state_store = subscriber_state_store_class(node_id=node_id)
    subscription_state_store = subscription_state_store_class(node_id=node_id)
    lock_manager = InMemoryLockManager()
    partitioner = NoOpEventSourcePartitioner()

    kwargs = {
        "node_id": node_id,
        "lock_manager": lock_manager,
        "logger": logger,
        "subscriber_state_store": subscriber_state_store,
        "subscription_state_store": subscription_state_store,
        "event_source_partitioner": partitioner,
    }
    if subscriber_max_time_since_last_seen is not None:
        kwargs["subscriber_max_time_since_last_seen"] = (
            subscriber_max_time_since_last_seen
        )
    if distribution_interval is not None:
        kwargs["distribution_interval"] = distribution_interval
    if leadership_max_duration is not None:
        kwargs["leadership_max_duration"] = leadership_max_duration
    if leadership_attempt_interval is not None:
        kwargs["leadership_attempt_interval"] = leadership_attempt_interval

    coordinator = DefaultEventSubscriptionCoordinator(**kwargs)

    return Context(
        coordinator=coordinator,
        node_id=node_id,
        lock_manager=lock_manager,
        logger=logger,
        subscriber_state_store=subscriber_state_store,
        subscription_state_store=subscription_state_store,
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
        event_source_identifier = random_event_source_identifier()

        subscriber = random_capturing_subscriber(
            subscription_requests=[event_source_identifier]
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber)

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        assert subscriptions == [
            EventSubscriptionState(
                group=subscriber.group,
                id=subscriber.id,
                node_id=node_id,
                event_sources=[event_source_identifier],
            )
        ]

    async def test_distributes_many_sources_for_single_subscriber_instance(
        self,
    ):
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()

        subscriber = random_capturing_subscriber(
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ]
        )

        context = make_coordinator()
        coordinator = context.coordinator
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber)

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
        event_sequence_identifier = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[event_sequence_identifier],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[event_sequence_identifier],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

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
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

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
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group_1,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group_2,
            subscription_requests=[
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )
        subscriber_3 = random_capturing_subscriber(
            subscriber_group=subscriber_group_2,
            subscription_requests=[
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)
        await subscriber_state_store.add(subscriber_3)

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
    async def test_distributes_additional_sources_to_single_subscriber_instance(
        self,
    ):
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        subscriber = random_capturing_subscriber(
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ]
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber)

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
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

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
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_4,
                event_sequence_identifier_2,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber)

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
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_3,
                event_sequence_identifier_2,
            ],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_3,
                event_sequence_identifier_2,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

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

    async def test_distributes_to_subscribers_with_most_sources_when_different_requests_per_subscriber(
        self,
    ):
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
            ],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        subscriber_3 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)
        await subscriber_state_store.add(subscriber_3)

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

        assert len(subscriptions) == 2
        assert subscriber_1_subscription is None
        assert subscriber_2_subscription is not None
        assert subscriber_3_subscription is not None

        assert len(subscriber_2_subscription.event_sources) == 1
        assert len(subscriber_3_subscription.event_sources) == 1

        sources = subscription_event_sources(subscriptions)

        assert set(sources) == {
            event_sequence_identifier_1,
            event_sequence_identifier_2,
        }


class TestDistributeExistingSubscriptionSubscriberChanges:
    async def test_distributes_to_single_additional_subscriber_instance(self):
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1)

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

        await subscriber_state_store.add(subscriber_2)

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
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        subscriber_3 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1)

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

        await subscriber_state_store.add(subscriber_2)
        await subscriber_state_store.add(subscriber_3)

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
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ],
        )
        subscriber_3 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)
        await subscriber_state_store.add(subscriber_3)

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

        await subscriber_state_store.remove(subscriber_2)
        await subscriber_state_store.remove(subscriber_3)

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
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

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

        await subscriber_state_store.remove(subscriber_1)
        await subscriber_state_store.remove(subscriber_2)

        await coordinator.distribute()

        subscriptions = await subscription_state_store.list()

        assert len(subscriptions) == 0

    async def test_distributes_to_new_subscriber_group_instances(self):
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        subscriber_1_group_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group_1,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        subscriber_2_group_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group_1,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        subscriber_1_group_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group_2,
            subscription_requests=[
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )
        subscriber_2_group_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group_2,
            subscription_requests=[
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1_group_1)
        await subscriber_state_store.add(subscriber_2_group_1)

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

        await subscriber_state_store.add(subscriber_1_group_2)
        await subscriber_state_store.add(subscriber_2_group_2)

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
                if status == ProcessStatus.RUNNING:
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
                if status == ProcessStatus.RUNNING:
                    return

        task = asyncio.create_task(coordinator.coordinate())

        await wait_until_running()

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        assert coordinator.status == ProcessStatus.STOPPED

        async with lock_manager.try_lock(COORDINATOR_LOCK_NAME) as lock:
            assert lock.locked is True

    async def test_releases_lock_if_coordinate_encounters_error(self):
        context = make_coordinator(
            subscriber_state_store_class=ThrowingEventSubscriberStateStore
        )
        coordinator = context.coordinator
        lock_manager = context.lock_manager

        await asyncio.gather(coordinator.coordinate(), return_exceptions=True)

        assert coordinator.status == ProcessStatus.ERRORED
        async with lock_manager.try_lock(COORDINATOR_LOCK_NAME) as lock:
            assert lock.locked is True

    async def test_releases_lock_after_coordinator_leadership_max_duration_has_passed(
        self,
    ):
        context = make_coordinator(
            distribution_interval=timedelta(milliseconds=20),
            leadership_max_duration=timedelta(milliseconds=50),
        )
        coordinator = context.coordinator

        task = asyncio.create_task(coordinator.coordinate())

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = coordinator.status
                if status == ProcessStatus.RUNNING:
                    return

        async def wait_for_lock(
            lock_name: str, timeout: timedelta | None = None
        ) -> Lock:
            async with context.lock_manager.wait_for_lock(
                lock_name, timeout=timeout
            ) as lock:
                return lock

        await wait_until_running()

        lock = await wait_for_lock(
            COORDINATOR_LOCK_NAME, timeout=timedelta(milliseconds=75)
        )

        assert lock.locked is True

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

    async def test_retakes_lock_after_coordinator_leadership_max_duration_has_passed(
        self,
    ):
        context = make_coordinator(
            distribution_interval=timedelta(milliseconds=20),
            leadership_max_duration=timedelta(milliseconds=50),
            leadership_attempt_interval=timedelta(milliseconds=20),
        )
        coordinator = context.coordinator
        lock_manager = context.lock_manager

        task = asyncio.create_task(coordinator.coordinate())

        async def wait_until_status(target_status: ProcessStatus):
            while True:
                await asyncio.sleep(0)
                status = coordinator.status
                if status == target_status:
                    return

        async def wait_for_lock(
            lock_name: str, timeout: timedelta | None = None
        ) -> Lock:
            async with context.lock_manager.wait_for_lock(
                lock_name, timeout=timeout
            ) as lock:
                return lock

        await wait_until_status(ProcessStatus.RUNNING)
        await wait_for_lock(
            COORDINATOR_LOCK_NAME, timeout=timedelta(milliseconds=60)
        )

        await wait_until_status(ProcessStatus.WAITING)

        await wait_until_status(ProcessStatus.RUNNING)

        async with lock_manager.try_lock(COORDINATOR_LOCK_NAME) as lock:
            task.cancel()
            assert lock.locked is False


class TestCoordinateDistribution:
    async def test_distributes_subscriptions(self):
        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()
        subscriber_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name())
        ]
        subscriber = CapturingEventSubscriber(
            group=subscriber_group,
            id=subscriber_id,
            subscription_requests=subscriber_subscription_requests,
        )

        context = make_coordinator()
        coordinator = context.coordinator
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber)

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
        assert (
            subscriptions[0].event_sources == subscriber_subscription_requests
        )

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
                if status == ProcessStatus.RUNNING:
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

            async def coordinator_started():
                while True:
                    await asyncio.sleep(0)
                    status = coordinator.status
                    if status == ProcessStatus.RUNNING:
                        return

            await asyncio.wait_for(
                coordinator_started(),
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
                    if status == ProcessStatus.RUNNING:
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
                    if status == ProcessStatus.RUNNING:
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


class TestDistributeLogging:
    async def test_logs_on_starting_distribution(self):
        context = make_coordinator()
        coordinator = context.coordinator
        logger = context.logger
        node_id = context.node_id

        await coordinator.distribute()

        startup_log_event = logger.find_event(
            "event.processing.broker.coordinator.distribution.starting",
        )

        assert startup_log_event is not None
        assert startup_log_event.level == LogLevel.DEBUG
        assert startup_log_event.is_async is True
        assert startup_log_event.context == {"node": node_id}

    async def test_logs_existing_status_on_distribution(self):
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()

        subscriber = random_capturing_subscriber(
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ]
        )

        context = make_coordinator()
        coordinator = context.coordinator
        node_id = context.node_id
        logger = context.logger
        subscription_state_store = context.subscription_state_store

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

        await coordinator.distribute()

        initial_status_log_event = logger.find_event(
            "event.processing.broker.coordinator.distribution.existing-status",
        )

        assert initial_status_log_event is not None
        assert initial_status_log_event.level == LogLevel.DEBUG
        assert initial_status_log_event.is_async is True
        assert initial_status_log_event.context == {
            "node": node_id,
            "subscriber_groups": {
                subscriber.group: {
                    subscriber.id: {
                        "sources": [
                            event_sequence_identifier_1.serialise(),
                            event_sequence_identifier_2.serialise(),
                        ]
                    }
                }
            },
        }

    async def test_logs_latest_status_on_distribution(self):
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_4,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_1,
            ],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_4,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_1,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        logger = context.logger
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

        await coordinator.distribute()

        initial_status_log_event = logger.find_event(
            "event.processing.broker.coordinator.distribution.latest-status",
        )

        assert initial_status_log_event is not None
        assert initial_status_log_event.level == LogLevel.DEBUG
        assert initial_status_log_event.is_async is True
        assert initial_status_log_event.context == {
            "node": node_id,
            "subscriber_groups": {
                subscriber_group: {
                    subscriber_1.id: {
                        "subscription_requests": unordered(
                            [
                                event_sequence_identifier_1.serialise(),
                                event_sequence_identifier_2.serialise(),
                                event_sequence_identifier_3.serialise(),
                                event_sequence_identifier_4.serialise(),
                            ]
                        )
                    },
                    subscriber_2.id: {
                        "subscription_requests": unordered(
                            [
                                event_sequence_identifier_1.serialise(),
                                event_sequence_identifier_2.serialise(),
                                event_sequence_identifier_3.serialise(),
                                event_sequence_identifier_4.serialise(),
                            ]
                        )
                    },
                },
            },
        }

    async def test_logs_updated_status_on_distribution(self):
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_4,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_1,
            ],
        )
        subscriber_2 = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_4,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_1,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        logger = context.logger
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ],
            )
        )

        await coordinator.distribute()

        updated_status_log_event = logger.find_event(
            "event.processing.broker.coordinator.distribution.updated-status",
        )

        assert updated_status_log_event is not None
        assert updated_status_log_event.level == LogLevel.DEBUG
        assert updated_status_log_event.is_async is True

        context = updated_status_log_event.context
        subscriber_1_status = context["subscriber_groups"][subscriber_group][
            subscriber_1.id
        ]
        subscriber_2_status = context["subscriber_groups"][subscriber_group][
            subscriber_2.id
        ]

        assert context["node"] == node_id
        assert len(subscriber_1_status["sources"]) > 0
        assert len(subscriber_2_status["sources"]) > 0
        assert (
            event_sequence_identifier_1.serialise()
            in subscriber_1_status["sources"]
        )
        assert (
            event_sequence_identifier_2.serialise()
            in subscriber_1_status["sources"]
        )
        assert [
            *subscriber_1_status["sources"],
            *subscriber_2_status["sources"],
        ] == unordered(
            [
                event_sequence_identifier_1.serialise(),
                event_sequence_identifier_2.serialise(),
                event_sequence_identifier_3.serialise(),
                event_sequence_identifier_4.serialise(),
            ]
        )

    async def test_logs_on_completing_distribution(self):
        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()
        event_sequence_identifier_3 = random_event_source_identifier()
        event_sequence_identifier_4 = random_event_source_identifier()
        event_sequence_identifier_5 = random_event_source_identifier()

        subscriber_group = data.random_subscriber_group()

        old_subscriber = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_4,
                event_sequence_identifier_5,
            ],
        )
        continuing_subscriber = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_4,
                event_sequence_identifier_5,
            ],
        )
        new_subscriber = random_capturing_subscriber(
            subscriber_group=subscriber_group,
            subscription_requests=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_4,
                event_sequence_identifier_5,
            ],
        )

        context = make_coordinator()
        coordinator = context.coordinator
        logger = context.logger
        node_id = context.node_id
        subscriber_state_store = context.subscriber_state_store
        subscription_state_store = context.subscription_state_store

        await subscriber_state_store.add(continuing_subscriber)
        await subscriber_state_store.add(new_subscriber)

        await subscription_state_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=old_subscriber.id,
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
                id=continuing_subscriber.id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_3,
                    event_sequence_identifier_4,
                ],
            ),
        )

        await coordinator.distribute()

        startup_log_event = logger.find_event(
            "event.processing.broker.coordinator.distribution.complete",
        )

        assert startup_log_event is not None
        assert startup_log_event.level == LogLevel.DEBUG
        assert startup_log_event.is_async is True
        assert startup_log_event.context == {
            "node": node_id,
            "subscription_changes": {
                "additions": 1,
                "removals": 1,
                "replacements": 1,
            },
        }
