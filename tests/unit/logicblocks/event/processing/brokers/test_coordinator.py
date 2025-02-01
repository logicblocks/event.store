from collections.abc import Sequence

from logicblocks.event.processing.broker import (
    EventSubscriber,
    EventSubscriberStateStore,
    EventSubscriptionCoordinator,
    EventSubscriptionSourceMappingStore,
    EventSubscriptionState,
    EventSubscriptionStateStore,
    InMemoryEventSubscriberStateStore,
    InMemoryEventSubscriptionSourceMappingStore,
    InMemoryEventSubscriptionStateStore,
    InMemoryLockManager,
)
from logicblocks.event.store import EventSource
from logicblocks.event.testing import data
from logicblocks.event.types import StreamIdentifier
from logicblocks.event.types.identifier import EventSourceIdentifier


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

    async def accept(self, source: EventSource) -> None:
        self.sources.append(source)

    async def withdraw(self, source: EventSource) -> None:
        self.sources.remove(source)


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


def make_coordinator() -> tuple[
    EventSubscriptionCoordinator,
    EventSubscriberStateStore,
    EventSubscriptionStateStore,
    EventSubscriptionSourceMappingStore,
]:
    subscriber_store = InMemoryEventSubscriberStateStore()
    subscription_store = InMemoryEventSubscriptionStateStore()
    subscription_sources_store = InMemoryEventSubscriptionSourceMappingStore()
    lock_manager = InMemoryLockManager()

    coordinator = EventSubscriptionCoordinator(
        lock_manager=lock_manager,
        subscriber_store=subscriber_store,
        subscription_store=subscription_store,
        subscription_sources_store=subscription_sources_store,
    )

    return (
        coordinator,
        subscriber_store,
        subscription_store,
        subscription_sources_store,
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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber)
        await subscription_sources_store.add(
            subscriber_group=subscriber.group,
            event_sources=[event_sequence_identifier],
        )

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

        assert subscriptions == [
            EventSubscriptionState(
                group=subscriber.group,
                id=subscriber.id,
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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber)
        await subscription_sources_store.add(
            subscriber_group=subscriber.group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber_1)
        await subscriber_store.add(subscriber_2)

        await subscription_sources_store.add(
            subscriber_group=subscriber_group,
            event_sources=[event_sequence_identifier],
        )

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber_1)
        await subscriber_store.add(subscriber_2)

        await subscription_sources_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber_1)
        await subscriber_store.add(subscriber_2)
        await subscriber_store.add(subscriber_3)

        await subscription_sources_store.add(
            subscriber_group=subscriber_group_1,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        await subscription_sources_store.add(
            subscriber_group=subscriber_group_2,
            event_sources=[
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber)

        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber.group,
                id=subscriber.id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ],
            ),
        )

        await subscription_sources_store.add(
            subscriber_group=subscriber.group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber_1)
        await subscriber_store.add(subscriber_2)

        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                event_sources=[
                    event_sequence_identifier_1,
                ],
            ),
        )
        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_2.id,
                event_sources=[
                    event_sequence_identifier_2,
                ],
            ),
        )

        await subscription_sources_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_4,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
                event_sequence_identifier_1,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber)

        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber.id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                    event_sequence_identifier_3,
                    event_sequence_identifier_4,
                ],
            ),
        )

        await subscription_sources_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_4,
                event_sequence_identifier_2,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber_1)
        await subscriber_store.add(subscriber_2)

        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ],
            ),
        )
        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_2.id,
                event_sources=[
                    event_sequence_identifier_3,
                    event_sequence_identifier_4,
                ],
            ),
        )

        await subscription_sources_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_3,
                event_sequence_identifier_2,
            ],
        )

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber_1)

        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ],
            ),
        )

        await subscription_sources_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        await subscriber_store.add(subscriber_2)

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber_1)

        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ],
            ),
        )

        await subscription_sources_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        await subscriber_store.add(subscriber_2)
        await subscriber_store.add(subscriber_3)

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber_1)
        await subscriber_store.add(subscriber_2)
        await subscriber_store.add(subscriber_3)

        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                event_sources=[event_sequence_identifier_1],
            ),
        )
        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_2.id,
                event_sources=[event_sequence_identifier_2],
            )
        )
        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_3.id,
                event_sources=[event_sequence_identifier_3],
            )
        )

        await subscription_sources_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
                event_sequence_identifier_3,
            ],
        )

        await subscriber_store.remove(subscriber_2)
        await subscriber_store.remove(subscriber_3)

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber_1)
        await subscriber_store.add(subscriber_2)

        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                event_sources=[event_sequence_identifier_1],
            ),
        )
        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_2.id,
                event_sources=[event_sequence_identifier_2],
            )
        )

        await subscription_sources_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        await subscriber_store.remove(subscriber_1)
        await subscriber_store.remove(subscriber_2)

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

        assert len(subscriptions) == 0

    async def test_removes_subscription_sources_for_a_subscriber_group_without_any_subscriber_instances(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        subscriber_1 = random_subscriber(subscriber_group=subscriber_group)
        subscriber_2 = random_subscriber(subscriber_group=subscriber_group)

        event_sequence_identifier_1 = random_event_source_identifier()
        event_sequence_identifier_2 = random_event_source_identifier()

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber_1)
        await subscriber_store.add(subscriber_2)

        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_1.id,
                event_sources=[event_sequence_identifier_1],
            ),
        )
        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_2.id,
                event_sources=[event_sequence_identifier_2],
            )
        )

        await subscription_sources_store.add(
            subscriber_group=subscriber_group,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )

        await subscriber_store.remove(subscriber_1)
        await subscriber_store.remove(subscriber_2)

        await coordinator.distribute()

        subscription_sources = await subscription_sources_store.list()

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

        (
            coordinator,
            subscriber_store,
            subscription_store,
            subscription_sources_store,
        ) = make_coordinator()

        await subscriber_store.add(subscriber_1_group_1)
        await subscriber_store.add(subscriber_2_group_1)

        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group_1,
                id=subscriber_1_group_1.id,
                event_sources=[
                    event_sequence_identifier_1,
                ],
            ),
        )
        await subscription_store.add(
            EventSubscriptionState(
                group=subscriber_group_1,
                id=subscriber_2_group_1.id,
                event_sources=[
                    event_sequence_identifier_2,
                ],
            ),
        )

        await subscription_sources_store.add(
            subscriber_group=subscriber_group_1,
            event_sources=[
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ],
        )
        await subscription_sources_store.add(
            subscriber_group=subscriber_group_2,
            event_sources=[
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ],
        )

        await subscriber_store.add(subscriber_1_group_2)
        await subscriber_store.add(subscriber_2_group_2)

        await coordinator.distribute()

        subscriptions = await subscription_store.list()

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
