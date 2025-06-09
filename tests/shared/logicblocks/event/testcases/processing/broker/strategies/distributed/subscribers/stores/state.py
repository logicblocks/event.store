from abc import abstractmethod
from datetime import UTC, datetime, timedelta
from random import shuffle

from logicblocks.event.processing.broker.strategies.distributed import (
    EventSubscriberState,
    EventSubscriberStateStore,
)
from logicblocks.event.testing import (
    data,
)
from logicblocks.event.testsupport import (
    DummyEventSubscriber,
)
from logicblocks.event.types import CategoryIdentifier
from logicblocks.event.utils.clock import Clock, TimezoneRequiredStaticClock


class EventSubscriberStateStoreCases:
    @abstractmethod
    def construct_store(
        self, node_id: str, clock: Clock
    ) -> EventSubscriberStateStore:
        raise NotImplementedError()

    async def test_adds_single_subscriber_details(self):
        now = datetime.now(UTC)
        clock = TimezoneRequiredStaticClock(now=now, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()
        subscriber_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]

        subscriber = DummyEventSubscriber(
            group=subscriber_group,
            id=subscriber_id,
            subscription_requests=subscriber_subscription_requests,
        )

        await store.add(subscriber)

        states = await store.list()

        assert len(states) == 1
        assert states[0] == EventSubscriberState(
            group=subscriber_group,
            id=subscriber_id,
            node_id=node_id,
            subscription_requests=subscriber_subscription_requests,
            last_seen=now,
        )

    async def test_adds_many_subscriber_details(self):
        now = datetime.now(UTC)
        clock = TimezoneRequiredStaticClock(now=now, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]
        subscriber_1 = DummyEventSubscriber(
            group=subscriber_1_group,
            id=subscriber_1_id,
            subscription_requests=subscriber_1_subscription_requests,
        )

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]
        subscriber_2 = DummyEventSubscriber(
            group=subscriber_2_group,
            id=subscriber_2_id,
            subscription_requests=subscriber_2_subscription_requests,
        )

        await store.add(subscriber_1)
        await store.add(subscriber_2)

        states = await store.list()

        assert len(states) == 2
        assert states[0] == EventSubscriberState(
            group=subscriber_1_group,
            id=subscriber_1_id,
            node_id=node_id,
            subscription_requests=subscriber_1_subscription_requests,
            last_seen=now,
        )
        assert states[1] == EventSubscriberState(
            group=subscriber_2_group,
            id=subscriber_2_id,
            node_id=node_id,
            subscription_requests=subscriber_2_subscription_requests,
            last_seen=now,
        )

    async def test_adding_already_added_subscriber_updates_last_seen(self):
        time_1 = datetime.now(UTC)
        clock = TimezoneRequiredStaticClock(now=time_1, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()
        subscriber_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]

        subscriber = DummyEventSubscriber(
            group=subscriber_group,
            id=subscriber_id,
            subscription_requests=subscriber_subscription_requests,
        )

        await store.add(subscriber)

        time_2 = time_1 + timedelta(seconds=5)
        clock.set(time_2)

        await store.add(subscriber)

        states = await store.list()

        assert states[0] == EventSubscriberState(
            group=subscriber_group,
            id=subscriber_id,
            node_id=node_id,
            subscription_requests=subscriber_subscription_requests,
            last_seen=time_2,
        )

    async def test_removes_subscriber(self):
        now = datetime.now(UTC)
        clock = TimezoneRequiredStaticClock(now=now, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group = data.random_subscriber_group()

        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]

        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]

        subscriber_1 = DummyEventSubscriber(
            group=subscriber_group,
            id=subscriber_1_id,
            subscription_requests=subscriber_1_subscription_requests,
        )
        subscriber_2 = DummyEventSubscriber(
            group=subscriber_group,
            id=subscriber_2_id,
            subscription_requests=subscriber_2_subscription_requests,
        )

        await store.add(subscriber_1)
        await store.add(subscriber_2)

        await store.remove(subscriber_1)

        states = await store.list()

        assert states == [
            EventSubscriberState(
                group=subscriber_group,
                id=subscriber_2_id,
                node_id=node_id,
                subscription_requests=subscriber_2_subscription_requests,
                last_seen=now,
            )
        ]

    async def test_does_nothing_if_removing_unknown_subscriber(self):
        now = datetime.now(UTC)
        clock = TimezoneRequiredStaticClock(now=now, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group = data.random_subscriber_group()

        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]

        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]

        subscriber_1 = DummyEventSubscriber(
            group=subscriber_group,
            id=subscriber_1_id,
            subscription_requests=subscriber_1_subscription_requests,
        )
        subscriber_2 = DummyEventSubscriber(
            group=subscriber_group,
            id=subscriber_2_id,
            subscription_requests=subscriber_2_subscription_requests,
        )

        await store.add(subscriber_1)

        await store.remove(subscriber_2)

        states = await store.list()

        assert states == [
            EventSubscriberState(
                group=subscriber_group,
                id=subscriber_1_id,
                node_id=node_id,
                subscription_requests=subscriber_1_subscription_requests,
                last_seen=now,
            )
        ]

    async def test_lists_subscribers_by_group(self):
        now = datetime.now(UTC)
        clock = TimezoneRequiredStaticClock(now=now, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_1_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
        ]
        subscriber_group_2 = data.random_subscriber_group()
        subscriber_group_2_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
        ]

        subscriber_group_1_subscribers = [
            DummyEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
                subscription_requests=subscriber_group_1_subscription_requests,
            )
            for id in range(1, 4)
        ]
        subscriber_group_2_subscribers = [
            DummyEventSubscriber(
                group=subscriber_group_2,
                id=str(id),
                subscription_requests=subscriber_group_2_subscription_requests,
            )
            for id in range(1, 4)
        ]

        subscribers = (
            subscriber_group_1_subscribers + subscriber_group_2_subscribers
        )
        shuffle(subscribers)

        for subscriber in subscribers:
            await store.add(subscriber)

        found_states = await store.list(subscriber_group=subscriber_group_1)

        expected_states = [
            EventSubscriberState(
                group=subscriber_group_1,
                id=str(id),
                node_id=node_id,
                subscription_requests=subscriber_group_1_subscription_requests,
                last_seen=now,
            )
            for id in range(1, 4)
        ]

        assert set(found_states) == set(expected_states)

    async def test_lists_subscribers_more_recently_seen_than_max_time(self):
        now = datetime.now(UTC)
        max_age = timedelta(seconds=60)
        older_than_max_age_time = now - timedelta(seconds=90)
        just_newer_than_max_age_time = now - timedelta(
            seconds=59, milliseconds=999
        )
        recent_time = now - timedelta(seconds=5)

        clock = TimezoneRequiredStaticClock(now=now, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_1_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
        ]

        clock.set(older_than_max_age_time)

        older_than_max_age_subscribers = [
            DummyEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
                subscription_requests=subscriber_group_1_subscription_requests,
            )
            for id in range(1, 4)
        ]

        for subscriber in older_than_max_age_subscribers:
            await store.add(subscriber)

        clock.set(just_newer_than_max_age_time)

        just_newer_than_max_age_subscribers = [
            DummyEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
                subscription_requests=subscriber_group_1_subscription_requests,
            )
            for id in range(4, 8)
        ]

        for subscriber in just_newer_than_max_age_subscribers:
            await store.add(subscriber)

        clock.set(recent_time)

        recent_subscribers = [
            DummyEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
                subscription_requests=subscriber_group_1_subscription_requests,
            )
            for id in range(8, 12)
        ]

        for subscriber in recent_subscribers:
            await store.add(subscriber)

        clock.set(now)

        found_states = await store.list(max_time_since_last_seen=max_age)

        just_newer_than_max_age_states = [
            EventSubscriberState(
                group=subscriber.group,
                id=subscriber.id,
                node_id=node_id,
                subscription_requests=subscriber.subscription_requests,
                last_seen=just_newer_than_max_age_time,
            )
            for subscriber in just_newer_than_max_age_subscribers
        ]

        recent_states = [
            EventSubscriberState(
                group=subscriber.group,
                id=subscriber.id,
                node_id=node_id,
                subscription_requests=subscriber.subscription_requests,
                last_seen=recent_time,
            )
            for subscriber in recent_subscribers
        ]

        expected_states = just_newer_than_max_age_states + recent_states

        assert set(found_states) == set(expected_states)

    async def test_lists_subscribers_by_group_more_recently_seen_than_max_time(
        self,
    ):
        now = datetime.now(UTC)
        max_age = timedelta(seconds=60)
        older_than_max_age_time = now - timedelta(seconds=90)
        newer_than_max_age_time = now - timedelta(seconds=30)

        clock = TimezoneRequiredStaticClock(now=now, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_1_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
        ]

        subscriber_group_2 = data.random_subscriber_group()
        subscriber_group_2_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
        ]

        clock.set(older_than_max_age_time)

        older_than_max_age_group_1_subscribers = [
            DummyEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
                subscription_requests=subscriber_group_1_subscription_requests,
            )
            for id in range(1, 4)
        ]
        older_than_max_age_group_2_subscribers = [
            DummyEventSubscriber(
                group=subscriber_group_2,
                id=str(id),
                subscription_requests=subscriber_group_2_subscription_requests,
            )
            for id in range(4, 8)
        ]
        older_than_max_age_subscriber_keys = (
            older_than_max_age_group_1_subscribers
            + older_than_max_age_group_2_subscribers
        )

        for subscriber in older_than_max_age_subscriber_keys:
            await store.add(subscriber)

        clock.set(newer_than_max_age_time)

        newer_than_max_age_group_1_subscribers = [
            DummyEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
                subscription_requests=subscriber_group_1_subscription_requests,
            )
            for id in range(8, 12)
        ]
        newer_than_max_age_group_2_subscribers = [
            DummyEventSubscriber(
                group=subscriber_group_2,
                id=str(id),
                subscription_requests=subscriber_group_2_subscription_requests,
            )
            for id in range(12, 16)
        ]
        newer_than_max_age_subscribers = (
            newer_than_max_age_group_1_subscribers
            + newer_than_max_age_group_2_subscribers
        )

        for subscriber in newer_than_max_age_subscribers:
            await store.add(subscriber)

        clock.set(now)

        found_states = await store.list(
            subscriber_group=subscriber_group_1,
            max_time_since_last_seen=max_age,
        )

        expected_states = [
            EventSubscriberState(
                group=subscriber.group,
                id=subscriber.id,
                node_id=node_id,
                subscription_requests=subscriber.subscription_requests,
                last_seen=newer_than_max_age_time,
            )
            for subscriber in newer_than_max_age_group_1_subscribers
        ]

        assert set(found_states) == set(expected_states)

    async def test_updates_last_seen_on_heartbeat(self):
        now = datetime.now(UTC)
        previous_last_seen_time = now - timedelta(seconds=10)
        updated_last_seen_time = now

        clock = TimezoneRequiredStaticClock(now=now, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]
        subscriber_1 = DummyEventSubscriber(
            group=subscriber_1_group,
            id=subscriber_1_id,
            subscription_requests=subscriber_1_subscription_requests,
        )

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]
        subscriber_2 = DummyEventSubscriber(
            group=subscriber_2_group,
            id=subscriber_2_id,
            subscription_requests=subscriber_2_subscription_requests,
        )

        clock.set(previous_last_seen_time)

        await store.add(subscriber_1)
        await store.add(subscriber_2)

        clock.set(now)

        await store.heartbeat(subscriber_1)

        states = await store.list()

        assert len(states) == 2

        subscriber_1_state = next(
            state for state in states if state.id == subscriber_1.id
        )
        subscriber_2_state = next(
            state for state in states if state.id == subscriber_2.id
        )

        assert subscriber_1_state.last_seen == updated_last_seen_time
        assert subscriber_2_state.last_seen == previous_last_seen_time

    async def test_does_nothing_if_heartbeat_called_for_unknown_subscriber(
        self,
    ):
        now = datetime.now(UTC)
        clock = TimezoneRequiredStaticClock(now=now, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber = DummyEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(category=data.random_event_category_name()),
            ],
        )

        await store.heartbeat(subscriber)

        assert await store.list() == []

    async def test_purges_subscribers_that_have_not_been_seen_for_5_minutes_by_default(
        self,
    ):
        now = datetime.now(UTC)
        five_minutes_ago = now - timedelta(minutes=5)
        just_under_five_minutes_ago = now - timedelta(minutes=4, seconds=59)

        clock = TimezoneRequiredStaticClock(now=now, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]
        subscriber_1 = DummyEventSubscriber(
            group=subscriber_1_group,
            id=subscriber_1_id,
            subscription_requests=subscriber_1_subscription_requests,
        )

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]
        subscriber_2 = DummyEventSubscriber(
            group=subscriber_2_group,
            id=subscriber_2_id,
            subscription_requests=subscriber_2_subscription_requests,
        )

        clock.set(five_minutes_ago)

        await store.add(subscriber_1)

        clock.set(just_under_five_minutes_ago)

        await store.add(subscriber_2)

        clock.set(now)

        await store.purge()

        states = await store.list()

        assert len(states) == 1
        assert states[0].id == subscriber_2.id

    async def test_purges_subscribers_that_have_not_been_seen_since_specified_max_time(
        self,
    ):
        now = datetime.now(UTC)
        max_age = timedelta(minutes=2)
        two_minutes_ago = now - timedelta(minutes=2)
        just_under_two_minutes_ago = now - timedelta(minutes=1, seconds=59)

        clock = TimezoneRequiredStaticClock(now=now, tz=UTC)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]
        subscriber_1 = DummyEventSubscriber(
            group=subscriber_1_group,
            id=subscriber_1_id,
            subscription_requests=subscriber_1_subscription_requests,
        )

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
            CategoryIdentifier(category=data.random_event_category_name()),
        ]
        subscriber_2 = DummyEventSubscriber(
            group=subscriber_2_group,
            id=subscriber_2_id,
            subscription_requests=subscriber_2_subscription_requests,
        )

        clock.set(two_minutes_ago)

        await store.add(subscriber_1)

        clock.set(just_under_two_minutes_ago)

        await store.add(subscriber_2)

        clock.set(now)

        await store.purge(max_time_since_last_seen=max_age)

        states = await store.list()

        assert len(states) == 1
        assert states[0].id == subscriber_2.id
