from abc import abstractmethod
from datetime import UTC, datetime, timedelta
from random import shuffle

import pytest

from logicblocks.event.processing.broker import (
    EventSubscriberState,
    EventSubscriberStateStore,
)
from logicblocks.event.processing.broker.types import EventSubscriberKey
from logicblocks.event.testing import (
    data,
)
from logicblocks.event.utils.clock import Clock, StaticClock


class EventSubscriberStateStoreCases:
    @abstractmethod
    def construct_store(
        self, node_id: str, clock: Clock
    ) -> EventSubscriberStateStore:
        raise NotImplementedError()

    async def test_adds_single_subscriber_details(self):
        now = datetime.now(UTC)
        clock = StaticClock(now=now)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        subscriber_key = EventSubscriberKey(
            group=subscriber_group, id=subscriber_id
        )

        await store.add(subscriber_key)

        states = await store.list()

        assert len(states) == 1
        assert states[0] == EventSubscriberState(
            group=subscriber_group,
            id=subscriber_id,
            node_id=node_id,
            last_seen=now,
        )

    async def test_adds_many_subscriber_details(self):
        now = datetime.now(UTC)
        clock = StaticClock(now=now)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_key = EventSubscriberKey(
            group=subscriber_1_group, id=subscriber_1_id
        )

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_key = EventSubscriberKey(
            group=subscriber_2_group, id=subscriber_2_id
        )

        await store.add(subscriber_1_key)
        await store.add(subscriber_2_key)

        states = await store.list()

        assert len(states) == 2
        assert states[0] == EventSubscriberState(
            group=subscriber_1_group,
            id=subscriber_1_id,
            node_id=node_id,
            last_seen=now,
        )
        assert states[1] == EventSubscriberState(
            group=subscriber_2_group,
            id=subscriber_2_id,
            node_id=node_id,
            last_seen=now,
        )

    async def test_adding_already_added_subscriber_updates_last_seen(self):
        time_1 = datetime.now(UTC)
        clock = StaticClock(time_1)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        subscriber_key = EventSubscriberKey(
            group=subscriber_group, id=subscriber_id
        )

        await store.add(subscriber_key)

        time_2 = time_1 + timedelta(seconds=5)
        clock.set(time_2)

        await store.add(subscriber_key)

        states = await store.list()

        assert states[0] == EventSubscriberState(
            group=subscriber_group,
            id=subscriber_id,
            node_id=node_id,
            last_seen=time_2,
        )

    async def test_removes_subscriber(self):
        now = datetime.now(UTC)
        clock = StaticClock(now)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group = data.random_subscriber_group()

        subscriber_1_id = data.random_subscriber_id()
        subscriber_2_id = data.random_subscriber_id()

        subscriber_1_key = EventSubscriberKey(
            group=subscriber_group, id=subscriber_1_id
        )
        subscriber_2_key = EventSubscriberKey(
            group=subscriber_group, id=subscriber_2_id
        )

        await store.add(subscriber_1_key)
        await store.add(subscriber_2_key)

        await store.remove(subscriber_1_key)

        states = await store.list()

        assert states == [
            EventSubscriberState(
                group=subscriber_group,
                id=subscriber_2_id,
                node_id=node_id,
                last_seen=now,
            )
        ]

    async def test_raises_if_removing_unknown_subscriber(self):
        now = datetime.now(UTC)
        clock = StaticClock(now)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group = data.random_subscriber_group()

        subscriber_1_id = data.random_subscriber_id()
        subscriber_2_id = data.random_subscriber_id()

        subscriber_1_key = EventSubscriberKey(
            group=subscriber_group,
            id=subscriber_1_id,
        )
        subscriber_2_key = EventSubscriberKey(
            group=subscriber_group,
            id=subscriber_2_id,
        )

        await store.add(subscriber_1_key)

        with pytest.raises(ValueError):
            await store.remove(subscriber_2_key)

    async def test_lists_subscribers_by_group(self):
        now = datetime.now(UTC)
        clock = StaticClock(now)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        subscriber_group_1_subscriber_keys = [
            EventSubscriberKey(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(1, 4)
        ]
        subscriber_group_2_subscriber_keys = [
            EventSubscriberKey(
                group=subscriber_group_2,
                id=str(id),
            )
            for id in range(1, 4)
        ]

        subscribers = (
            subscriber_group_1_subscriber_keys
            + subscriber_group_2_subscriber_keys
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

        clock = StaticClock(now)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group_1 = data.random_subscriber_group()

        clock.set(older_than_max_age_time)

        older_than_max_age_subscriber_keys = [
            EventSubscriberKey(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(1, 4)
        ]

        for subscriber in older_than_max_age_subscriber_keys:
            await store.add(subscriber)

        clock.set(just_newer_than_max_age_time)

        just_newer_than_max_age_subscriber_keys = [
            EventSubscriberKey(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(4, 8)
        ]

        for subscriber in just_newer_than_max_age_subscriber_keys:
            await store.add(subscriber)

        clock.set(recent_time)

        recent_subscriber_keys = [
            EventSubscriberKey(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(8, 12)
        ]

        for subscriber in recent_subscriber_keys:
            await store.add(subscriber)

        clock.set(now)

        found_states = await store.list(max_time_since_last_seen=max_age)

        just_newer_than_max_age_states = [
            EventSubscriberState(
                group=subscriber.group,
                id=subscriber.id,
                node_id=node_id,
                last_seen=just_newer_than_max_age_time,
            )
            for subscriber in just_newer_than_max_age_subscriber_keys
        ]

        recent_states = [
            EventSubscriberState(
                group=subscriber.group,
                id=subscriber.id,
                node_id=node_id,
                last_seen=recent_time,
            )
            for subscriber in recent_subscriber_keys
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

        clock = StaticClock(now)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        clock.set(older_than_max_age_time)

        older_than_max_age_group_1_subscriber_keys = [
            EventSubscriberKey(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(1, 4)
        ]
        older_than_max_age_group_2_subscriber_keys = [
            EventSubscriberKey(
                group=subscriber_group_2,
                id=str(id),
            )
            for id in range(4, 8)
        ]
        older_than_max_age_subscriber_keys = (
            older_than_max_age_group_1_subscriber_keys
            + older_than_max_age_group_2_subscriber_keys
        )

        for subscriber in older_than_max_age_subscriber_keys:
            await store.add(subscriber)

        clock.set(newer_than_max_age_time)

        newer_than_max_age_group_1_subscriber_keys = [
            EventSubscriberKey(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(8, 12)
        ]
        newer_than_max_age_group_2_subscriber_keys = [
            EventSubscriberKey(
                group=subscriber_group_2,
                id=str(id),
            )
            for id in range(12, 16)
        ]
        newer_than_max_age_subscribers = (
            newer_than_max_age_group_1_subscriber_keys
            + newer_than_max_age_group_2_subscriber_keys
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
                last_seen=newer_than_max_age_time,
            )
            for subscriber in newer_than_max_age_group_1_subscriber_keys
        ]

        assert set(found_states) == set(expected_states)

    async def test_updates_last_seen_on_heartbeat(self):
        now = datetime.now(UTC)
        previous_last_seen_time = now - timedelta(seconds=10)
        updated_last_seen_time = now

        clock = StaticClock(now)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_key = EventSubscriberKey(
            group=subscriber_1_group,
            id=subscriber_1_id,
        )

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_key = EventSubscriberKey(
            group=subscriber_2_group,
            id=subscriber_2_id,
        )

        clock.set(previous_last_seen_time)

        await store.add(subscriber_1_key)
        await store.add(subscriber_2_key)

        clock.set(now)

        await store.heartbeat(subscriber_1_key)

        states = await store.list()

        assert len(states) == 2

        subscriber_1_state = next(
            state for state in states if state.id == subscriber_1_key.id
        )
        subscriber_2_state = next(
            state for state in states if state.id == subscriber_2_key.id
        )

        assert subscriber_1_state.last_seen == updated_last_seen_time
        assert subscriber_2_state.last_seen == previous_last_seen_time

    async def test_raises_if_heartbeat_called_for_unknown_subscriber(self):
        now = datetime.now(UTC)
        clock = StaticClock(now)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_key = EventSubscriberKey(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
        )

        with pytest.raises(ValueError):
            await store.heartbeat(subscriber_key)

    async def test_purges_subscribers_that_have_not_been_seen_for_5_minutes_by_default(
        self,
    ):
        now = datetime.now(UTC)
        five_minutes_ago = now - timedelta(minutes=5)
        just_under_five_minutes_ago = now - timedelta(minutes=4, seconds=59)

        clock = StaticClock(now)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_key = EventSubscriberKey(
            group=subscriber_1_group,
            id=subscriber_1_id,
        )

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_key = EventSubscriberKey(
            group=subscriber_2_group,
            id=subscriber_2_id,
        )

        clock.set(five_minutes_ago)

        await store.add(subscriber_1_key)

        clock.set(just_under_five_minutes_ago)

        await store.add(subscriber_2_key)

        clock.set(now)

        await store.purge()

        states = await store.list()

        assert len(states) == 1
        assert states[0].id == subscriber_2_key.id

    async def test_purges_subscribers_that_have_not_been_seen_since_specified_max_time(
        self,
    ):
        now = datetime.now(UTC)
        max_age = timedelta(minutes=2)
        two_minutes_ago = now - timedelta(minutes=2)
        just_under_two_minutes_ago = now - timedelta(minutes=1, seconds=59)

        clock = StaticClock(now)
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id, clock=clock)

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_key = EventSubscriberKey(
            group=subscriber_1_group,
            id=subscriber_1_id,
        )

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_key = EventSubscriberKey(
            group=subscriber_2_group,
            id=subscriber_2_id,
        )

        clock.set(two_minutes_ago)

        await store.add(subscriber_1_key)

        clock.set(just_under_two_minutes_ago)

        await store.add(subscriber_2_key)

        clock.set(now)

        await store.purge(max_time_since_last_seen=max_age)

        states = await store.list()

        assert len(states) == 1
        assert states[0].id == subscriber_2_key.id
