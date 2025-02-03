from abc import abstractmethod
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

import pytest

from logicblocks.event.processing.broker import NodeState, NodeStateStore
from logicblocks.event.testing import data
from logicblocks.event.utils.clock import Clock, StaticClock


class NodeStateStoreCases:
    @abstractmethod
    def construct_store(self, clock: Clock) -> NodeStateStore:
        raise NotImplementedError

    @abstractmethod
    async def read_nodes(self, store: NodeStateStore) -> Sequence[NodeState]:
        raise NotImplementedError

    async def test_add_adds_node(self):
        node_id = data.random_node_id()
        last_seen = datetime.now(UTC)
        clock = StaticClock(now=last_seen)

        store = self.construct_store(clock=clock)

        await store.add(node_id)

        nodes = await self.read_nodes(store)

        assert len(nodes) == 1
        assert nodes[0] == NodeState(node_id, last_seen)

    async def test_add_updates_last_seen_if_already_added(self):
        time_1 = datetime.now(UTC)
        clock = StaticClock(time_1)
        node_id = data.random_node_id()

        store = self.construct_store(clock=clock)

        await store.add(node_id)

        time_2 = time_1 + timedelta(seconds=5)
        clock.set(time_2)

        await store.add(node_id)

        states = await store.list()

        assert states[0] == NodeState(
            node_id=node_id,
            last_seen=time_2,
        )

    async def test_heartbeat_updates_last_seen_when_present(self):
        node_id = data.random_node_id()

        last_seen_1 = datetime.now(UTC)
        last_seen_2 = last_seen_1 + timedelta(seconds=5)

        clock = StaticClock(now=last_seen_1)

        store = self.construct_store(clock=clock)

        await store.add(node_id)

        clock.set(now=last_seen_2)

        await store.heartbeat(node_id)

        nodes = await self.read_nodes(store)

        assert len(nodes) == 1
        assert nodes[0] == NodeState(node_id, last_seen_2)

    async def test_raises_if_heartbeat_called_for_unknown_node(self):
        now = datetime.now(UTC)
        clock = StaticClock(now)
        node_id = data.random_node_id()
        store = self.construct_store(clock=clock)

        with pytest.raises(ValueError):
            await store.heartbeat(node_id)

    async def test_list_lists_nodes(self):
        now = datetime.now(UTC)
        clock = StaticClock(now)
        store = self.construct_store(clock=clock)

        node_id_1 = data.random_node_id()
        node_id_2 = data.random_node_id()

        await store.add(node_id_1)
        await store.add(node_id_2)

        found_nodes = await store.list()

        expected_nodes = [
            NodeState(
                node_id=node_id,
                last_seen=now,
            )
            for node_id in [node_id_1, node_id_2]
        ]

        assert set(found_nodes) == set(expected_nodes)

    async def test_list_lists_nodes_more_recently_seen_than_max_time(self):
        now = datetime.now(UTC)
        max_age = timedelta(seconds=60)
        older_than_max_age_time = now - timedelta(seconds=90)
        just_newer_than_max_age_time = now - timedelta(
            seconds=59, milliseconds=999
        )
        recent_time = now - timedelta(seconds=5)

        clock = StaticClock(now)

        store = self.construct_store(clock=clock)

        clock.set(older_than_max_age_time)

        older_than_max_age_node_ids = [str(node_id) for node_id in range(1, 4)]

        for node_id in older_than_max_age_node_ids:
            await store.add(node_id)

        clock.set(just_newer_than_max_age_time)

        just_newer_than_max_age_node_ids = [
            str(node_id) for node_id in range(4, 8)
        ]

        for node_id in just_newer_than_max_age_node_ids:
            await store.add(node_id)

        clock.set(recent_time)

        recent_node_ids = [str(node_id) for node_id in range(8, 12)]

        for node_id in recent_node_ids:
            await store.add(node_id)

        clock.set(now)

        found_states = await store.list(max_time_since_last_seen=max_age)

        just_newer_than_max_age_states = [
            NodeState(
                node_id=node_id,
                last_seen=just_newer_than_max_age_time,
            )
            for node_id in just_newer_than_max_age_node_ids
        ]

        recent_states = [
            NodeState(
                node_id=node_id,
                last_seen=recent_time,
            )
            for node_id in recent_node_ids
        ]

        expected_states = just_newer_than_max_age_states + recent_states

        assert set(found_states) == set(expected_states)

    async def test_remove_removes_node(self):
        node_id = data.random_node_id()
        last_seen = datetime.now(UTC)

        clock = StaticClock(now=last_seen)

        store = self.construct_store(clock=clock)

        await store.add(node_id)

        await store.remove(node_id)

        nodes = await self.read_nodes(store)

        assert len(nodes) == 0

    async def test_remove_raises_if_removing_unknown_subscriber(self):
        now = datetime.now(UTC)
        clock = StaticClock(now)
        store = self.construct_store(clock=clock)

        node_id_1 = data.random_node_id()
        node_id_2 = data.random_node_id()

        await store.add(node_id_1)

        with pytest.raises(ValueError):
            await store.remove(node_id_2)

    async def test_purges_node_that_have_not_been_seen_for_5_minutes_by_default(
        self,
    ):
        now = datetime.now(UTC)
        five_minutes_ago = now - timedelta(minutes=5)
        just_under_five_minutes_ago = now - timedelta(minutes=4, seconds=59)

        clock = StaticClock(now)

        store = self.construct_store(clock=clock)

        node_id_1 = data.random_node_id()
        node_id_2 = data.random_node_id()

        clock.set(five_minutes_ago)

        await store.add(node_id_1)

        clock.set(just_under_five_minutes_ago)

        await store.add(node_id_2)

        clock.set(now)

        await store.purge()

        states = await store.list()

        assert len(states) == 1
        assert states[0].node_id == node_id_2

    async def test_purges_nodes_that_have_not_been_seen_since_specified_max_time(
        self,
    ):
        now = datetime.now(UTC)
        max_age = timedelta(minutes=2)
        two_minutes_ago = now - timedelta(minutes=2)
        just_under_two_minutes_ago = now - timedelta(minutes=1, seconds=59)

        clock = StaticClock(now)
        store = self.construct_store(clock=clock)

        node_id_1 = data.random_node_id()
        node_id_2 = data.random_node_id()

        clock.set(two_minutes_ago)

        await store.add(node_id_1)

        clock.set(just_under_two_minutes_ago)

        await store.add(node_id_2)

        clock.set(now)

        await store.purge(max_time_since_last_seen=max_age)

        states = await store.list()

        assert len(states) == 1
        assert states[0].node_id == node_id_2
