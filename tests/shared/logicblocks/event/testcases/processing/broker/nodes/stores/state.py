from abc import abstractmethod
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from logicblocks.event.processing.broker import NodeState, NodeStateStore
from logicblocks.event.testing import data
from logicblocks.event.utils.clock import Clock, StaticClock


class NodeStateStoreCases:
    @abstractmethod
    def construct_store(self, node_id: str, clock: Clock) -> NodeStateStore:
        raise NotImplementedError

    @abstractmethod
    async def read_nodes(self, store: NodeStateStore) -> Sequence[NodeState]:
        raise NotImplementedError

    async def test_registers_node_when_absent(self):
        node_id = data.random_node_id()
        last_seen = datetime.now(UTC)
        clock = StaticClock(now=last_seen)

        store = self.construct_store(node_id=node_id, clock=clock)

        await store.heartbeat()

        nodes = await self.read_nodes(store)

        assert len(nodes) == 1
        assert nodes[0] == NodeState(node_id, last_seen)

    async def test_updates_last_seen_when_present(self):
        node_id = data.random_node_id()

        last_seen_1 = datetime.now(UTC)
        last_seen_2 = last_seen_1 + timedelta(seconds=5)

        clock = StaticClock(now=last_seen_1)

        store = self.construct_store(node_id=node_id, clock=clock)

        await store.heartbeat()

        clock.set(now=last_seen_2)

        await store.heartbeat()

        nodes = await self.read_nodes(store)

        assert len(nodes) == 1
        assert nodes[0] == NodeState(node_id, last_seen_2)

    async def test_removes_node_on_stop(self):
        node_id = data.random_node_id()
        last_seen = datetime.now(UTC)

        clock = StaticClock(now=last_seen)

        store = self.construct_store(node_id=node_id, clock=clock)

        await store.heartbeat()

        await store.stop()

        nodes = await self.read_nodes(store)

        assert len(nodes) == 0
