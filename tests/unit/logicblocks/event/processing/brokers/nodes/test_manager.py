import asyncio
from collections import defaultdict
from datetime import timedelta

from logicblocks.event.processing.broker import (
    InMemoryNodeStateStore,
    NodeManager,
)
from logicblocks.event.testing import data
from logicblocks.event.utils.clock import Clock, SystemClock


class CountingInMemoryNodeStateStore(InMemoryNodeStateStore):
    counts: dict[str, int]

    def __init__(self, clock: Clock = SystemClock()):
        super().__init__(clock)
        self.counts = defaultdict(lambda: 0)

    async def heartbeat(self, node_id: str) -> None:
        self.counts[f"heartbeat:{node_id}"] += 1
        return await super().heartbeat(node_id)

    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(minutes=5)
    ) -> None:
        max_time_str = str(int(max_time_since_last_seen.total_seconds()))
        self.counts[f"purge:{max_time_str}"] += 1
        return await super().purge(max_time_since_last_seen)


class TestNodeManager:
    async def test_execute_adds_node_state_when_started(self):
        node_id = data.random_node_id()
        node_state_store = InMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id, node_state_store=node_state_store
        )

        task = asyncio.create_task(manager.execute())

        try:

            async def get_nodes():
                while True:
                    nodes = await node_state_store.list()
                    if len(nodes) > 0:
                        return nodes
                    else:
                        await asyncio.sleep(0)

            nodes = await asyncio.wait_for(
                get_nodes(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            assert len(nodes) == 1
            assert nodes[0].node_id == node_id
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_execute_removes_node_state_when_interrupted(self):
        node_id = data.random_node_id()
        node_state_store = InMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id, node_state_store=node_state_store
        )

        task = asyncio.create_task(manager.execute())

        try:

            async def get_nodes():
                while True:
                    nodes = await node_state_store.list()
                    if len(nodes) > 0:
                        return nodes
                    else:
                        await asyncio.sleep(0)

            await asyncio.wait_for(
                get_nodes(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            task.cancel()

            await asyncio.gather(task, return_exceptions=True)

            nodes = await node_state_store.list()

            assert len(nodes) == 0
        finally:
            if not task.cancelled():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_execute_heartbeats_every_heartbeat_interval(self):
        node_id = data.random_node_id()
        node_state_store = CountingInMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id,
            node_state_store=node_state_store,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        task = asyncio.create_task(manager.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        heartbeat_count = node_state_store.counts[f"heartbeat:{node_id}"]

        assert 2 <= heartbeat_count <= 3

    async def test_execute_purges_every_purge_interval(self):
        node_id = data.random_node_id()
        node_state_store = CountingInMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id,
            node_state_store=node_state_store,
            purge_interval=timedelta(milliseconds=20),
            node_max_age=timedelta(minutes=3),
        )

        task = asyncio.create_task(manager.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        purge_count = node_state_store.counts["purge:180"]

        assert 2 <= purge_count <= 3
