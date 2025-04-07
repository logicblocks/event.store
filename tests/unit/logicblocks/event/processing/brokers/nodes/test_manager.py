import asyncio
from collections import defaultdict
from datetime import timedelta

from logicblocks.event.processing.broker import (
    InMemoryNodeStateStore,
    NodeManager,
)
from logicblocks.event.testing import data
from logicblocks.event.testlogging.logger import CapturingLogger, LogLevel
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
    async def test_start_logs_starting(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        node_state_store = CountingInMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id,
            node_state_store=node_state_store,
            logger=logger,
            heartbeat_interval=timedelta(seconds=5),
            purge_interval=timedelta(seconds=30),
            node_max_age=timedelta(minutes=3),
        )

        await manager.start()

        startup_event = logger.find_event(
            "event.processing.broker.node-manager.starting"
        )

        assert startup_event is not None
        assert startup_event.level == LogLevel.INFO
        assert startup_event.is_async is True
        assert startup_event.context == {
            "node": node_id,
            "heartbeat_interval_seconds": 5.0,
            "purge_interval_seconds": 30.0,
            "node_max_age_seconds": 180.0,
        }

    async def test_start_registers_node(self):
        node_id = data.random_node_id()
        node_state_store = InMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id, node_state_store=node_state_store
        )

        await manager.start()

        nodes = await node_state_store.list()

        assert len(nodes) == 1
        assert nodes[0].node_id == node_id

    async def test_start_logs_registering_of_node(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        node_state_store = InMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id, node_state_store=node_state_store, logger=logger
        )

        await manager.start()

        register_event = logger.find_event(
            "event.processing.broker.node-manager.registering-node"
        )

        assert register_event is not None
        assert register_event.level == LogLevel.INFO
        assert register_event.is_async is True
        assert register_event.context == {"node": node_id}

    async def test_stop_logs_shutdown(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        node_state_store = InMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id, node_state_store=node_state_store, logger=logger
        )

        await node_state_store.add(node_id)

        await manager.stop()

        shutdown_event = logger.find_event(
            "event.processing.broker.node-manager.stopped"
        )

        assert shutdown_event is not None
        assert shutdown_event.level == LogLevel.INFO
        assert shutdown_event.is_async is True
        assert shutdown_event.context == {
            "node": node_id,
        }

    async def test_stop_unregisters_node(self):
        node_id = data.random_node_id()
        node_state_store = InMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id, node_state_store=node_state_store
        )

        await node_state_store.add(node_id)

        await manager.stop()

        nodes = await node_state_store.list()

        assert len(nodes) == 0

    async def test_stop_logs_unregistering_of_node(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        node_state_store = InMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id, node_state_store=node_state_store, logger=logger
        )

        await node_state_store.add(node_id)

        await manager.stop()

        unregister_event = logger.find_event(
            "event.processing.broker.node-manager.unregistering-node"
        )

        assert unregister_event is not None
        assert unregister_event.level == LogLevel.INFO
        assert unregister_event.is_async is True
        assert unregister_event.context == {"node": node_id}

    async def test_maintain_heartbeats_every_heartbeat_interval(self):
        node_id = data.random_node_id()
        node_state_store = CountingInMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id,
            node_state_store=node_state_store,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        await node_state_store.add(node_id)

        task = asyncio.create_task(manager.maintain())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        heartbeat_count = node_state_store.counts[f"heartbeat:{node_id}"]

        assert 2 <= heartbeat_count <= 3

    async def test_maintain_logs_on_heartbeat(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        node_state_store = CountingInMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id,
            node_state_store=node_state_store,
            heartbeat_interval=timedelta(milliseconds=20),
            logger=logger,
        )

        task = asyncio.create_task(manager.maintain())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        heartbeat_log_events = logger.find_events(
            "event.processing.broker.node-manager.sending-heartbeat"
        )

        assert len(heartbeat_log_events) > 0
        assert heartbeat_log_events[0].level == LogLevel.DEBUG
        assert heartbeat_log_events[0].is_async is True
        assert heartbeat_log_events[0].context == {"node": node_id}

    async def test_maintain_purges_every_purge_interval(self):
        node_id = data.random_node_id()
        node_state_store = CountingInMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id,
            node_state_store=node_state_store,
            purge_interval=timedelta(milliseconds=20),
            node_max_age=timedelta(minutes=3),
        )

        task = asyncio.create_task(manager.maintain())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        purge_count = node_state_store.counts["purge:180"]

        assert 2 <= purge_count <= 3

    async def test_maintain_logs_on_purge(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        node_state_store = CountingInMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id,
            node_state_store=node_state_store,
            logger=logger,
            purge_interval=timedelta(milliseconds=20),
            node_max_age=timedelta(minutes=3),
        )

        task = asyncio.create_task(manager.maintain())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        purge_log_events = logger.find_events(
            "event.processing.broker.node-manager.purging-nodes"
        )

        assert len(purge_log_events) > 0
        assert purge_log_events[0].level == LogLevel.DEBUG
        assert purge_log_events[0].is_async is True
        assert purge_log_events[0].context == {
            "node": node_id,
            "node_max_age_seconds": 180.0,
        }
