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

    async def test_logs_on_startup(self):
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

        task = asyncio.create_task(manager.execute())

        try:

            async def startup_completed():
                while True:
                    if len(await node_state_store.list()) == 0:
                        await asyncio.sleep(0)
                    else:
                        return

            await asyncio.wait_for(
                startup_completed(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

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

        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_logs_on_registering_node(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        node_state_store = InMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id, node_state_store=node_state_store, logger=logger
        )

        task = asyncio.create_task(manager.execute())

        try:

            async def has_node():
                while True:
                    nodes = await node_state_store.list()
                    if len(nodes) > 0:
                        return nodes
                    else:
                        await asyncio.sleep(0)

            await asyncio.wait_for(
                has_node(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            register_event = logger.find_event(
                "event.processing.broker.node-manager.registering-node"
            )

            assert register_event is not None
            assert register_event.level == LogLevel.INFO
            assert register_event.is_async is True
            assert register_event.context == {"node": node_id}
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_logs_on_unregistering_node(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        node_state_store = InMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id, node_state_store=node_state_store, logger=logger
        )

        task = asyncio.create_task(manager.execute())

        try:

            async def has_node():
                while True:
                    nodes = await node_state_store.list()
                    if len(nodes) > 0:
                        return nodes
                    else:
                        await asyncio.sleep(0)

            await asyncio.wait_for(
                has_node(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            task.cancel()

            await asyncio.gather(task, return_exceptions=True)

            unregister_event = logger.find_event(
                "event.processing.broker.node-manager.unregistering-node"
            )

            assert unregister_event is not None
            assert unregister_event.level == LogLevel.INFO
            assert unregister_event.is_async is True
            assert unregister_event.context == {"node": node_id}
        finally:
            if not task.cancelled():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_logs_on_shutdown(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        node_state_store = InMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id, node_state_store=node_state_store, logger=logger
        )

        task = asyncio.create_task(manager.execute())

        try:

            async def startup_completed():
                while True:
                    if len(await node_state_store.list()) == 0:
                        await asyncio.sleep(0)
                    else:
                        return

            await asyncio.wait_for(
                startup_completed(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            task.cancel()

            await asyncio.gather(task, return_exceptions=True)

            shutdown_event = logger.find_event(
                "event.processing.broker.node-manager.stopped"
            )

            assert shutdown_event is not None
            assert shutdown_event.level == LogLevel.INFO
            assert shutdown_event.is_async is True
            assert shutdown_event.context == {
                "node": node_id,
            }

        finally:
            if not task.cancelled():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_logs_on_heartbeat(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        node_state_store = CountingInMemoryNodeStateStore()

        manager = NodeManager(
            node_id=node_id,
            node_state_store=node_state_store,
            heartbeat_interval=timedelta(milliseconds=20),
            logger=logger,
        )

        task = asyncio.create_task(manager.execute())

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

    async def test_logs_on_purge(self):
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

        task = asyncio.create_task(manager.execute())

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
