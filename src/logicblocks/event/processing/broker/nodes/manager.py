import asyncio
from datetime import timedelta

from structlog.types import FilteringBoundLogger

from ..logger import default_logger
from .stores import NodeStateStore


class NodeManager:
    def __init__(
        self,
        node_id: str,
        node_state_store: NodeStateStore,
        logger: FilteringBoundLogger = default_logger,
        heartbeat_interval: timedelta = timedelta(seconds=10),
        purge_interval: timedelta = timedelta(minutes=1),
        node_max_age: timedelta = timedelta(minutes=10),
    ):
        self._node_id = node_id
        self._node_state_store = node_state_store
        self._logger = logger.bind(node=node_id)
        self._heartbeat_interval = heartbeat_interval
        self._purge_interval = purge_interval
        self._node_max_age = node_max_age

    async def execute(self):
        await self._logger.ainfo(
            "event.processing.broker.node-manager.starting",
            heartbeat_interval_seconds=self._heartbeat_interval.total_seconds(),
            purge_interval_seconds=self._purge_interval.total_seconds(),
            node_max_age_seconds=self._node_max_age.total_seconds(),
        )

        try:
            await self.register()

            heartbeat_task = asyncio.create_task(self.heartbeat())
            purge_task = asyncio.create_task(self.purge())

            await asyncio.gather(
                heartbeat_task, purge_task, return_exceptions=True
            )
        finally:
            await self.unregister()
            await self._logger.ainfo(
                "event.processing.broker.node-manager.stopped",
            )

    async def register(self):
        await self._node_state_store.add(self._node_id)

    async def unregister(self):
        await self._node_state_store.remove(self._node_id)

    async def heartbeat(self):
        while True:
            await self._logger.ainfo(
                "event.processing.broker.node-manager.sending-heartbeat"
            )
            await self._node_state_store.heartbeat(self._node_id)
            await asyncio.sleep(self._heartbeat_interval.total_seconds())

    async def purge(self):
        while True:
            await self._logger.ainfo(
                "event.processing.broker.node-manager.purging-nodes",
                node_max_age_seconds=self._node_max_age.total_seconds(),
            )
            await self._node_state_store.purge(
                max_time_since_last_seen=self._node_max_age
            )
            await asyncio.sleep(self._purge_interval.total_seconds())
