import asyncio
from datetime import timedelta

from .stores import NodeStateStore


class NodeManager:
    def __init__(
        self,
        node_id: str,
        node_state_store: NodeStateStore,
        heartbeat_interval: timedelta = timedelta(seconds=10),
        purge_interval: timedelta = timedelta(minutes=1),
        node_max_age: timedelta = timedelta(minutes=10),
    ):
        self._node_id = node_id
        self._node_state_store = node_state_store
        self._heartbeat_interval = heartbeat_interval
        self._purge_interval = purge_interval
        self._node_max_age = node_max_age

    async def execute(self):
        try:
            await self.register()

            heartbeat_task = asyncio.create_task(self.heartbeat())
            purge_task = asyncio.create_task(self.purge())

            await asyncio.gather(heartbeat_task, purge_task)
        finally:
            await self.unregister()

    async def register(self):
        await self._node_state_store.add(self._node_id)

    async def unregister(self):
        await self._node_state_store.remove(self._node_id)

    async def heartbeat(self):
        while True:
            await self._node_state_store.heartbeat(self._node_id)
            await asyncio.sleep(self._heartbeat_interval.total_seconds())

    async def purge(self):
        while True:
            await self._node_state_store.purge(
                max_time_since_last_seen=self._node_max_age
            )
            await asyncio.sleep(self._purge_interval.total_seconds())
