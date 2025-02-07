import asyncio
from datetime import timedelta
from typing import Self

from ..sources import EventSubscriptionSourceMappingStore
from ..types import EventSubscriber, EventSubscriberHealth
from .stores import EventSubscriberStateStore, EventSubscriberStore


class EventSubscriberManager:
    def __init__(
        self,
        node_id: str,
        subscriber_store: EventSubscriberStore,
        subscriber_state_store: EventSubscriberStateStore,
        subscription_source_mapping_store: EventSubscriptionSourceMappingStore,
        heartbeat_interval: timedelta = timedelta(seconds=10),
        purge_interval: timedelta = timedelta(minutes=1),
        subscriber_max_age: timedelta = timedelta(minutes=10),
    ):
        self._node_id = node_id
        self._subscriber_store = subscriber_store
        self._subscriber_state_store = subscriber_state_store
        self._subscription_source_mapping_store = (
            subscription_source_mapping_store
        )
        self._heartbeat_interval = heartbeat_interval
        self._purge_interval = purge_interval
        self._subscriber_max_age = subscriber_max_age

    async def add(self, subscriber: EventSubscriber) -> Self:
        await self._subscriber_store.add(subscriber)
        return self

    async def execute(self):
        try:
            await self.register()

            heartbeat_task = asyncio.create_task(self.heartbeat())
            purge_task = asyncio.create_task(self.purge())

            await asyncio.gather(heartbeat_task, purge_task)
        finally:
            await self.unregister()

    async def register(self):
        for subscriber in await self._subscriber_store.list():
            await self._subscriber_state_store.add(subscriber.key)
            await self._subscription_source_mapping_store.add(
                subscriber.group, subscriber.sequences
            )

    async def unregister(self):
        for subscriber in await self._subscriber_store.list():
            await self._subscriber_state_store.remove(subscriber.key)
            await self._subscription_source_mapping_store.remove(
                subscriber.group
            )

    async def heartbeat(self):
        while True:
            for subscriber in await self._subscriber_store.list():
                health = subscriber.health()
                if health == EventSubscriberHealth.HEALTHY:
                    await self._subscriber_state_store.heartbeat(
                        subscriber.key
                    )
            await asyncio.sleep(self._heartbeat_interval.total_seconds())

    async def purge(self):
        while True:
            await self._subscriber_state_store.purge(
                max_time_since_last_seen=self._subscriber_max_age
            )
            await asyncio.sleep(self._purge_interval.total_seconds())
