import asyncio
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, Self

from structlog.types import FilteringBoundLogger

from logicblocks.event.types import str_serialisation_fallback

from ..logger import default_logger
from ..types import EventSubscriber, EventSubscriberHealth
from .stores import EventSubscriberStateStore, EventSubscriberStore


def log_event_name(event: str) -> str:
    return f"event.processing.broker.subscriber-manager.{event}"


class EventSubscriberManager:
    def __init__(
        self,
        node_id: str,
        subscriber_store: EventSubscriberStore,
        subscriber_state_store: EventSubscriberStateStore,
        logger: FilteringBoundLogger = default_logger,
        heartbeat_interval: timedelta = timedelta(seconds=10),
        purge_interval: timedelta = timedelta(minutes=1),
        subscriber_max_age: timedelta = timedelta(minutes=10),
    ):
        self._node_id = node_id
        self._subscriber_store = subscriber_store
        self._subscriber_state_store = subscriber_state_store
        self._logger = logger.bind(node=node_id)
        self._heartbeat_interval = heartbeat_interval
        self._purge_interval = purge_interval
        self._subscriber_max_age = subscriber_max_age

    async def _subscriber_keys(self) -> Sequence[Mapping[str, Any]]:
        return [
            subscriber.key.dict()
            for subscriber in await self._subscriber_store.list()
        ]

    async def add(self, subscriber: EventSubscriber) -> Self:
        await self._subscriber_store.add(subscriber)
        return self

    async def start(self):
        await self._logger.ainfo(
            log_event_name("starting"),
            subscribers=(await self._subscriber_keys()),
            heartbeat_interval_seconds=self._heartbeat_interval.total_seconds(),
            purge_interval_seconds=self._purge_interval.total_seconds(),
            subscriber_max_age_seconds=self._subscriber_max_age.total_seconds(),
        )

        await self.register()

    async def maintain(self):
        heartbeat_task = asyncio.create_task(self.heartbeat())
        purge_task = asyncio.create_task(self.purge())

        await asyncio.gather(
            heartbeat_task, purge_task, return_exceptions=True
        )

    async def stop(self):
        await self.unregister()

        await self._logger.ainfo(
            log_event_name("stopped"),
            subscribers=(await self._subscriber_keys()),
        )

    async def register(self):
        for subscriber in await self._subscriber_store.list():
            await self._logger.ainfo(
                log_event_name("registering-subscriber"),
                subscriber={
                    "group": subscriber.group,
                    "id": subscriber.id,
                    "subscription_requests": [
                        sequence.serialise(fallback=str_serialisation_fallback)
                        for sequence in subscriber.subscription_requests
                    ],
                },
            )
            await self._subscriber_state_store.add(subscriber)

    async def unregister(self):
        for subscriber in await self._subscriber_store.list():
            await self._logger.ainfo(
                log_event_name("unregistering-subscriber"),
                subscriber={"group": subscriber.group, "id": subscriber.id},
            )
            await self._subscriber_state_store.remove(subscriber)

    async def heartbeat(self):
        while True:
            await self._logger.adebug(
                log_event_name("sending-heartbeats"),
                subscribers=[
                    subscriber.key.dict()
                    for subscriber in await self._subscriber_store.list()
                ],
            )
            for subscriber in await self._subscriber_store.list():
                health = subscriber.health()
                if health == EventSubscriberHealth.HEALTHY:
                    await self._logger.adebug(
                        log_event_name("subscriber-healthy"),
                        subscriber=subscriber.key.dict(),
                    )
                    await self._subscriber_state_store.heartbeat(subscriber)
                else:
                    await self._logger.aerror(
                        log_event_name("subscriber-unhealthy"),
                        subscriber=subscriber.key.dict(),
                    )
            await asyncio.sleep(self._heartbeat_interval.total_seconds())

    async def purge(self):
        while True:
            await self._logger.adebug(
                log_event_name("purging-subscribers"),
                subscriber_max_age_seconds=self._subscriber_max_age.total_seconds(),
            )
            await self._subscriber_state_store.purge(
                max_time_since_last_seen=self._subscriber_max_age
            )
            await asyncio.sleep(self._purge_interval.total_seconds())
