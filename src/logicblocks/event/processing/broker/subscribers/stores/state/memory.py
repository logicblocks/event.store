from collections.abc import Sequence
from datetime import UTC, timedelta

from logicblocks.event.utils.clock import Clock, SystemClock

from ....types import EventSubscriber
from .base import EventSubscriberState, EventSubscriberStateStore


class InMemoryEventSubscriberStateStore(EventSubscriberStateStore):
    def __init__(self, node_id: str, clock: Clock = SystemClock()):
        self.node_id = node_id
        self.clock = clock
        self.subscribers: list[EventSubscriberState] = []

    async def add(self, subscriber: EventSubscriber) -> None:
        existing = next(
            (
                candidate
                for candidate in self.subscribers
                if subscriber.key == candidate.key
            ),
            None,
        )
        if existing is not None:
            await self.heartbeat(subscriber)
            return

        self.subscribers.append(
            EventSubscriberState(
                group=subscriber.group,
                id=subscriber.id,
                node_id=self.node_id,
                subscription_requests=subscriber.subscription_requests,
                last_seen=self.clock.now(UTC),
            )
        )

    async def remove(self, subscriber: EventSubscriber) -> None:
        existing = next(
            (
                candidate
                for candidate in self.subscribers
                if subscriber.key == candidate.key
            ),
            None,
        )
        if existing is None:
            return

        self.subscribers.remove(existing)

    async def list(
        self,
        subscriber_group: str | None = None,
        max_time_since_last_seen: timedelta | None = None,
    ) -> Sequence[EventSubscriberState]:
        subscribers: list[EventSubscriberState] = self.subscribers
        if subscriber_group is not None:
            subscribers = [
                subscriber
                for subscriber in self.subscribers
                if subscriber.group == subscriber_group
            ]
        if max_time_since_last_seen is not None:
            now = self.clock.now(UTC)
            cutoff = now - max_time_since_last_seen
            subscribers = [
                subscriber
                for subscriber in subscribers
                if subscriber.last_seen > cutoff
            ]
        return subscribers

    async def heartbeat(self, subscriber: EventSubscriber) -> None:
        index, existing = next(
            (
                (index, candidate)
                for index, candidate in enumerate(self.subscribers)
                if subscriber.key == candidate.key
            ),
            (None, None),
        )
        if existing is None or index is None:
            return

        self.subscribers[index] = EventSubscriberState(
            group=subscriber.group,
            id=subscriber.id,
            node_id=self.node_id,
            subscription_requests=subscriber.subscription_requests,
            last_seen=self.clock.now(UTC),
        )

    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(minutes=5)
    ) -> None:
        cutoff_time = self.clock.now(UTC) - max_time_since_last_seen
        for subscriber in self.subscribers:
            if subscriber.last_seen <= cutoff_time:
                self.subscribers.remove(subscriber)
