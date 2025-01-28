from collections.abc import Sequence
from datetime import UTC, timedelta

from logicblocks.event.processing.broker.types import EventSubscriber
from logicblocks.event.utils.clock import Clock, SystemClock

from .base import EventSubscriberState, EventSubscriberStore


class InMemoryEventSubscriberStore(EventSubscriberStore):
    def __init__(self, clock: Clock = SystemClock()):
        self.clock = clock
        self.subscribers: list[EventSubscriberState] = []

    async def add(self, subscriber: EventSubscriber) -> None:
        existing = next(
            (
                candidate
                for candidate in self.subscribers
                if subscriber.name == candidate.name
                and subscriber.id == candidate.id
            ),
            None,
        )
        if existing is not None:
            await self.heartbeat(subscriber)
            return

        self.subscribers.append(
            EventSubscriberState(
                name=subscriber.name,
                id=subscriber.id,
                last_seen=self.clock.now(UTC),
            )
        )

    async def list(
        self,
        subscriber_name: str | None = None,
        max_age: timedelta | None = None,
    ) -> Sequence[EventSubscriberState]:
        subscribers: list[EventSubscriberState] = self.subscribers
        if subscriber_name is not None:
            subscribers = [
                subscriber
                for subscriber in self.subscribers
                if subscriber.name == subscriber_name
            ]
        if max_age is not None:
            now = self.clock.now(UTC)
            cutoff = now - max_age
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
                if subscriber.name == candidate.name
                and subscriber.id == candidate.id
            ),
            (None, None),
        )
        if existing is None or index is None:
            raise ValueError(
                f"Unknown subscriber: {subscriber.name} {subscriber.id}"
            )

        self.subscribers[index] = EventSubscriberState(
            name=subscriber.name,
            id=subscriber.id,
            last_seen=self.clock.now(UTC),
        )

    async def purge(self, max_age: timedelta = timedelta(seconds=300)) -> None:
        cutoff_time = self.clock.now(UTC) - max_age
        for subscriber in self.subscribers:
            if subscriber.last_seen <= cutoff_time:
                self.subscribers.remove(subscriber)
