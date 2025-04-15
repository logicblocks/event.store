from typing import Sequence

from ....types import EventSubscriber, EventSubscriberKey
from .base import EventSubscriberStore


class InMemoryEventSubscriberStore(EventSubscriberStore):
    def __init__(self):
        self.subscribers: dict[EventSubscriberKey, EventSubscriber] = {}

    async def add(self, subscriber: EventSubscriber) -> None:
        self.subscribers[subscriber.key] = subscriber

    async def remove(self, subscriber: EventSubscriber) -> None:
        if subscriber.key not in self.subscribers:
            return
        self.subscribers.pop(subscriber.key)

    async def get(self, key: EventSubscriberKey) -> EventSubscriber | None:
        return self.subscribers.get(key, None)

    async def list(self) -> Sequence[EventSubscriber]:
        return [subscriber for subscriber in self.subscribers.values()]
