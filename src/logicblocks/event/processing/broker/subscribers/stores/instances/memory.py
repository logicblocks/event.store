from typing import Sequence

from logicblocks.event.types import Event

from ....types import EventSubscriber, EventSubscriberKey
from .base import EventSubscriberStore


class InMemoryEventSubscriberStore[E: Event](EventSubscriberStore[E]):
    def __init__(self):
        self.subscribers: dict[EventSubscriberKey, EventSubscriber[E]] = {}

    async def add(self, subscriber: EventSubscriber[E]) -> None:
        self.subscribers[subscriber.key] = subscriber

    async def remove(self, subscriber: EventSubscriber[E]) -> None:
        if subscriber.key not in self.subscribers:
            return
        self.subscribers.pop(subscriber.key)

    async def get(self, key: EventSubscriberKey) -> EventSubscriber[E] | None:
        return self.subscribers.get(key, None)

    async def list(self) -> Sequence[EventSubscriber[E]]:
        return [subscriber for subscriber in self.subscribers.values()]
