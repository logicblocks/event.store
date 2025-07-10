from typing import Sequence

from logicblocks.event.types import BaseEvent

from ....types import EventSubscriber, EventSubscriberKey
from .base import EventSubscriberStore


class InMemoryEventSubscriberStore(EventSubscriberStore):
    def __init__(self):
        self.subscribers: dict[
            EventSubscriberKey, EventSubscriber[BaseEvent]
        ] = {}

    async def add(self, subscriber: EventSubscriber[BaseEvent]) -> None:
        self.subscribers[subscriber.key] = subscriber

    async def remove(self, subscriber: EventSubscriber[BaseEvent]) -> None:
        if subscriber.key not in self.subscribers:
            return
        self.subscribers.pop(subscriber.key)

    async def get(
        self, key: EventSubscriberKey
    ) -> EventSubscriber[BaseEvent] | None:
        return self.subscribers.get(key, None)

    async def list(self) -> Sequence[EventSubscriber[BaseEvent]]:
        return [subscriber for subscriber in self.subscribers.values()]
