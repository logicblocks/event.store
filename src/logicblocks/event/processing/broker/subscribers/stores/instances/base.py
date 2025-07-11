from abc import ABC, abstractmethod
from collections.abc import Sequence

from logicblocks.event.types import Event

from ....types import EventSubscriber, EventSubscriberKey


class EventSubscriberStore[E: Event](ABC):
    @abstractmethod
    async def add(self, subscriber: EventSubscriber[E]) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def remove(self, subscriber: EventSubscriber[E]) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def get(self, key: EventSubscriberKey) -> EventSubscriber[E] | None:
        raise NotImplementedError()

    @abstractmethod
    async def list(self) -> Sequence[EventSubscriber[E]]:
        raise NotImplementedError()
