from abc import ABC, abstractmethod
from collections.abc import Sequence

from logicblocks.event.types import BaseEvent

from ....types import EventSubscriber, EventSubscriberKey


class EventSubscriberStore(ABC):
    @abstractmethod
    async def add(self, subscriber: EventSubscriber[BaseEvent]) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def remove(self, subscriber: EventSubscriber[BaseEvent]) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def get(
        self, key: EventSubscriberKey
    ) -> EventSubscriber[BaseEvent] | None:
        raise NotImplementedError()

    @abstractmethod
    async def list(self) -> Sequence[EventSubscriber[BaseEvent]]:
        raise NotImplementedError()
