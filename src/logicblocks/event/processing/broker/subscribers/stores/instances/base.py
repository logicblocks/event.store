from abc import ABC, abstractmethod
from collections.abc import Sequence

from logicblocks.event.sources.base import BaseEvent

from ....types import EventSubscriber, EventSubscriberKey


class EventSubscriberStore(ABC):
    @abstractmethod
    async def add[E: BaseEvent](self, subscriber: EventSubscriber[E]) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def remove[E: BaseEvent](
        self, subscriber: EventSubscriber[E]
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def get[E: BaseEvent](
        self, key: EventSubscriberKey[E]
    ) -> EventSubscriber[E] | None:
        raise NotImplementedError()

    @abstractmethod
    async def list(self) -> Sequence[EventSubscriber[BaseEvent]]:
        raise NotImplementedError()
