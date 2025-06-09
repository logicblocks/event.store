from abc import ABC, abstractmethod
from collections.abc import Sequence

from ....types import EventSubscriber, EventSubscriberKey


class EventSubscriberStore(ABC):
    @abstractmethod
    async def add(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def remove(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def get(self, key: EventSubscriberKey) -> EventSubscriber | None:
        raise NotImplementedError()

    @abstractmethod
    async def list(self) -> Sequence[EventSubscriber]:
        raise NotImplementedError()
