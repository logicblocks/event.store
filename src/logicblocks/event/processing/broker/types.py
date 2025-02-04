from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum

from logicblocks.event.store import EventSource


@dataclass(frozen=True)
class EventSubscriberKey:
    group: str
    id: str


class EventSubscriberHealth(StrEnum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


class EventSubscriber(ABC):
    @property
    @abstractmethod
    def group(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def id(self) -> str:
        raise NotImplementedError

    @property
    def key(self) -> EventSubscriberKey:
        return EventSubscriberKey(self.group, self.id)

    @abstractmethod
    def health(self) -> EventSubscriberHealth:
        raise NotImplementedError

    @abstractmethod
    async def accept(self, source: EventSource) -> None:
        raise NotImplementedError

    @abstractmethod
    async def withdraw(self, source: EventSource) -> None:
        raise NotImplementedError
