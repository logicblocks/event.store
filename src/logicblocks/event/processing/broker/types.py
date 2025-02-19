from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from logicblocks.event.store import EventSource
from logicblocks.event.types.identifier import EventSourceIdentifier


@dataclass(frozen=True)
class EventSubscriberKey:
    group: str
    id: str

    def dict(self) -> Mapping[str, Any]:
        return {"group": self.group, "id": self.id}


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

    @property
    @abstractmethod
    def subscription_requests(self) -> Sequence[EventSourceIdentifier]:
        raise NotImplementedError

    @abstractmethod
    def health(self) -> EventSubscriberHealth:
        raise NotImplementedError

    @abstractmethod
    async def accept(self, source: EventSource[EventSourceIdentifier]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def withdraw(
        self, source: EventSource[EventSourceIdentifier]
    ) -> None:
        raise NotImplementedError
