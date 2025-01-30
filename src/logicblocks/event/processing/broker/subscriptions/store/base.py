from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from logicblocks.event.types.identifier import EventSequenceIdentifier


@dataclass(frozen=True)
class EventSubscriptionKey:
    group: str
    id: str


@dataclass(frozen=True)
class EventSubscriptionState:
    group: str
    id: str
    event_sources: Sequence[EventSequenceIdentifier]

    @property
    def key(self) -> EventSubscriptionKey:
        return EventSubscriptionKey(group=self.group, id=self.id)


class EventSubscriptionChangeType(StrEnum):
    ADD = "add"
    REMOVE = "remove"
    REPLACE = "replace"


@dataclass(frozen=True)
class EventSubscriptionChange:
    type: EventSubscriptionChangeType
    state: EventSubscriptionState


class EventSubscriptionStore(ABC):
    @abstractmethod
    async def list(self) -> Sequence[EventSubscriptionState]:
        raise NotImplementedError()

    @abstractmethod
    async def get(
        self, key: EventSubscriptionKey
    ) -> EventSubscriptionState | None:
        raise NotImplementedError()

    @abstractmethod
    async def add(self, subscription: EventSubscriptionState) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def remove(self, subscription: EventSubscriptionState) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def replace(self, subscription: EventSubscriptionState) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def apply(self, changes: Sequence[EventSubscriptionChange]) -> None:
        raise NotImplementedError()
