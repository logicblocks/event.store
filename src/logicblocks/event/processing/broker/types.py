from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass

from logicblocks.event.store import EventSource
from logicblocks.event.types.identifier import EventSequenceIdentifier


class EventBroker(ABC):
    @abstractmethod
    async def register(self, subscriber: "EventSubscriber") -> None:
        raise NotImplementedError()


class EventSubscriber(ABC):
    @property
    @abstractmethod
    def group(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def id(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    async def subscribe(self, broker: EventBroker) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def accept(self, source: EventSource) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def revoke(self, source: EventSource) -> None:
        raise NotImplementedError()


@dataclass(frozen=True)
class EventSubscriptionSources:
    subscriber_group: str
    event_sources: Sequence[EventSequenceIdentifier]
