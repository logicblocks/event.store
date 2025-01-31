from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass

from logicblocks.event.types import EventSequenceIdentifier


@dataclass(frozen=True)
class EventSubscriptionSources:
    subscriber_group: str
    event_sources: Sequence[EventSequenceIdentifier]


class EventSubscriptionSourcesStore(ABC):
    @abstractmethod
    async def add(
        self,
        subscriber_group: str,
        event_sources: Sequence[EventSequenceIdentifier],
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def remove(self, subscriber_group: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def list(self) -> Sequence[EventSubscriptionSources]:
        raise NotImplementedError()
