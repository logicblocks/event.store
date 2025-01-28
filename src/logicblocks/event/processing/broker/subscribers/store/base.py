from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from ...types import EventSubscriber


@dataclass(frozen=True)
class EventSubscriberState:
    name: str
    id: str
    last_seen: datetime


class EventSubscriberStore(ABC):
    @abstractmethod
    async def add(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def list(
        self,
        subscriber_name: str | None = None,
        max_age: timedelta | None = None,
    ) -> Sequence[EventSubscriberState]:
        raise NotImplementedError()

    @abstractmethod
    async def heartbeat(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def purge(self, max_age: timedelta = timedelta(seconds=360)) -> None:
        raise NotImplementedError()
