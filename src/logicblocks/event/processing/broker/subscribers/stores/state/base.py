from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from ....subscriptions import EventSubscriptionKey
from ....types import EventSubscriberKey


@dataclass(frozen=True)
class EventSubscriberState:
    group: str
    id: str
    node_id: str
    last_seen: datetime

    @property
    def key(self) -> EventSubscriberKey:
        return EventSubscriberKey(self.group, self.id)

    @property
    def subscription_key(self) -> EventSubscriptionKey:
        return EventSubscriptionKey(self.group, self.id)


class EventSubscriberStateStore(ABC):
    @abstractmethod
    async def add(self, subscriber: EventSubscriberKey) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def remove(self, subscriber: EventSubscriberKey) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def list(
        self,
        subscriber_group: str | None = None,
        max_time_since_last_seen: timedelta | None = None,
    ) -> Sequence[EventSubscriberState]:
        raise NotImplementedError()

    @abstractmethod
    async def heartbeat(self, subscriber: EventSubscriberKey) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(seconds=360)
    ) -> None:
        raise NotImplementedError()
