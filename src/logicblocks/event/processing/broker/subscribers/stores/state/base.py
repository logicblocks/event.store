from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from logicblocks.event.types import EventSourceIdentifier

from ....subscriptions import EventSubscriptionKey
from ....types import EventSubscriber, EventSubscriberKey


@dataclass(frozen=True)
class EventSubscriberState:
    group: str
    id: str
    node_id: str
    subscription_requests: Sequence[EventSourceIdentifier]
    last_seen: datetime

    def __post_init__(self):
        object.__setattr__(
            self, "subscription_requests", tuple(self.subscription_requests)
        )

    @property
    def key(self) -> EventSubscriberKey:
        return EventSubscriberKey(self.group, self.id)

    @property
    def subscription_key(self) -> EventSubscriptionKey:
        return EventSubscriptionKey(self.group, self.id)


class EventSubscriberStateStore(ABC):
    @abstractmethod
    async def add(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError

    @abstractmethod
    async def remove(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError

    @abstractmethod
    async def list(
        self,
        subscriber_group: str | None = None,
        max_time_since_last_seen: timedelta | None = None,
    ) -> Sequence[EventSubscriberState]:
        raise NotImplementedError

    @abstractmethod
    async def heartbeat(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError

    @abstractmethod
    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(minutes=5)
    ) -> None:
        raise NotImplementedError
