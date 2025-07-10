from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from logicblocks.event.types import BaseEvent, EventSourceIdentifier

from ......types import EventSubscriberKey


@dataclass(frozen=True)
class EventSubscriptionKey[E: BaseEvent]:
    group: str
    id: str


@dataclass(frozen=True)
class EventSubscriptionState[E: BaseEvent]:
    group: str
    id: str
    node_id: str
    event_sources: Sequence[EventSourceIdentifier]

    @property
    def key(self) -> EventSubscriptionKey[E]:
        return EventSubscriptionKey(group=self.group, id=self.id)

    @property
    def subscriber_key(self) -> EventSubscriberKey[E]:
        return EventSubscriberKey(group=self.group, id=self.id)


class EventSubscriptionStateChangeType(StrEnum):
    ADD = "add"
    REMOVE = "remove"
    REPLACE = "replace"


@dataclass(frozen=True)
class EventSubscriptionStateChange[E: BaseEvent]:
    type: EventSubscriptionStateChangeType
    subscription: EventSubscriptionState[E]


class EventSubscriptionStateStore(ABC):
    @abstractmethod
    async def list(self) -> Sequence[EventSubscriptionState[BaseEvent]]:
        raise NotImplementedError

    @abstractmethod
    async def get[E: BaseEvent](
        self, key: EventSubscriptionKey[E]
    ) -> EventSubscriptionState[E] | None:
        raise NotImplementedError

    @abstractmethod
    async def add[E: BaseEvent](
        self, subscription: EventSubscriptionState[E]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def remove[E: BaseEvent](
        self, subscription: EventSubscriptionState[E]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def replace[E: BaseEvent](
        self, subscription: EventSubscriptionState[E]
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def apply(
        self, changes: Sequence[EventSubscriptionStateChange[BaseEvent]]
    ) -> None:
        raise NotImplementedError
