from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence, Set
from typing import Self

from logicblocks.event.types import (
    CategoryIdentifier,
    JsonPersistable,
    JsonValue,
    LogIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
    StringPersistable,
)

from ..conditions import NoCondition, WriteCondition
from ..constraints import QueryConstraint

# type Listable = identifier.Categories | identifier.Streams
# type Readable = identifier.Log | identifier.Category | identifier.Stream
type Saveable = StreamIdentifier
type Scannable = LogIdentifier | CategoryIdentifier | StreamIdentifier
type Latestable = LogIdentifier | CategoryIdentifier | StreamIdentifier


class EventSerialisationGuarantee(ABC):
    LOG: Self
    CATEGORY: Self
    STREAM: Self

    @abstractmethod
    def lock_name(self, namespace: str, target: Saveable) -> str:
        raise NotImplementedError


class LogEventSerialisationGuarantee(EventSerialisationGuarantee):
    def lock_name(self, namespace: str, target: Saveable) -> str:
        return namespace


class CategoryEventSerialisationGuarantee(EventSerialisationGuarantee):
    def lock_name(self, namespace: str, target: Saveable) -> str:
        return f"{namespace}.{target.category}"


class StreamEventSerialisationGuarantee(EventSerialisationGuarantee):
    def lock_name(self, namespace: str, target: Saveable) -> str:
        return f"{namespace}.{target.category}.{target.stream}"


EventSerialisationGuarantee.LOG = LogEventSerialisationGuarantee()
EventSerialisationGuarantee.CATEGORY = CategoryEventSerialisationGuarantee()
EventSerialisationGuarantee.STREAM = StreamEventSerialisationGuarantee()


class EventStorageAdapter(ABC):
    @abstractmethod
    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent[Name, Payload]],
        condition: WriteCondition = NoCondition(),
    ) -> Sequence[StoredEvent[Name, Payload]]:
        raise NotImplementedError()

    @abstractmethod
    async def latest(
        self, *, target: Latestable
    ) -> StoredEvent[str, JsonValue] | None:
        raise NotImplementedError()

    @abstractmethod
    def scan(
        self,
        *,
        target: Scannable = LogIdentifier(),
        constraints: Set[QueryConstraint] = frozenset(),
    ) -> AsyncIterator[StoredEvent[str, JsonValue]]:
        raise NotImplementedError()
