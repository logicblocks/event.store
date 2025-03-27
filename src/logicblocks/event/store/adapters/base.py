from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence, Set
from enum import StrEnum

from logicblocks.event.store.conditions import NoCondition, WriteCondition
from logicblocks.event.store.constraints import QueryConstraint
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

# type Listable = identifier.Categories | identifier.Streams
# type Readable = identifier.Log | identifier.Category | identifier.Stream
type Saveable = StreamIdentifier
type Scannable = LogIdentifier | CategoryIdentifier | StreamIdentifier
type Latestable = LogIdentifier | CategoryIdentifier | StreamIdentifier


class EventOrderingGuarantee(StrEnum):
    LOG = "log"


class EventStorageAdapter(ABC):
    @abstractmethod
    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent[Name, Payload]],
        condition: WriteCondition = NoCondition,
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
