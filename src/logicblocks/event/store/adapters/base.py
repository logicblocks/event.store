from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Mapping, Sequence, Set
from typing import overload

from logicblocks.event.sources.constraints import QueryConstraint
from logicblocks.event.types import (
    CategoryIdentifier,
    EventSourceIdentifier,
    JsonPersistable,
    JsonValue,
    LogIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
    StringPersistable,
)

from ..conditions import NoCondition, WriteCondition
from ..types import StreamPublishDefinition

# type Listable = identifier.Categories | identifier.Streams
# type Readable = identifier.Log | identifier.Category | identifier.Stream
type Saveable = StreamIdentifier | CategoryIdentifier
type Scannable = LogIdentifier | CategoryIdentifier | StreamIdentifier
type Latestable = LogIdentifier | CategoryIdentifier | StreamIdentifier


class EventSerialisationGuarantee[Identifier: EventSourceIdentifier](ABC):
    LOG: "LogEventSerialisationGuarantee"
    CATEGORY: "CategoryEventSerialisationGuarantee"
    STREAM: "StreamEventSerialisationGuarantee"

    @abstractmethod
    def lock_name(self, namespace: str, target: Identifier) -> str:
        raise NotImplementedError


type AnyEventSerialisationGuarantee = (
    LogEventSerialisationGuarantee
    | CategoryEventSerialisationGuarantee
    | StreamEventSerialisationGuarantee
)


class LogEventSerialisationGuarantee(
    EventSerialisationGuarantee[
        CategoryIdentifier | StreamIdentifier | LogIdentifier
    ]
):
    def lock_name(
        self,
        namespace: str,
        target: CategoryIdentifier | StreamIdentifier | LogIdentifier,
    ) -> str:
        return namespace


class CategoryEventSerialisationGuarantee(
    EventSerialisationGuarantee[CategoryIdentifier | StreamIdentifier]
):
    def lock_name(
        self, namespace: str, target: CategoryIdentifier | StreamIdentifier
    ) -> str:
        return f"{namespace}.{target.category}"


class StreamEventSerialisationGuarantee(
    EventSerialisationGuarantee[StreamIdentifier]
):
    def lock_name(self, namespace: str, target: StreamIdentifier) -> str:
        return f"{namespace}.{target.category}.{target.stream}"


EventSerialisationGuarantee.LOG = LogEventSerialisationGuarantee()
EventSerialisationGuarantee.CATEGORY = CategoryEventSerialisationGuarantee()
EventSerialisationGuarantee.STREAM = StreamEventSerialisationGuarantee()


class EventStorageAdapter(ABC):
    @overload
    @abstractmethod
    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: StreamIdentifier,
        events: Sequence[NewEvent[Name, Payload]],
        condition: WriteCondition = NoCondition(),
    ) -> Sequence[StoredEvent[Name, Payload]]:
        raise NotImplementedError()

    @overload
    @abstractmethod
    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: CategoryIdentifier,
        streams: Mapping[str, StreamPublishDefinition[Name, Payload]],
    ) -> Mapping[str, Sequence[StoredEvent[Name, Payload]]]:
        raise NotImplementedError()

    @abstractmethod
    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent[Name, Payload]] | None = None,
        condition: WriteCondition = NoCondition(),
        streams: Mapping[str, StreamPublishDefinition[Name, Payload]]
        | None = None,
    ) -> (
        Sequence[StoredEvent[Name, Payload]]
        | Mapping[str, Sequence[StoredEvent[Name, Payload]]]
    ):
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
