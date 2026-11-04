from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Mapping, Sequence, Set
from typing import overload

from logicblocks.event.sources import constraints
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
    """The scope within which a storage adapter serialises writes.

    Writes that share a lock name are serialised with respect to each other;
    writes with different lock names may proceed concurrently. Use one of the
    provided instances:

    - `LOG`: all writes to the store are serialised.
    - `CATEGORY`: writes to the same category are serialised.
    - `STREAM`: writes to the same stream are serialised.

    Narrower scopes allow more write concurrency. With `CATEGORY` or `STREAM`,
    events are not necessarily committed in sequence number order across
    categories or streams respectively, so a reader scanning the log may
    observe a later sequence number before an earlier one is committed.
    """

    LOG: "LogEventSerialisationGuarantee"
    CATEGORY: "CategoryEventSerialisationGuarantee"
    STREAM: "StreamEventSerialisationGuarantee"

    @abstractmethod
    def lock_name(self, namespace: str, target: Identifier) -> str:
        """Determine the name of the lock to hold when writing to a target.

        Args:
            namespace: A namespace for the lock name, such as a table name.
            target: The identifier being written to.

        Returns:
            the lock name.
        """
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
    """Abstract base class for event storage backends.

    Implement this class to provide a custom backend for an `EventStore`.
    Implementations must satisfy the following contract:

    - Events within a stream have contiguous, zero-based positions, assigned
      in the order in which they are saved.
    - Every stored event has a sequence number that is unique across the
      whole store and increases with the order in which events are saved.
    - A save is atomic: either all of its events are stored or none are.
    - Write conditions are evaluated against the state of each target
      stream after acquiring the write lock(s) dictated by the adapter's
      serialisation guarantee, so that no conflicting write can occur
      between evaluation and storage.
    - `scan` and `latest` order events by sequence number.
    """

    @overload
    @abstractmethod
    async def save[
        Name: StringPersistable,
        Payload: JsonPersistable,
        Metadata: JsonPersistable,
    ](
        self,
        *,
        target: StreamIdentifier,
        events: Sequence[NewEvent[Name, Payload, Metadata]],
        condition: WriteCondition = NoCondition(),
    ) -> Sequence[StoredEvent[Name, Payload, Metadata]]:
        raise NotImplementedError()

    @overload
    @abstractmethod
    async def save[
        Name: StringPersistable,
        Payload: JsonPersistable,
        Metadata: JsonPersistable,
    ](
        self,
        *,
        target: CategoryIdentifier,
        streams: Mapping[
            str, StreamPublishDefinition[Name, Payload, Metadata]
        ],
    ) -> Mapping[str, Sequence[StoredEvent[Name, Payload, Metadata]]]:
        raise NotImplementedError()

    @abstractmethod
    async def save[
        Name: StringPersistable,
        Payload: JsonPersistable,
        Metadata: JsonPersistable,
    ](
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent[Name, Payload, Metadata]] | None = None,
        condition: WriteCondition = NoCondition(),
        streams: Mapping[str, StreamPublishDefinition[Name, Payload, Metadata]]
        | None = None,
    ) -> (
        Sequence[StoredEvent[Name, Payload, Metadata]]
        | Mapping[str, Sequence[StoredEvent[Name, Payload, Metadata]]]
    ):
        """Save events to a stream, or to multiple streams in a category.

        When `target` is a `StreamIdentifier`, `events` are appended to the
        stream, provided `condition` is met for the stream.

        When `target` is a `CategoryIdentifier`, `streams` maps stream names
        within the category to `StreamPublishDefinition`s. Each stream's
        events are appended provided its own condition (default
        `NoCondition`) is met. All streams are written in a single atomic
        operation: if any condition is unmet, nothing is written.

        Args:
            target: The stream or category to save to.
            events: The events to save, in order. Required for a stream
                target.
            condition: The write condition for a stream target.
            streams: The per-stream definitions. Required for a category
                target.

        Returns:
            for a stream target, the stored events in the order given; for a
            category target, a mapping from stream name to the stored events
            for that stream. Stored events retain the unserialised name,
            payload and metadata of the new events.

        Raises:
            UnmetWriteConditionError: If a write condition is not met, in
                which case no events are written.
            ValueError: If the argument required for the target type is
                missing, or the target type is unsupported.
        """
        raise NotImplementedError()

    @abstractmethod
    async def latest(
        self, *, target: Latestable
    ) -> StoredEvent[str, JsonValue, JsonValue] | None:
        """Get the latest event in the log, a category or a stream.

        Args:
            target: The log, category or stream to read from.

        Returns:
            the event with the highest sequence number in the target, in
            serialised form, or `None` if the target contains no events.
        """
        raise NotImplementedError()

    @abstractmethod
    def scan(
        self,
        *,
        target: Scannable = LogIdentifier(),
        constraints: Set[constraints.QueryConstraint] = frozenset(),
    ) -> AsyncIterator[StoredEvent[str, JsonValue, JsonValue]]:
        """Iterate over the events in the log, a category or a stream.

        Args:
            target: The log, category or stream to scan. Defaults to the
                whole log.
            constraints: Query constraints that each yielded event must
                satisfy. Adapters may raise for constraint types they do
                not support.

        Returns:
            an async iterator over the matching events in serialised form,
            in ascending sequence number order.
        """
        raise NotImplementedError()
