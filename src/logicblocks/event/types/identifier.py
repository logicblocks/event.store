from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, TypedDict

from . import default_serialisation_fallback
from .json import JsonValue, JsonValueSerialisable


class Identifier(ABC, JsonValueSerialisable):
    """Base class for identifiers, which serialise to JSON objects."""

    @abstractmethod
    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        """Serialise the identifier to a JSON object.

        The object has a `type` key naming the kind of identifier, plus a
        key for each of the identifier's fields.

        Args:
            fallback: Called to serialise any nested value that cannot
                otherwise be serialised.

        Returns:
            the serialised identifier.
        """
        raise NotImplementedError

    def __hash__(self):
        return hash(repr(self))


class PartitionIdentifier(Identifier, ABC):
    """Base class for identifiers of a partition of an event source."""

    pass


class EventSourceIdentifier(Identifier, ABC):
    """Base class for identifiers of a source of events.

    Built-in subclasses are `LogIdentifier`, `CategoryIdentifier` and
    `StreamIdentifier`. Projections record the identifier of the source
    they were projected from.
    """

    pass


@dataclass(frozen=True)
class StreamNamePrefixPartitionIdentifier(PartitionIdentifier):
    """Identifies the partition of streams whose names share a prefix.

    Attributes:
        value: The stream name prefix.
    """

    value: str

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {"type": "stream-name-prefix", "value": self.value}

    def __repr__(self) -> str:
        return f"StreamNamePrefixPartitionIdentifier(value='{self.value}')"


@dataclass(frozen=True)
class LogIdentifier(EventSourceIdentifier):
    """Identifies the whole event log, across all categories and streams.

    Serialises to `{"type": "log"}`.
    """

    __hash__ = Identifier.__hash__

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {"type": "log"}

    def __repr__(self) -> str:
        return "LogIdentifier()"


@dataclass(frozen=True)
class LogPartitionIdentifier(EventSourceIdentifier):
    """Identifies a partition of the whole event log.

    Attributes:
        partition: The partition of the log.
    """

    partition: PartitionIdentifier

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {
            "type": "log-partition",
            "partition": self.partition.serialise(fallback),
        }

    def __repr__(self) -> str:
        return f"LogPartitionIdentifier(partition={self.partition})"


@dataclass(frozen=True)
class CategoryIdentifier(EventSourceIdentifier):
    """Identifies a category of streams.

    Serialises to `{"type": "category", "category": ...}`.

    Attributes:
        category: The category name.
    """

    __hash__ = Identifier.__hash__

    category: str

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {"type": "category", "category": self.category}

    def __repr__(self) -> str:
        return f"CategoryIdentifier(category='{self.category}')"


@dataclass(frozen=True)
class CategoryPartitionIdentifier(EventSourceIdentifier):
    """Identifies a partition of a category.

    Attributes:
        category: The category name.
        partition: The partition of the category.
    """

    category: str
    partition: PartitionIdentifier

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {
            "type": "category-partition",
            "partition": self.partition.serialise(fallback),
        }

    def __repr__(self) -> str:
        return (
            f"CategoryPartitionIdentifier("
            f"category='{self.category}',"
            f"partition={self.partition}"
            f")"
        )


@dataclass(frozen=True)
class StreamIdentifier(EventSourceIdentifier):
    """Identifies a single stream within a category.

    Serialises to `{"type": "stream", "category": ..., "stream": ...}`.

    Attributes:
        category: The name of the category the stream belongs to.
        stream: The stream name.
    """

    __hash__ = Identifier.__hash__

    category: str
    stream: str

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {
            "type": "stream",
            "category": self.category,
            "stream": self.stream,
        }

    def __repr__(self) -> str:
        return (
            f"StreamIdentifier("
            f"category='{self.category}',"
            f"stream='{self.stream}')"
        )


class LogIdentifierDict(TypedDict):
    type: Literal["log"]


class CategoryIdentifierDict(TypedDict):
    type: Literal["category"]
    category: str


class StreamIdentifierDict(TypedDict):
    type: Literal["stream"]
    category: str
    stream: str


type EventSequenceIdentifierDict = (
    LogIdentifierDict | CategoryIdentifierDict | StreamIdentifierDict
)

type EventSequenceIdentifier = (
    LogIdentifier | CategoryIdentifier | StreamIdentifier
)


def event_sequence_identifier(
    serialised: EventSequenceIdentifierDict,
) -> EventSequenceIdentifier:
    """Deserialise a log, category or stream identifier.

    Args:
        serialised: The serialised identifier, as produced by `serialise`.

    Returns:
        the corresponding identifier.

    Raises:
        ValueError: If the value is not a serialised log, category or
            stream identifier.
    """
    match serialised:
        case {"type": "log"}:
            return LogIdentifier()
        case {"type": "category", "category": category}:
            return CategoryIdentifier(category=str(category))
        case {"type": "stream", "category": category, "stream": stream}:
            return StreamIdentifier(category=str(category), stream=str(stream))
        case _:  # pragma: no cover
            raise ValueError("Invalid serialised event sequence identifier.")


def target(
    *, category: str | None = None, stream: str | None = None
) -> EventSequenceIdentifier:
    """Build the identifier for a category and optional stream.

    Args:
        category: The category name.
        stream: The stream name. Requires `category`.

    Returns:
        a `StreamIdentifier` if both are given, a `CategoryIdentifier` if
            only `category` is given, otherwise a `LogIdentifier`.

    Raises:
        ValueError: If `stream` is given without `category`.
    """
    if category is not None and stream is not None:
        return StreamIdentifier(category=category, stream=stream)
    elif category is not None:
        return CategoryIdentifier(category=category)
    elif stream is not None:
        raise ValueError(
            "Invalid target, if stream provided, category must also be provided"
        )
    else:
        return LogIdentifier()
