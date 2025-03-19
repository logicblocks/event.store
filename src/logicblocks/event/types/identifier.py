from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, TypedDict

from . import default_serialisation_fallback
from .json import JsonValue, JsonValueSerialisable


class Identifier(ABC, JsonValueSerialisable):
    @abstractmethod
    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        raise NotImplementedError

    def __hash__(self):
        return hash(repr(self))


class PartitionIdentifier(Identifier, ABC):
    pass


class EventSourceIdentifier(Identifier, ABC):
    pass


@dataclass(frozen=True)
class StreamNamePrefixPartitionIdentifier(PartitionIdentifier):
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
    match serialised:
        case {"type": "log"}:
            return LogIdentifier()
        case {"type": "category", "category": category}:
            return CategoryIdentifier(category=category)
        case {"type": "stream", "category": category, "stream": stream}:
            return StreamIdentifier(category=category, stream=stream)
        case _:  # pragma: no cover
            raise ValueError("Invalid serialised event sequence identifier.")


def target(
    *, category: str | None = None, stream: str | None = None
) -> EventSequenceIdentifier:
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
