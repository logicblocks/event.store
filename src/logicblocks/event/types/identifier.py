import json
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypedDict, cast


class Identifier(ABC):
    @abstractmethod
    def dict(self) -> Mapping[str, Any]:
        raise NotImplementedError()

    def json(self) -> str:
        return json.dumps(self.dict())


@dataclass(frozen=True)
class LogIdentifier(Identifier):
    def dict(self):
        return {"type": "log"}

    def __repr__(self) -> str:
        return "LogIdentifier()"

    def __hash__(self):
        return hash(self.json())


@dataclass(frozen=True)
class CategoryIdentifier(Identifier):
    category: str

    def dict(self):
        return {"type": "category", "category": self.category}

    def __repr__(self) -> str:
        return f"CategoryIdentifier(category='{self.category}')"

    def __hash__(self):
        return hash(self.json())


@dataclass(frozen=True)
class StreamIdentifier(Identifier):
    category: str
    stream: str

    def dict(self):
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

    def __hash__(self):
        return hash(self.json())


type EventSequenceIdentifier = (
    LogIdentifier | CategoryIdentifier | StreamIdentifier
)


class LogIdentifierDict(TypedDict):
    type: str


class CategoryIdentifierDict(TypedDict):
    type: str
    category: str


class StreamIdentifierDict(TypedDict):
    type: str
    category: str
    stream: str


type EventSequenceIdentifierDict = (
    LogIdentifierDict | CategoryIdentifierDict | StreamIdentifierDict
)


def event_sequence_identifier(
    serialised: EventSequenceIdentifierDict,
) -> EventSequenceIdentifier:
    match serialised["type"]:
        case "log":
            return LogIdentifier()
        case "category":
            resolved = cast(CategoryIdentifierDict, serialised)
            return CategoryIdentifier(category=resolved["category"])
        case "stream":
            resolved = cast(StreamIdentifierDict, serialised)
            return StreamIdentifier(
                category=resolved["category"], stream=resolved["stream"]
            )
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
