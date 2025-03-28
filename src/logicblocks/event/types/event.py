from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable

from logicblocks.event.utils.clock import Clock, SystemClock

from . import default_serialisation_fallback
from .conversion import JsonPersistable, serialise_to_json_value
from .json import JsonValue, JsonValueSerialisable


@dataclass(frozen=True)
class NewEvent[Name = str, Payload = JsonValue](JsonValueSerialisable):
    name: Name
    payload: Payload
    observed_at: datetime
    occurred_at: datetime

    def __init__(
        self,
        *,
        name: Name,
        payload: Payload,
        observed_at: datetime | None = None,
        occurred_at: datetime | None = None,
        clock: Clock = SystemClock(),
    ):
        if observed_at is None:
            observed_at = clock.now(UTC)
        if occurred_at is None:
            occurred_at = observed_at

        object.__setattr__(self, "name", name)
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "occurred_at", occurred_at)

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {
            "name": serialise_to_json_value(self.name, fallback),
            "payload": serialise_to_json_value(self.payload, fallback),
            "observed_at": self.observed_at.isoformat(),
            "occurred_at": self.occurred_at.isoformat(),
        }

    def summarise(self):
        return {
            "name": self.name,
            "observed_at": self.observed_at.isoformat(),
            "occurred_at": self.occurred_at.isoformat(),
        }

    def __repr__(self):
        return (
            f"NewEvent("
            f"name={self.name}, "
            f"payload={repr(self.payload)}, "
            f"observed_at={self.observed_at}, "
            f"occurred_at={self.occurred_at})"
        )

    def __hash__(self):
        return hash(repr(self))


@dataclass(frozen=True)
class StoredEvent[Name = str, Payload = JsonValue](JsonValueSerialisable):
    id: str
    name: Name
    stream: str
    category: str
    position: int
    sequence_number: int
    payload: Payload
    observed_at: datetime
    occurred_at: datetime

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {
            "id": self.id,
            "name": serialise_to_json_value(self.name, fallback),
            "stream": self.stream,
            "category": self.category,
            "position": self.position,
            "sequence_number": self.sequence_number,
            "payload": serialise_to_json_value(self.payload, fallback),
            "observed_at": self.observed_at.isoformat(),
            "occurred_at": self.occurred_at.isoformat(),
        }

    def summarise(self) -> JsonValue:
        return {
            "id": self.id,
            "name": serialise_to_json_value(self.name),
            "stream": self.stream,
            "category": self.category,
            "position": self.position,
            "sequence_number": self.sequence_number,
            "observed_at": self.observed_at.isoformat(),
            "occurred_at": self.occurred_at.isoformat(),
        }

    def __repr__(self):
        return (
            f"StoredEvent("
            f"id={self.id}, "
            f"name={repr(self.name)}, "
            f"stream={self.stream}, "
            f"category={self.category}, "
            f"position={self.position}, "
            f"sequence_number={self.sequence_number}, "
            f"payload={repr(self.payload)}, "
            f"observed_at={self.observed_at}, "
            f"occurred_at={self.occurred_at})"
        )

    def __hash__(self):
        return hash(repr(self))


def serialise_stored_event(
    event: StoredEvent[JsonPersistable, JsonPersistable],
    fallback: Callable[[object], JsonValue] = default_serialisation_fallback,
) -> StoredEvent[JsonValue, JsonValue]:
    return StoredEvent[JsonValue, JsonValue](
        id=event.id,
        name=serialise_to_json_value(event.name, fallback),
        stream=event.stream,
        category=event.category,
        position=event.position,
        sequence_number=event.sequence_number,
        payload=serialise_to_json_value(event.payload, fallback),
        observed_at=event.observed_at,
        occurred_at=event.occurred_at,
    )
