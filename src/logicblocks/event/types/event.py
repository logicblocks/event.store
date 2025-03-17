from dataclasses import dataclass
from datetime import UTC, datetime

from logicblocks.event.utils.clock import Clock, SystemClock

from .conversion import serialise
from .json import JsonValue


@dataclass(frozen=True)
class NewEvent[Payload = JsonValue]:
    name: str
    payload: Payload
    observed_at: datetime
    occurred_at: datetime

    def __init__(
        self,
        *,
        name: str,
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

    def serialise(self) -> JsonValue:
        return {
            "name": self.name,
            "payload": serialise(self.payload),
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
class StoredEvent[Payload = JsonValue]:
    id: str
    name: str
    stream: str
    category: str
    position: int
    sequence_number: int
    payload: Payload
    observed_at: datetime
    occurred_at: datetime

    def serialise(self) -> JsonValue:
        return {
            "id": self.id,
            "name": self.name,
            "stream": self.stream,
            "category": self.category,
            "position": self.position,
            "sequence_number": self.sequence_number,
            "payload": serialise(self.payload),
            "observed_at": self.observed_at.isoformat(),
            "occurred_at": self.occurred_at.isoformat(),
        }

    def summarise(self) -> JsonValue:
        return {
            "id": self.id,
            "name": self.name,
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
            f"name={self.name}, "
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
