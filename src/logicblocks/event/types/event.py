import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from logicblocks.event.utils.clock import Clock, SystemClock


@dataclass(frozen=True)
class NewEvent:
    name: str
    payload: Mapping[str, Any]
    observed_at: datetime
    occurred_at: datetime

    def __init__(
        self,
        *,
        name: str,
        payload: Mapping[str, Any],
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

    def json(self):
        return json.dumps(
            {
                "name": self.name,
                "payload": self.payload,
                "observedAt": self.observed_at.isoformat(),
                "occurredAt": self.occurred_at.isoformat(),
            },
            sort_keys=True,
        )

    def __repr__(self):
        return (
            f"NewEvent("
            f"name={self.name}, "
            f"payload={dict(self.payload)}, "
            f"observed_at={self.observed_at}, "
            f"occurred_at={self.occurred_at})"
        )

    def __hash__(self):
        return hash(self.json())


@dataclass(frozen=True)
class StoredEvent:
    id: str
    name: str
    stream: str
    category: str
    position: int
    sequence_number: int
    payload: Mapping[str, Any]
    observed_at: datetime
    occurred_at: datetime

    def json(self):
        return json.dumps(
            {
                "id": self.id,
                "name": self.name,
                "stream": self.stream,
                "category": self.category,
                "position": self.position,
                "sequence_number": self.sequence_number,
                "payload": self.payload,
                "observedAt": self.observed_at.isoformat(),
                "occurredAt": self.occurred_at.isoformat(),
            },
            sort_keys=True,
        )

    def __repr__(self):
        return (
            f"StoredEvent("
            f"id={self.id}, "
            f"name={self.name}, "
            f"stream={self.stream}, "
            f"category={self.category}, "
            f"position={self.position}, "
            f"sequence_number={self.sequence_number}, "
            f"payload={dict(self.payload)}, "
            f"observed_at={self.observed_at}, "
            f"occurred_at={self.occurred_at})"
        )

    def __hash__(self):
        return hash(self.json())
