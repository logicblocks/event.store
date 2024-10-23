import json

from collections.abc import Mapping
from typing import Any
from datetime import datetime, UTC
from dataclasses import dataclass

from logicblocks.event.utils import SystemClock, Clock


@dataclass(frozen=True)
class NewEvent(object):
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
class StoredEvent(object):
    id: str
    name: str
    stream: str
    category: str
    position: int
    payload: Mapping[str, Any]
    observed_at: datetime
    occurred_at: datetime

    def __init__(
        self,
        *,
        id: str,
        name: str,
        stream: str,
        category: str,
        position: int,
        payload: Mapping[str, Any],
        observed_at: datetime,
        occurred_at: datetime,
    ):
        object.__setattr__(self, "id", id)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "stream", stream)
        object.__setattr__(self, "category", category)
        object.__setattr__(self, "position", position)
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "occurred_at", occurred_at)

    def json(self):
        return json.dumps(
            {
                "id": self.id,
                "name": self.name,
                "stream": self.stream,
                "category": self.category,
                "position": self.position,
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
            f"payload={dict(self.payload)}, "
            f"observed_at={self.observed_at}, "
            f"occurred_at={self.occurred_at})"
        )

    def __hash__(self):
        return hash(self.json())
