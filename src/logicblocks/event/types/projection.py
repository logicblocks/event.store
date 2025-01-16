import json
from dataclasses import dataclass

from logicblocks.event.types import (
    CategoryIdentifier,
    LogIdentifier,
    StreamIdentifier,
)

type Projectable = LogIdentifier | CategoryIdentifier | StreamIdentifier


@dataclass(frozen=True)
class Projection[T]:
    id: str
    name: str
    state: T
    version: int
    source: Projectable

    def __init__(
        self,
        *,
        id: str,
        name: str,
        state: T,
        version: int,
        source: Projectable,
    ):
        object.__setattr__(self, "id", id)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "version", version)
        object.__setattr__(self, "source", source)

    def json(self):
        return json.dumps(
            {
                "id": self.id,
                "name": self.name,
                "state": self.state,
                "version": self.version,
                "source": self.source.dict(),
            },
            default=lambda o: o.__dict__,
        )

    def __repr__(self):
        return (
            f"Projection("
            f"id='{self.id}',"
            f"name='{self.name}',"
            f"state={self.state},"
            f"version={self.version},"
            f"source={self.source})"
        )

    def __hash__(self):
        return hash(self.json())
