import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, cast

from logicblocks.event.types import (
    EventSourceIdentifier,
)

type Projectable = EventSourceIdentifier


def default_converter(value: object) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        value = cast(Mapping[Any, Any], value)
        if all(isinstance(key, str) for key in value.keys()):
            return value

    return value.__dict__


@dataclass(frozen=True)
class Projection[T = Mapping[str, Any]]:
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

    def dict(
        self, converter: Callable[[T], Mapping[str, Any]] = default_converter
    ) -> Mapping[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "state": converter(self.state),
            "version": self.version,
            "source": self.source.dict(),
        }

    def envelope(self) -> Mapping[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "version": self.version,
            "source": self.source.dict(),
        }

    def json(
        self, converter: Callable[[T], Mapping[str, Any]] = default_converter
    ):
        return json.dumps(self.dict(converter))

    def __repr__(self):
        return (
            f"Projection("
            f"id='{self.id}',"
            f"name='{self.name}',"
            f"state={repr(self.state)},"
            f"version={self.version},"
            f"source={repr(self.source)})"
        )

    def __hash__(self):
        return hash(repr(self))
