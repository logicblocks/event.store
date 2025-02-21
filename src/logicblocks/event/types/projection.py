import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from .codec import CodecOrMapping, deserialise, serialise
from .identifier import EventSourceIdentifier

type Projectable = EventSourceIdentifier


@dataclass(frozen=True)
class Projection[
    State: CodecOrMapping = Mapping[str, Any],
    Metadata: CodecOrMapping = Mapping[str, Any],
]:
    id: str
    name: str
    source: Projectable
    state: State
    metadata: Metadata

    def __init__(
        self,
        *,
        id: str,
        name: str,
        source: Projectable,
        state: State,
        metadata: Metadata,
    ):
        object.__setattr__(self, "id", id)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "metadata", metadata)

    def dict(self) -> Mapping[str, Any]:
        state = serialise(self.state) if self.state is not None else None
        metadata = (
            serialise(self.metadata) if self.metadata is not None else None
        )
        return {
            "id": self.id,
            "name": self.name,
            "source": self.source.dict(),
            "state": state,
            "metadata": metadata,
        }

    def envelope(self) -> Mapping[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "source": self.source.dict(),
        }

    def json(self):
        return json.dumps(self.dict())

    def __repr__(self):
        return (
            f"Projection("
            f"id='{self.id}',"
            f"name='{self.name}',"
            f"source={repr(self.source)},"
            f"state={repr(self.state)},"
            f"metadata={repr(self.metadata)})"
        )

    def __hash__(self):
        return hash(repr(self))


def serialise_projection(
    projection: Projection[CodecOrMapping, CodecOrMapping],
) -> Projection[Mapping[str, Any], Mapping[str, Any]]:
    return Projection[Mapping[str, Any], Mapping[str, Any]](
        id=projection.id,
        name=projection.name,
        state=serialise(projection.state),
        source=projection.source,
        metadata=serialise(projection.metadata),
    )


def deserialise_projection[
    State: CodecOrMapping = Mapping[str, Any],
    Metadata: CodecOrMapping = Mapping[str, Any],
](
    projection: Projection[Mapping[str, Any], Mapping[str, Any]],
    state_type: type[State] = Mapping[str, Any],
    metadata_type: type[Metadata] = Mapping[str, Any],
) -> Projection[State, Metadata]:
    return Projection[State, Metadata](
        id=projection.id,
        name=projection.name,
        state=deserialise(state_type, projection.state),
        source=projection.source,
        metadata=deserialise(metadata_type, projection.metadata),
    )
