from dataclasses import dataclass

from .conversion import Persistable, deserialise, serialise
from .identifier import EventSourceIdentifier
from .json import JsonValue, JsonValueType

type Projectable = EventSourceIdentifier


@dataclass(frozen=True)
class Projection[
    State = JsonValue,
    Metadata = JsonValue,
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

    def serialise(self) -> JsonValue:
        state = serialise(self.state)
        metadata = serialise(self.metadata)
        source = self.source.serialise()

        return {
            "id": self.id,
            "name": self.name,
            "source": source,
            "state": state,
            "metadata": metadata,
        }

    def summarise(self) -> JsonValue:
        return {
            "id": self.id,
            "name": self.name,
            "source": self.source.serialise(),
        }

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
    projection: Projection[Persistable, Persistable],
) -> Projection[JsonValue, JsonValue]:
    return Projection[JsonValue, JsonValue](
        id=projection.id,
        name=projection.name,
        state=serialise(projection.state),
        source=projection.source,
        metadata=serialise(projection.metadata),
    )


def deserialise_projection[
    State: Persistable = JsonValue,
    Metadata: Persistable = JsonValue,
](
    projection: Projection[JsonValue, JsonValue],
    state_type: type[State] = JsonValueType,
    metadata_type: type[Metadata] = JsonValueType,
) -> Projection[State, Metadata]:
    return Projection[State, Metadata](
        id=projection.id,
        name=projection.name,
        state=deserialise(state_type, projection.state),
        source=projection.source,
        metadata=deserialise(metadata_type, projection.metadata),
    )
