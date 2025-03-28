from collections.abc import Callable
from dataclasses import dataclass

from .conversion import (
    JsonPersistable,
    default_deserialisation_fallback,
    default_serialisation_fallback,
    deserialise_from_json_value,
    serialise_to_json_value,
)
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

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        state = serialise_to_json_value(self.state, fallback)
        metadata = serialise_to_json_value(self.metadata, fallback)
        source = self.source.serialise(fallback)

        return {
            "id": self.id,
            "name": self.name,
            "source": source,
            "state": state,
            "metadata": metadata,
        }

    def summarise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {
            "id": self.id,
            "name": self.name,
            "source": self.source.serialise(fallback),
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
    projection: Projection[JsonPersistable, JsonPersistable],
    fallback: Callable[[object], JsonValue] = default_serialisation_fallback,
) -> Projection[JsonValue, JsonValue]:
    return Projection[JsonValue, JsonValue](
        id=projection.id,
        name=projection.name,
        state=serialise_to_json_value(projection.state, fallback),
        source=projection.source,
        metadata=serialise_to_json_value(projection.metadata, fallback),
    )


# needed to prevent type narrowing
def _state_deserialisation_fallback[S](klass: type[S], value: object) -> S:
    return default_deserialisation_fallback(klass, value)


# needed to prevent type narrowing
def _metadata_deserialisation_fallback[M](klass: type[M], value: object) -> M:
    return default_deserialisation_fallback(klass, value)


def deserialise_projection[
    State: JsonPersistable = JsonValue,
    Metadata: JsonPersistable = JsonValue,
](
    projection: Projection[JsonValue, JsonValue],
    state_type: type[State] = JsonValueType,
    metadata_type: type[Metadata] = JsonValueType,
    state_fallback: Callable[
        [type[State], object], State
    ] = _state_deserialisation_fallback,
    metadata_fallback: Callable[
        [type[Metadata], object], Metadata
    ] = _metadata_deserialisation_fallback,
) -> Projection[State, Metadata]:
    return Projection[State, Metadata](
        id=projection.id,
        name=projection.name,
        state=deserialise_from_json_value(
            state_type, projection.state, state_fallback
        ),
        source=projection.source,
        metadata=deserialise_from_json_value(
            metadata_type, projection.metadata, metadata_fallback
        ),
    )
