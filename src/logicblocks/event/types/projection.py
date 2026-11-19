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
"""The type of identifier a projection can be projected from."""


@dataclass(frozen=True)
class Projection[
    State = JsonValue,
    Metadata = JsonValue,
]:
    """The result of projecting the events from an event source.

    A projection is identified by its name and id together.

    Type Parameters:
        State: The type of the projection state. Defaults to `JsonValue`.
        Metadata: The type of the projection metadata. Defaults to
            `JsonValue`.

    Attributes:
        id: The projection id, unique among projections with the same name.
        name: The projection name, typically shared by all projections
            produced by the same projector.
        source: The identifier of the event source projected from.
        state: The projection state.
        metadata: The projection metadata.
    """

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
        """Serialise the projection to a JSON value.

        Args:
            fallback: Called to serialise values that are neither JSON
                values nor `JsonValueSerialisable`. Defaults to raising a
                `ValueError`.

        Returns:
            a JSON object containing every field of the projection.
        """
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
        """Summarise the projection, omitting its state and metadata.

        Args:
            fallback: Called to serialise any value in the source that
                cannot otherwise be serialised.

        Returns:
            a JSON object containing the projection id, name and source.
        """
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
    """Serialise a projection's state and metadata to JSON values.

    Args:
        projection: The projection to serialise.
        fallback: Called to serialise values that are neither JSON values
            nor `JsonValueSerialisable`. Defaults to raising a `ValueError`.

    Returns:
        a copy of the projection with JSON value state and metadata.
    """
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
    """Deserialise a projection's state and metadata to the given types.

    Uses `deserialise_from_json_value` for both state and metadata.

    Args:
        projection: The projection with JSON value state and metadata.
        state_type: The type to deserialise the state into. Defaults to a
            JSON value.
        metadata_type: The type to deserialise the metadata into. Defaults
            to a JSON value.
        state_fallback: Called when the state cannot otherwise be
            deserialised. Defaults to raising a `ValueError`.
        metadata_fallback: Called when the metadata cannot otherwise be
            deserialised. Defaults to raising a `ValueError`.

    Returns:
        a copy of the projection with deserialised state and metadata.
    """
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
