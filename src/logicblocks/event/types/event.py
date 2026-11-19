from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable, Protocol

from logicblocks.event.utils.clock import Clock, SystemClock

from . import default_serialisation_fallback
from .conversion import JsonPersistable, serialise_to_json_value
from .json import JsonValue, JsonValueSerialisable


class Event(Protocol):
    """Protocol for events that can be summarised for logging."""

    def summarise(self) -> JsonValue:
        """Summarise the event, omitting its payload and metadata.

        Returns:
            a JSON summary of the event.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class NewEvent[Name = str, Payload = JsonValue, Metadata = JsonValue](
    JsonValueSerialisable
):
    """An event that has not yet been published to the event store.

    Type Parameters:
        Name: The type of the event name. Defaults to `str`.
        Payload: The type of the event payload. Defaults to `JsonValue`.
        Metadata: The type of the event metadata. Defaults to `JsonValue`.

    Args:
        name: The event name.
        payload: The event payload.
        metadata: The event metadata. Required; pass `None` if the event
            has no metadata.
        observed_at: When the event was observed. Defaults to the current
            time from `clock`, in UTC.
        occurred_at: When the event occurred. Defaults to `observed_at`.
        clock: The clock used to default `observed_at`.

    Attributes:
        name: The event name.
        payload: The event payload.
        metadata: The event metadata.
        observed_at: When the event was observed.
        occurred_at: When the event occurred.
    """

    name: Name
    payload: Payload
    metadata: Metadata
    observed_at: datetime
    occurred_at: datetime

    def __init__(
        self,
        *,
        name: Name,
        payload: Payload,
        metadata: Metadata,
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
        object.__setattr__(self, "metadata", metadata)
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "occurred_at", occurred_at)

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        """Serialise the event to a JSON value.

        Timestamps are serialised as ISO 8601 strings.

        Args:
            fallback: Called to serialise values that are neither JSON
                values nor `JsonValueSerialisable`. Defaults to raising a
                `ValueError`.

        Returns:
            a JSON object containing every field of the event.
        """
        return {
            "name": serialise_to_json_value(self.name, fallback),
            "payload": serialise_to_json_value(self.payload, fallback),
            "metadata": serialise_to_json_value(self.metadata, fallback),
            "observed_at": self.observed_at.isoformat(),
            "occurred_at": self.occurred_at.isoformat(),
        }

    def summarise(self):
        """Summarise the event as its name and timestamps only."""
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
            f"metadata={repr(self.metadata)}, "
            f"observed_at={self.observed_at}, "
            f"occurred_at={self.occurred_at})"
        )

    def __hash__(self):
        return hash(repr(self))


@dataclass(frozen=True)
class StoredEvent[Name = str, Payload = JsonValue, Metadata = JsonValue](
    JsonValueSerialisable, Event
):
    """An event that has been published to the event store.

    Type Parameters:
        Name: The type of the event name. Defaults to `str`.
        Payload: The type of the event payload. Defaults to `JsonValue`.
        Metadata: The type of the event metadata. Defaults to `JsonValue`.

    Attributes:
        id: The unique id assigned to the event by the store.
        name: The event name.
        stream: The name of the stream the event belongs to.
        category: The name of the category the event's stream belongs to.
        position: The event's position within its stream, starting at 0.
        sequence_number: A number ordering the event across the whole log.
        payload: The event payload.
        metadata: The event metadata.
        observed_at: When the event was observed.
        occurred_at: When the event occurred.
    """

    id: str
    name: Name
    stream: str
    category: str
    position: int
    sequence_number: int
    payload: Payload
    metadata: Metadata
    observed_at: datetime
    occurred_at: datetime

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        """Serialise the event to a JSON value.

        Timestamps are serialised as ISO 8601 strings.

        Args:
            fallback: Called to serialise values that are neither JSON
                values nor `JsonValueSerialisable`. Defaults to raising a
                `ValueError`.

        Returns:
            a JSON object containing every field of the event.
        """
        return {
            "id": self.id,
            "name": serialise_to_json_value(self.name, fallback),
            "stream": self.stream,
            "category": self.category,
            "position": self.position,
            "sequence_number": self.sequence_number,
            "payload": serialise_to_json_value(self.payload, fallback),
            "metadata": serialise_to_json_value(self.metadata, fallback),
            "observed_at": self.observed_at.isoformat(),
            "occurred_at": self.occurred_at.isoformat(),
        }

    def summarise(self) -> JsonValue:
        """Summarise the event, omitting its payload and metadata.

        Returns:
            a JSON object containing the event's identifying fields and
                timestamps.
        """
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
            f"metadata={repr(self.metadata)}, "
            f"observed_at={self.observed_at}, "
            f"occurred_at={self.occurred_at})"
        )

    def __hash__(self):
        return hash(repr(self))


def serialise_stored_event(
    event: StoredEvent[JsonPersistable, JsonPersistable, JsonPersistable],
    fallback: Callable[[object], JsonValue] = default_serialisation_fallback,
) -> StoredEvent[JsonValue, JsonValue, JsonValue]:
    """Serialise a stored event's name, payload and metadata to JSON values.

    Args:
        event: The event to serialise.
        fallback: Called to serialise values that are neither JSON values
            nor `JsonValueSerialisable`. Defaults to raising a `ValueError`.

    Returns:
        a copy of the event with JSON value name, payload and metadata.
    """
    return StoredEvent[JsonValue, JsonValue, JsonValue](
        id=event.id,
        name=serialise_to_json_value(event.name, fallback),
        stream=event.stream,
        category=event.category,
        position=event.position,
        sequence_number=event.sequence_number,
        payload=serialise_to_json_value(event.payload, fallback),
        metadata=serialise_to_json_value(event.metadata, fallback),
        observed_at=event.observed_at,
        occurred_at=event.occurred_at,
    )
