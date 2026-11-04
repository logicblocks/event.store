from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Self, TypedDict, Unpack

from logicblocks.event.types import (
    JsonValue,
    NewEvent,
    Projectable,
    Projection,
    StoredEvent,
    StreamIdentifier,
)
from logicblocks.event.utils.clock import Clock, SystemClock

from .data import (
    random_event_category_name,
    random_event_id,
    random_event_metadata,
    random_event_name,
    random_event_payload,
    random_event_position,
    random_event_sequence_number,
    random_event_stream_name,
    random_projection_id,
    random_projection_metadata,
    random_projection_name,
    random_projection_state,
)


class NewEventBuilderParams[
    Name = str,
    Payload = JsonValue,
    Metadata = JsonValue,
](TypedDict, total=False):
    name: Name
    payload: Payload
    metadata: Metadata
    occurred_at: datetime | None
    observed_at: datetime | None


@dataclass(frozen=True)
class NewEventBuilder[Name = str, Payload = JsonValue, Metadata = JsonValue]:
    """Immutable builder of `NewEvent` instances for tests.

    Each `with_*` method returns a new builder with that field replaced.
    Fields that are not provided default to random values, except the
    timestamps, which default to `None` so that `NewEvent` assigns them on
    `build`.

    Args:
        name: The event name. Defaults to a random name.
        payload: The event payload. Defaults to a random mapping.
        metadata: The event metadata. Defaults to a random mapping.
        occurred_at: When the event occurred.
        observed_at: When the event was observed.
    """

    name: Name
    payload: Payload
    metadata: Metadata
    occurred_at: datetime | None
    observed_at: datetime | None

    def __init__(
        self,
        *,
        name: Name | None = None,
        payload: Payload | None = None,
        metadata: Metadata | None = None,
        occurred_at: datetime | None = None,
        observed_at: datetime | None = None,
    ):
        object.__setattr__(self, "name", name or random_event_name())
        object.__setattr__(
            self,
            "payload",
            payload if payload is not None else random_event_payload(),
        )
        object.__setattr__(
            self,
            "metadata",
            metadata if metadata is not None else random_event_metadata(),
        )
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "observed_at", observed_at)

    def _clone(
        self,
        **kwargs: Unpack[NewEventBuilderParams[Name, Payload, Metadata]],
    ):
        name = kwargs.get("name", self.name)
        payload = kwargs.get("payload", self.payload)
        metadata = kwargs.get("metadata", self.metadata)
        occurred_at = kwargs.get("occurred_at", self.occurred_at)
        observed_at = kwargs.get("observed_at", self.observed_at)

        return NewEventBuilder(
            name=name,
            payload=payload,
            metadata=metadata,
            occurred_at=occurred_at,
            observed_at=observed_at,
        )

    def with_name(self, name: Name):
        """Return a copy of the builder with the given name."""
        return self._clone(name=name)

    def with_payload(self, payload: Payload):
        """Return a copy of the builder with the given payload."""
        return self._clone(payload=payload)

    def with_metadata(self, metadata: Metadata):
        """Return a copy of the builder with the given metadata."""
        return self._clone(metadata=metadata)

    def with_occurred_at(self, occurred_at: datetime | None):
        """Return a copy of the builder with the given occurrence time."""
        return self._clone(occurred_at=occurred_at)

    def with_observed_at(self, observed_at: datetime | None):
        """Return a copy of the builder with the given observation time."""
        return self._clone(observed_at=observed_at)

    def build(self):
        """Build a `NewEvent` from the builder's fields."""
        return NewEvent[Name, Payload, Metadata](
            name=self.name,
            payload=self.payload,
            metadata=self.metadata,
            occurred_at=self.occurred_at,
            observed_at=self.observed_at,
        )


class StoredEventBuilderParams[
    Name = str,
    Payload = JsonValue,
    Metadata = JsonValue,
](TypedDict, total=False):
    id: str
    name: Name
    stream: str
    category: str
    position: int
    sequence_number: int
    payload: Payload
    metadata: Metadata
    occurred_at: datetime | None
    observed_at: datetime | None


@dataclass(frozen=True)
class StoredEventBuilder[
    Name = str,
    Payload = JsonValue,
    Metadata = JsonValue,
]:
    """Immutable builder of `StoredEvent` instances for tests.

    Each `with_*` method returns a new builder with that field replaced.
    Fields that are not provided default to random values, except the
    timestamps: `observed_at` defaults to the current time from `clock`, in
    UTC, and `occurred_at` defaults to `observed_at`.

    Args:
        id: The event id. Defaults to a random UUID.
        name: The event name. Defaults to a random name.
        stream: The stream name. Defaults to a random name.
        category: The category name. Defaults to a random name.
        position: The position in the stream. Defaults to a random number.
        sequence_number: The sequence number. Defaults to a random number.
        payload: The event payload. Defaults to a random mapping.
        metadata: The event metadata. Defaults to a random mapping.
        occurred_at: When the event occurred.
        observed_at: When the event was observed.
        clock: The clock used to default `observed_at`.
    """

    id: str
    name: Name
    stream: str
    category: str
    position: int
    sequence_number: int
    payload: Payload
    metadata: Metadata
    occurred_at: datetime
    observed_at: datetime

    def __init__(
        self,
        *,
        id: str | None = None,
        name: Name | None = None,
        stream: str | None = None,
        category: str | None = None,
        position: int | None = None,
        sequence_number: int | None = None,
        payload: Payload | None = None,
        metadata: Metadata | None = None,
        occurred_at: datetime | None = None,
        observed_at: datetime | None = None,
        clock: Clock = SystemClock(),
    ):
        if observed_at is None:
            observed_at = clock.now(UTC)
        if occurred_at is None:
            occurred_at = observed_at

        object.__setattr__(self, "id", id or random_event_id())
        object.__setattr__(self, "name", name or random_event_name())
        object.__setattr__(
            self, "stream", stream or random_event_stream_name()
        )
        object.__setattr__(
            self, "category", category or random_event_category_name()
        )
        object.__setattr__(
            self,
            "position",
            position if position is not None else random_event_position(),
        )
        object.__setattr__(
            self,
            "sequence_number",
            sequence_number
            if sequence_number is not None
            else random_event_sequence_number(),
        )
        object.__setattr__(
            self,
            "payload",
            payload if payload is not None else random_event_payload(),
        )
        object.__setattr__(
            self,
            "metadata",
            metadata if metadata is not None else random_event_metadata(),
        )
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "observed_at", observed_at)

    def _clone(
        self,
        **kwargs: Unpack[StoredEventBuilderParams[Name, Payload, Metadata]],
    ):
        id = kwargs.get("id", self.id)
        name = kwargs.get("name", self.name)
        stream = kwargs.get("stream", self.stream)
        category = kwargs.get("category", self.category)
        position = kwargs.get("position", self.position)
        sequence_number = kwargs.get("sequence_number", self.sequence_number)
        payload = kwargs.get("payload", self.payload)
        metadata = kwargs.get("metadata", self.metadata)
        occurred_at = kwargs.get("occurred_at", self.occurred_at)
        observed_at = kwargs.get("observed_at", self.observed_at)

        return StoredEventBuilder(
            id=id,
            name=name,
            stream=stream,
            category=category,
            position=position,
            sequence_number=sequence_number,
            payload=payload,
            metadata=metadata,
            occurred_at=occurred_at,
            observed_at=observed_at,
        )

    def from_new_event(self, event: NewEvent[Name, Payload, Metadata]):
        """Return a copy of the builder with fields taken from a new event.

        Copies the name, payload, metadata and timestamps, leaving the id,
        stream, category, position and sequence number unchanged.

        Args:
            event: The new event to copy fields from.
        """
        return self._clone(
            name=event.name,
            payload=event.payload,
            metadata=event.metadata,
            occurred_at=event.occurred_at,
            observed_at=event.observed_at,
        )

    def with_id(self, id: str):
        """Return a copy of the builder with the given id."""
        return self._clone(id=id)

    def with_name(self, name: Name):
        """Return a copy of the builder with the given name."""
        return self._clone(name=name)

    def with_stream(self, stream: str):
        """Return a copy of the builder with the given stream name."""
        return self._clone(stream=stream)

    def with_category(self, category: str):
        """Return a copy of the builder with the given category name."""
        return self._clone(category=category)

    def with_position(self, position: int):
        """Return a copy of the builder with the given stream position."""
        return self._clone(position=position)

    def with_sequence_number(self, sequence_number: int):
        """Return a copy of the builder with the given sequence number."""
        return self._clone(sequence_number=sequence_number)

    def with_payload(self, payload: Payload):
        """Return a copy of the builder with the given payload."""
        return self._clone(payload=payload)

    def with_metadata(self, metadata: Metadata):
        """Return a copy of the builder with the given metadata."""
        return self._clone(metadata=metadata)

    def with_occurred_at(self, occurred_at: datetime | None):
        """Return a copy of the builder with the given occurrence time."""
        return self._clone(occurred_at=occurred_at)

    def with_observed_at(self, observed_at: datetime | None):
        """Return a copy of the builder with the given observation time."""
        return self._clone(observed_at=observed_at)

    def build(self):
        """Build a `StoredEvent` from the builder's fields."""
        return StoredEvent[Name, Payload, Metadata](
            id=self.id,
            name=self.name,
            stream=self.stream,
            category=self.category,
            position=self.position,
            sequence_number=self.sequence_number,
            payload=self.payload,
            metadata=self.metadata,
            occurred_at=self.occurred_at,
            observed_at=self.observed_at,
        )


class ProjectionBuilderParams[
    State = JsonValue,
    Metadata = JsonValue,
](TypedDict, total=False):
    id: str
    name: str
    source: Projectable
    state: State
    metadata: Metadata


class BaseProjectionBuilder[
    State = JsonValue,
    Metadata = JsonValue,
](ABC):
    """Base class for immutable builders of `Projection` instances for tests.

    Subclasses implement `default_state_factory` and
    `default_metadata_factory` to supply the state and metadata used when
    none is given. Each `with_*` method returns a new builder, of the same
    class, with that field replaced.

    Type Parameters:
        State: The type of the projection state.
        Metadata: The type of the projection metadata.

    Args:
        id: The projection id. Defaults to a random UUID.
        name: The projection name. Defaults to a random name.
        source: The source identifier. Defaults to a `StreamIdentifier`
            with random category and stream names.
        state: The projection state. Defaults to the result of
            `default_state_factory`.
        metadata: The projection metadata. Defaults to the result of
            `default_metadata_factory`.
    """

    id: str
    name: str
    source: Projectable
    state: State
    metadata: Metadata

    def __init__(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        source: Projectable | None = None,
        state: State | None = None,
        metadata: Metadata | None = None,
    ):
        object.__setattr__(
            self, "id", id if id is not None else random_projection_id()
        )
        object.__setattr__(
            self,
            "name",
            name if name is not None else random_projection_name(),
        )
        object.__setattr__(
            self,
            "source",
            source
            if source is not None
            else StreamIdentifier(
                category=random_event_category_name(),
                stream=random_event_stream_name(),
            ),
        )
        object.__setattr__(
            self,
            "state",
            state if state is not None else self.default_state_factory(),
        )
        object.__setattr__(
            self,
            "metadata",
            metadata
            if metadata is not None
            else self.default_metadata_factory(),
        )

    @abstractmethod
    def default_state_factory(self) -> State:
        """Create the state to use when none is given.

        Returns:
            the default projection state.
        """
        raise NotImplementedError

    @abstractmethod
    def default_metadata_factory(self) -> Metadata:
        """Create the metadata to use when none is given.

        Returns:
            the default projection metadata.
        """
        raise NotImplementedError

    def _clone(
        self, **kwargs: Unpack[ProjectionBuilderParams[State, Metadata]]
    ) -> Self:
        id = kwargs.get("id", self.id)
        name = kwargs.get("name", self.name)
        source = kwargs.get("source", self.source)
        state = kwargs.get("state", self.state)
        metadata = kwargs.get("metadata", self.metadata)

        return self.__class__(
            id=id,
            name=name,
            source=source,
            state=state,
            metadata=metadata,
        )

    def with_id(self, id: str) -> Self:
        """Return a copy of the builder with the given id."""
        return self._clone(id=id)

    def with_name(self, name: str) -> Self:
        """Return a copy of the builder with the given name."""
        return self._clone(name=name)

    def with_source(self, source: Projectable) -> Self:
        """Return a copy of the builder with the given source."""
        return self._clone(source=source)

    def with_state(self, state: State) -> Self:
        """Return a copy of the builder with the given state."""
        return self._clone(state=state)

    def with_metadata(self, metadata: Metadata) -> Self:
        """Return a copy of the builder with the given metadata."""
        return self._clone(metadata=metadata)

    def build(self) -> Projection[State, Metadata]:
        """Build the projection.

        Returns:
            a `Projection` with the builder's fields.
        """
        return Projection[State, Metadata](
            id=self.id,
            name=self.name,
            source=self.source,
            state=self.state,
            metadata=self.metadata,
        )


class MappingProjectionBuilder(
    BaseProjectionBuilder[Mapping[str, Any], Mapping[str, Any]]
):
    """Projection builder whose state and metadata are mappings.

    State and metadata default to random string-to-string mappings.
    """

    def default_state_factory(self) -> Mapping[str, Any]:
        """Create a random string-to-string mapping."""
        return random_projection_state()

    def default_metadata_factory(self) -> Mapping[str, Any]:
        """Create a random string-to-string mapping."""
        return random_projection_metadata()
