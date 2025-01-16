from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Self, TypedDict, Unpack

from logicblocks.event.types import (
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
    random_event_name,
    random_event_payload,
    random_event_position,
    random_event_sequence_number,
    random_event_stream_name,
    random_projection_id,
    random_projection_name,
    random_projection_state,
    random_projection_version,
)


class NewEventBuilderParams(TypedDict, total=False):
    name: str
    payload: Mapping[str, Any]
    occurred_at: datetime | None
    observed_at: datetime | None


@dataclass(frozen=True)
class NewEventBuilder:
    name: str
    payload: Mapping[str, Any]
    occurred_at: datetime | None
    observed_at: datetime | None

    def __init__(
        self,
        *,
        name: str | None = None,
        payload: Mapping[str, Any] | None = None,
        occurred_at: datetime | None = None,
        observed_at: datetime | None = None,
    ):
        object.__setattr__(self, "name", name or random_event_name())
        object.__setattr__(self, "payload", payload or random_event_payload())
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "observed_at", observed_at)

    def _clone(self, **kwargs: Unpack[NewEventBuilderParams]):
        return NewEventBuilder(
            name=kwargs.get("name", self.name),
            payload=kwargs.get("payload", self.payload),
            occurred_at=kwargs.get("occurred_at", self.occurred_at),
            observed_at=kwargs.get("observed_at", self.observed_at),
        )

    def with_name(self, name: str):
        return self._clone(name=name)

    def with_payload(self, payload: Mapping[str, Any]):
        return self._clone(payload=payload)

    def with_occurred_at(self, occurred_at: datetime | None):
        return self._clone(occurred_at=occurred_at)

    def with_observed_at(self, observed_at: datetime | None):
        return self._clone(observed_at=observed_at)

    def build(self):
        return NewEvent(
            name=self.name,
            payload=self.payload,
            occurred_at=self.occurred_at,
            observed_at=self.observed_at,
        )


class StoredEventBuilderParams(TypedDict, total=False):
    id: str
    name: str
    stream: str
    category: str
    position: int
    sequence_number: int
    payload: Mapping[str, Any]
    occurred_at: datetime | None
    observed_at: datetime | None


@dataclass(frozen=True)
class StoredEventBuilder:
    id: str
    name: str
    stream: str
    category: str
    position: int
    sequence_number: int
    payload: Mapping[str, Any]
    occurred_at: datetime
    observed_at: datetime

    def __init__(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        stream: str | None = None,
        category: str | None = None,
        position: int | None = None,
        sequence_number: int | None = None,
        payload: Mapping[str, Any] | None = None,
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
        object.__setattr__(self, "payload", payload or random_event_payload())
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "observed_at", observed_at)

    def _clone(self, **kwargs: Unpack[StoredEventBuilderParams]):
        return StoredEventBuilder(
            id=kwargs.get("id", self.id),
            name=kwargs.get("name", self.name),
            stream=kwargs.get("stream", self.stream),
            category=kwargs.get("category", self.category),
            position=kwargs.get("position", self.position),
            sequence_number=kwargs.get(
                "sequence_number", self.sequence_number
            ),
            payload=kwargs.get("payload", self.payload),
            occurred_at=kwargs.get("occurred_at", self.occurred_at),
            observed_at=kwargs.get("observed_at", self.observed_at),
        )

    def from_new_event(self, event: NewEvent):
        return self._clone(
            name=event.name,
            payload=event.payload,
            occurred_at=event.occurred_at,
            observed_at=event.observed_at,
        )

    def with_id(self, id: str):
        return self._clone(id=id)

    def with_name(self, name: str):
        return self._clone(name=name)

    def with_stream(self, stream: str):
        return self._clone(stream=stream)

    def with_category(self, category: str):
        return self._clone(category=category)

    def with_position(self, position: int):
        return self._clone(position=position)

    def with_sequence_number(self, sequence_number: int):
        return self._clone(sequence_number=sequence_number)

    def with_payload(self, payload: Mapping[str, Any]):
        return self._clone(payload=payload)

    def with_occurred_at(self, occurred_at: datetime | None):
        return self._clone(occurred_at=occurred_at)

    def with_observed_at(self, observed_at: datetime | None):
        return self._clone(observed_at=observed_at)

    def build(self):
        return StoredEvent(
            id=self.id,
            name=self.name,
            stream=self.stream,
            category=self.category,
            position=self.position,
            sequence_number=self.sequence_number,
            payload=self.payload,
            occurred_at=self.occurred_at,
            observed_at=self.observed_at,
        )


class ProjectionBuilderParams[T](TypedDict, total=False):
    id: str
    name: str
    state: T
    version: int
    source: Projectable


class BaseProjectionBuilder[T](ABC):
    id: str
    name: str
    state: T
    version: int
    source: Projectable

    def __init__(
        self,
        *,
        id: str | None = None,
        name: str | None = None,
        state: T | None = None,
        version: int | None = None,
        source: Projectable | None = None,
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
            "state",
            state if state is not None else self.default_state_factory(),
        )
        object.__setattr__(
            self,
            "version",
            version if version is not None else random_projection_version(),
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

    @abstractmethod
    def default_state_factory(self) -> T:
        raise NotImplementedError()

    def _clone(self, **kwargs: Unpack[ProjectionBuilderParams[T]]) -> Self:
        return self.__class__(
            id=kwargs.get("id", self.id),
            name=kwargs.get("name", self.name),
            state=kwargs.get("state", self.state),
            version=kwargs.get("version", self.version),
            source=kwargs.get("source", self.source),
        )

    def with_id(self, id: str) -> Self:
        return self._clone(id=id)

    def with_name(self, name: str) -> Self:
        return self._clone(name=name)

    def with_state(self, state: T) -> Self:
        return self._clone(state=state)

    def with_version(self, version: int) -> Self:
        return self._clone(version=version)

    def with_source(self, source: Projectable) -> Self:
        return self._clone(source=source)

    def build(self) -> Projection[T]:
        return Projection[T](
            id=self.id,
            name=self.name,
            state=self.state,
            version=self.version,
            source=self.source,
        )


class MappingProjectionBuilder(BaseProjectionBuilder[Mapping[str, Any]]):
    def default_state_factory(self) -> Mapping[str, Any]:
        return random_projection_state()
