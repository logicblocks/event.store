from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Any, Unpack, TypedDict

from frozendict import frozendict

from .data import (
    random_event_id,
    random_event_name,
    random_event_payload,
    random_event_stream_name,
    random_event_category_name,
)
from logicblocks.event.types.event import NewEvent, StoredEvent
from logicblocks.event.utils import Clock, SystemClock


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
        object.__setattr__(
            self, "payload", frozendict(payload or random_event_payload())
        )
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
    payload: Mapping[str, Any]
    occurred_at: datetime | None
    observed_at: datetime | None


@dataclass(frozen=True)
class StoredEventBuilder(object):
    id: str
    name: str
    stream: str
    category: str
    position: int
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
        object.__setattr__(self, "position", position or 0)
        object.__setattr__(
            self, "payload", frozendict(payload or random_event_payload())
        )
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "observed_at", observed_at)

    def _clone(self, **kwargs: Unpack[StoredEventBuilderParams]):
        return StoredEventBuilder(
            id=kwargs.get("id", self.id),
            name=kwargs.get("name", self.name),
            stream=kwargs.get("stream", self.stream),
            category=kwargs.get("category", self.category),
            position=kwargs.get("position", self.position),
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
            payload=self.payload,
            occurred_at=self.occurred_at,
            observed_at=self.observed_at,
        )