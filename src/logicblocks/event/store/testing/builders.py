from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Any, Optional, Unpack, TypedDict

from frozendict import frozendict

from .data import random_event_name, random_event_payload
from ..types.event import NewEvent


class NewEventBuilderParams(TypedDict, total=False):
    name: str
    payload: Mapping[str, Any]
    occurred_at: Optional[datetime]
    observed_at: Optional[datetime]


@dataclass(frozen=True)
class NewEventBuilder:
    name: str
    payload: Mapping[str, Any]
    occurred_at: Optional[datetime]
    observed_at: Optional[datetime]

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        payload: Optional[Mapping[str, Any]] = None,
        occurred_at: Optional[datetime] = None,
        observed_at: Optional[datetime] = None,
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

    def with_occurred_at(self, occurred_at: datetime):
        return self._clone(occurred_at=occurred_at)

    def with_observed_at(self, observed_at: datetime):
        return self._clone(observed_at=observed_at)

    def build(self):
        return NewEvent(name=self.name, payload=self.payload)
