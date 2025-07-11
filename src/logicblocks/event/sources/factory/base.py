from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Self, TypeVar

from logicblocks.event.sources import EventSource
from logicblocks.event.types import (
    BaseEvent,
    EventSourceIdentifier,
)

Arg = TypeVar("Arg", bound=Any, default=Any, infer_variance=True)
Event = TypeVar("Event", bound=BaseEvent, default=BaseEvent, covariant=True)
Identifier = TypeVar(
    "Identifier", bound=EventSourceIdentifier, infer_variance=True
)


class EventSourceFactory(ABC):
    @abstractmethod
    def register_constructor(
        self,
        identifier_type: type[Identifier],
        constructor: Callable[
            [Identifier, Arg], EventSource[Identifier, Event]
        ],
    ) -> Self:
        raise NotImplementedError

    @abstractmethod
    def construct(
        self, identifier: Identifier
    ) -> EventSource[Identifier, Event]:
        raise NotImplementedError
