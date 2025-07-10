from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Self

from logicblocks.event.sources import EventSource
from logicblocks.event.types import (
    BaseEvent,
    EventSourceIdentifier,
)


class EventSourceFactory[ConstructorArg = Any](ABC):
    @abstractmethod
    def register_constructor[I: EventSourceIdentifier](
        self,
        identifier_type: type[I],
        constructor: Callable[[I, ConstructorArg], EventSource[I, BaseEvent]],
    ) -> Self:
        raise NotImplementedError

    @abstractmethod
    def construct[I: EventSourceIdentifier](
        self, identifier: I
    ) -> EventSource[I, BaseEvent]:
        raise NotImplementedError
