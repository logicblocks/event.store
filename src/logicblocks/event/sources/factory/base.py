from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Self

from logicblocks.event.sources import EventSource
from logicblocks.event.types import (
    BaseEvent,
    EventSourceIdentifier,
)


class EventSourceFactory[A = Any, E: BaseEvent = BaseEvent](ABC):
    @abstractmethod
    def register_constructor[I: EventSourceIdentifier](
            self,
            identifier_type: type[I],
            constructor: Callable[[I, A], EventSource[I, E]],
    ) -> Self:
        raise NotImplementedError
    
    @abstractmethod
    def construct[I: EventSourceIdentifier](
            self, identifier: I
    ) -> EventSource[I, E]:
        raise NotImplementedError
