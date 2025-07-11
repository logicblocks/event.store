from abc import ABC, abstractmethod

from logicblocks.event.sources import EventSource
from logicblocks.event.types import (
    BaseEvent,
    EventSourceIdentifier,
)

class EventSourceFactory[E: BaseEvent](ABC):
    @abstractmethod
    def construct[I: EventSourceIdentifier](
        self, identifier: I
    ) -> EventSource[I, E]:
        raise NotImplementedError
