from abc import ABC, abstractmethod

from logicblocks.event.sources import EventSource
from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
)


class EventSourceFactory[E: Event](ABC):
    @abstractmethod
    def construct[I: EventSourceIdentifier](
        self, identifier: I
    ) -> EventSource[I, E]:
        raise NotImplementedError
