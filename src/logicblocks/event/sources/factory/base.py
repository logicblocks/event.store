from abc import ABC, abstractmethod

from logicblocks.event.sources import EventSource
from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
)


class EventSourceFactory[E: Event](ABC):
    """Abstract factory for constructing event sources from identifiers."""

    @abstractmethod
    def construct[I: EventSourceIdentifier](
        self, identifier: I
    ) -> EventSource[I, E]:
        """Construct an event source for an identifier.

        Args:
            identifier: The identifier of the event source to construct.

        Returns:
            an event source for the identifier.
        """
        raise NotImplementedError
