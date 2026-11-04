from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence, Set

from logicblocks.event.types import Event, EventSourceIdentifier

from .constraints import QueryConstraint


class EventSource[I: EventSourceIdentifier, E: Event](ABC):
    """Abstract base class for a readable source of events.

    An event source is identified by an `EventSourceIdentifier`, such as a
    log, category or stream identifier, and provides access to the events it
    contains. Event sources are async iterable: iterating an event source
    directly is equivalent to calling `iterate` with no constraints.

    Subclasses must implement `identifier`, `latest` and `iterate`.
    """

    @property
    @abstractmethod
    def identifier(self) -> I:
        """The identifier of this event source."""
        raise NotImplementedError()

    @abstractmethod
    async def latest(self) -> E | None:
        """Get the most recent event in the source.

        Returns:
            the latest event, or `None` if the source contains no events.
        """
        pass

    async def read(
        self,
        *,
        constraints: Set[QueryConstraint] = frozenset(),
    ) -> Sequence[E]:
        """Read all events in the source into a sequence.

        Args:
            constraints: Query constraints that each returned event must
                satisfy. Defaults to no constraints.

        Returns:
            the matching events, in the order produced by `iterate`.
        """
        return [event async for event in self.iterate(constraints=constraints)]

    @abstractmethod
    def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[E]:
        """Iterate over the events in the source.

        Args:
            constraints: Query constraints that each yielded event must
                satisfy. Defaults to no constraints.

        Returns:
            an async iterator over the matching events.
        """
        raise NotImplementedError()

    def __aiter__(self) -> AsyncIterator[E]:
        return self.iterate()
