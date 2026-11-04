from abc import ABC, abstractmethod

from logicblocks.event.sources import constraints
from logicblocks.event.types import Event, JsonObject


class EventConsumerStateConverter[E: Event](ABC):
    """Converts between events and the consumer state that tracks them.

    Implementations define what state is recorded when an event is
    processed and how that state is turned into a query constraint that
    resumes consumption after the last processed event.
    """

    @abstractmethod
    def event_to_state(self, event: E) -> JsonObject:
        """Return the consumer state to record for a processed event.

        Args:
            event: The event that has been processed.
        """
        raise NotImplementedError()

    @abstractmethod
    def initial_state(self) -> JsonObject:
        """Return the consumer state to use when no events are processed."""
        raise NotImplementedError()

    @abstractmethod
    def state_to_query_constraint(
        self, state: JsonObject
    ) -> constraints.QueryConstraint | None:
        """Convert consumer state into a constraint for resuming consumption.

        Args:
            state: The consumer state to convert.

        Returns:
            a constraint selecting events after those already processed, or
            `None` if all events should be consumed.
        """
        raise NotImplementedError()
