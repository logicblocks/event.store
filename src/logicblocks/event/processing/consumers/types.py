from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence

from logicblocks.event.types import Event


class EventConsumer(ABC):
    """Base class for consumers that consume events from some source."""

    @abstractmethod
    async def consume_all(self) -> None:
        """Consume all events currently available to this consumer."""
        raise NotImplementedError()


class EventProcessor[E: Event](ABC):
    """Base class for processors that handle events one at a time.

    When used with an `EventSourceConsumer`, each event is acknowledged
    automatically once `process_event` returns successfully.
    """

    @abstractmethod
    async def process_event(self, event: E) -> None:
        """Process a single event.

        Args:
            event: The event to process.
        """
        raise NotImplementedError()


class EventProcessorManager[E: Event](ABC):
    """Allows a processor to control acknowledgement and state persistence.

    Passed to a `ManagedEventIteratorProcessor` so that it can decide when
    events count as processed and when consumer state is saved.
    """

    @abstractmethod
    def acknowledge(self, events: E | Sequence[E]) -> None:
        """Record one or more events as processed.

        Args:
            events: The event, or sequence of events, to acknowledge.
        """
        pass

    @abstractmethod
    async def commit(self, *, force: bool = False) -> None:
        """Persist the consumer state for acknowledged events.

        Args:
            force: If `True`, save the state regardless of the persistence
                interval. Otherwise, save only if the persistence interval
                has been reached.
        """
        pass


type EventIterator[E: Event] = AsyncIterator[E]


class AutoCommitEventIteratorProcessor[E: Event](ABC):
    """Base class for processors that consume an iterator of events.

    Each event is acknowledged, and the consumer state saved if needed, when
    the processor requests the next event from the iterator.
    """

    @abstractmethod
    async def process(self, events: EventIterator[E]) -> None:
        """Process events from the provided iterator.

        Args:
            events: An asynchronous iterator over the events to process.
        """
        raise NotImplementedError()


class ManagedEventIteratorProcessor[E: Event](ABC):
    """Base class for processors that manage acknowledgement themselves.

    Events are not acknowledged automatically; the processor must use the
    provided manager to acknowledge events and commit state.
    """

    @abstractmethod
    async def process(
        self, events: EventIterator[E], manager: EventProcessorManager[E]
    ) -> None:
        """Process events from the provided iterator.

        Args:
            events: An asynchronous iterator over the events to process.
            manager: The manager used to acknowledge events and commit
                consumer state.
        """
        raise NotImplementedError()


type SupportedProcessors[E: Event] = (
    EventProcessor[E]
    | AutoCommitEventIteratorProcessor[E]
    | ManagedEventIteratorProcessor[E]
)
