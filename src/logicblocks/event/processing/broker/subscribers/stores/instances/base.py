from abc import ABC, abstractmethod
from collections.abc import Sequence

from logicblocks.event.types import Event

from ....types import EventSubscriber, EventSubscriberKey


class EventSubscriberStore[E: Event](ABC):
    """Holds the subscriber instances registered with a broker on a node."""

    @abstractmethod
    async def add(self, subscriber: EventSubscriber[E]) -> None:
        """Add a subscriber, replacing any existing one with the same key."""
        raise NotImplementedError()

    @abstractmethod
    async def remove(self, subscriber: EventSubscriber[E]) -> None:
        """Remove a subscriber, if present."""
        raise NotImplementedError()

    @abstractmethod
    async def get(self, key: EventSubscriberKey) -> EventSubscriber[E] | None:
        """Return the subscriber with the given key, or `None` if absent."""
        raise NotImplementedError()

    @abstractmethod
    async def list(self) -> Sequence[EventSubscriber[E]]:
        """Return all subscribers in the store."""
        raise NotImplementedError()
