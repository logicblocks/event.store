from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from logicblocks.event.sources import EventSource
from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
)


@dataclass(frozen=True)
class EventSubscriberKey:
    """The unique identity of a subscriber, formed of its group and ID.

    Attributes:
        group: The group the subscriber belongs to.
        id: The ID of the subscriber within its group.
    """

    group: str
    id: str

    def dict(self) -> Mapping[str, Any]:
        """Return the key as a mapping with `group` and `id` entries."""
        return {"group": self.group, "id": self.id}


class EventSubscriberHealth(StrEnum):
    """The health of an event subscriber, as reported to the broker."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


class EventSubscriber[E: Event](ABC):
    """A consumer of event sources allocated by an event broker.

    Each subscriber declares the event sources it would like to consume via
    `subscription_requests`. The broker then calls `accept` with each event
    source allocated to the subscriber and `withdraw` with each event source
    that is no longer allocated to it.

    Subscribers sharing a group are treated as instances of the same logical
    consumer, so that a distributed broker can spread the requested event
    sources across them.
    """

    @property
    @abstractmethod
    def group(self) -> str:
        """The group to which the subscriber belongs."""
        raise NotImplementedError

    @property
    @abstractmethod
    def id(self) -> str:
        """The ID of the subscriber, unique within its group."""
        raise NotImplementedError

    @property
    def key(self) -> EventSubscriberKey:
        """The key identifying the subscriber, from its group and ID."""
        return EventSubscriberKey(self.group, self.id)

    @property
    @abstractmethod
    def subscription_requests(self) -> Sequence[EventSourceIdentifier]:
        """The identifiers of the event sources the subscriber requests."""
        raise NotImplementedError

    @abstractmethod
    def health(self) -> EventSubscriberHealth:
        """Return the current health of the subscriber.

        A distributed broker only sends heartbeats for healthy subscribers,
        so an unhealthy subscriber eventually stops receiving allocations.

        Returns:
            the health of the subscriber.
        """
        raise NotImplementedError

    @abstractmethod
    async def accept(
        self, source: EventSource[EventSourceIdentifier, E]
    ) -> None:
        """Accept an event source allocated to the subscriber.

        Implementations should tolerate being passed a source they have
        already accepted, as some brokers reallocate sources periodically.

        Args:
            source: The event source allocated to the subscriber.
        """
        raise NotImplementedError

    @abstractmethod
    async def withdraw(
        self, source: EventSource[EventSourceIdentifier, E]
    ) -> None:
        """Withdraw an event source no longer allocated to the subscriber.

        Args:
            source: The event source being revoked from the subscriber.
        """
        raise NotImplementedError
