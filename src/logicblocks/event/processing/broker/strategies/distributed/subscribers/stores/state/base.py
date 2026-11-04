from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from logicblocks.event.types import EventSourceIdentifier

from ......types import EventSubscriber, EventSubscriberKey
from ....subscriptions import EventSubscriptionKey


@dataclass(frozen=True)
class EventSubscriberState:
    """The shared record of a subscriber, as seen by the coordinator.

    Attributes:
        group: The group the subscriber belongs to.
        id: The ID of the subscriber within its group.
        node_id: The ID of the node on which the subscriber runs.
        subscription_requests: The event sources the subscriber requests.
        last_seen: When the subscriber was last registered or heartbeated.
    """

    group: str
    id: str
    node_id: str
    subscription_requests: Sequence[EventSourceIdentifier]
    last_seen: datetime

    def __post_init__(self):
        object.__setattr__(
            self, "subscription_requests", tuple(self.subscription_requests)
        )

    @property
    def key(self) -> EventSubscriberKey:
        return EventSubscriberKey(self.group, self.id)

    @property
    def subscription_key(self) -> EventSubscriptionKey:
        return EventSubscriptionKey(self.group, self.id)


class EventSubscriberStateStore(ABC):
    """Shared store of subscriber state used for distributed coordination."""

    @abstractmethod
    async def add(self, subscriber: EventSubscriber[Any]) -> None:
        """Record a subscriber as present on this node and seen now.

        If the subscriber is already present, it is refreshed instead.
        """
        raise NotImplementedError

    @abstractmethod
    async def remove(self, subscriber: EventSubscriber[Any]) -> None:
        """Remove a subscriber's state, if present."""
        raise NotImplementedError

    @abstractmethod
    async def list(
        self,
        subscriber_group: str | None = None,
        max_time_since_last_seen: timedelta | None = None,
    ) -> Sequence[EventSubscriberState]:
        """List subscriber state, optionally filtered.

        Args:
            subscriber_group: If given, only include subscribers in this
                group.
            max_time_since_last_seen: If given, only include subscribers
                seen more recently than this.

        Returns:
            the matching subscriber states.
        """
        raise NotImplementedError

    @abstractmethod
    async def heartbeat(self, subscriber: EventSubscriber[Any]) -> None:
        """Record an existing subscriber as seen now.

        Subscribers not already in the store are ignored.
        """
        raise NotImplementedError

    @abstractmethod
    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(minutes=5)
    ) -> None:
        """Remove all subscribers not seen within the given time."""
        raise NotImplementedError
