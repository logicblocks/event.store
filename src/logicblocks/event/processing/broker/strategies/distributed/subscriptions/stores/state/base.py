from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum

from logicblocks.event.types import EventSourceIdentifier

from ......types import EventSubscriberKey


@dataclass(frozen=True)
class EventSubscriptionKey:
    """The unique identity of a subscription, matching its subscriber's."""

    group: str
    id: str


@dataclass(frozen=True)
class EventSubscriptionState:
    """The event sources allocated to a subscriber.

    Attributes:
        group: The group of the subscriber.
        id: The ID of the subscriber within its group.
        node_id: The ID of the node recorded against the subscription.
        event_sources: The event sources allocated to the subscriber.
    """

    group: str
    id: str
    node_id: str
    event_sources: Sequence[EventSourceIdentifier]

    @property
    def key(self) -> EventSubscriptionKey:
        return EventSubscriptionKey(group=self.group, id=self.id)

    @property
    def subscriber_key(self) -> EventSubscriberKey:
        return EventSubscriberKey(group=self.group, id=self.id)


class EventSubscriptionStateChangeType(StrEnum):
    """The kind of change to make to a stored subscription."""

    ADD = "add"
    REMOVE = "remove"
    REPLACE = "replace"


@dataclass(frozen=True)
class EventSubscriptionStateChange:
    """A change to apply to the subscription state store."""

    type: EventSubscriptionStateChangeType
    subscription: EventSubscriptionState


class EventSubscriptionStateStore(ABC):
    """Shared store of subscriptions used for distributed coordination."""

    @abstractmethod
    async def list(self) -> Sequence[EventSubscriptionState]:
        """Return all subscriptions."""
        raise NotImplementedError

    @abstractmethod
    async def get(
        self, key: EventSubscriptionKey
    ) -> EventSubscriptionState | None:
        """Return the subscription with the given key, or `None` if absent."""
        raise NotImplementedError

    @abstractmethod
    async def add(self, subscription: EventSubscriptionState) -> None:
        """Add a new subscription.

        Raises:
            ValueError: If a subscription with the same key already exists.
        """
        raise NotImplementedError

    @abstractmethod
    async def remove(self, subscription: EventSubscriptionState) -> None:
        """Remove an existing subscription.

        Raises:
            ValueError: If no subscription with the same key exists.
        """
        raise NotImplementedError

    @abstractmethod
    async def replace(self, subscription: EventSubscriptionState) -> None:
        """Replace the event sources of an existing subscription.

        Raises:
            ValueError: If no subscription with the same key exists.
        """
        raise NotImplementedError

    @abstractmethod
    async def apply(
        self, changes: Sequence[EventSubscriptionStateChange]
    ) -> None:
        """Apply a batch of additions, removals and replacements.

        Args:
            changes: The changes to apply, at most one per subscription key.

        Raises:
            ValueError: If more than one change targets the same key, or if
                any individual change is invalid.
        """
        raise NotImplementedError
