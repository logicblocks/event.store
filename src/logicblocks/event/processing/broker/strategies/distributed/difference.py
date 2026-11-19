from collections.abc import Sequence, Set
from dataclasses import dataclass

from logicblocks.event.types import EventSourceIdentifier

from ...types import EventSubscriberKey
from .subscriptions import (
    EventSubscriptionKey,
    EventSubscriptionState,
)


@dataclass(frozen=True)
class EventSubscriptionChange:
    """The allocation or revocation of one event source for a subscriber."""

    group: str
    id: str
    event_source: EventSourceIdentifier

    @property
    def key(self) -> EventSubscriptionKey:
        return EventSubscriptionKey(self.group, self.id)

    @property
    def subscriber_key(self) -> EventSubscriberKey:
        return EventSubscriberKey(self.group, self.id)


@dataclass(frozen=True)
class EventSubscriptionChangeset:
    """The event sources allocated and revoked between two snapshots."""

    allocations: Set[EventSubscriptionChange]
    revocations: Set[EventSubscriptionChange]


class EventSubscriptionDifference:
    """Calculates per event source changes between subscription snapshots."""

    @staticmethod
    def diff(
        existing: Sequence[EventSubscriptionState],
        updated: Sequence[EventSubscriptionState],
    ) -> EventSubscriptionChangeset:
        """Calculate the event sources allocated and revoked.

        Args:
            existing: The previous subscriptions.
            updated: The current subscriptions.

        Returns:
            the changeset, with an allocation for each event source
                present only in `updated` and a revocation for each event
                source present only in `existing`, per subscription.
        """
        allocations: set[EventSubscriptionChange] = set()
        revocations: set[EventSubscriptionChange] = set()

        existing_map = {
            subscription.key: subscription for subscription in existing
        }
        updated_map = {
            subscription.key: subscription for subscription in updated
        }

        new_subscriptions = [
            subscription
            for subscription in updated
            if subscription.key not in existing_map
        ]
        old_subscriptions = [
            subscription
            for subscription in existing
            if subscription.key not in updated_map
        ]
        updated_subscriptions = [
            subscription
            for subscription in updated
            if subscription.key in existing_map
        ]

        for new_subscription in new_subscriptions:
            for event_source in new_subscription.event_sources:
                allocations.add(
                    EventSubscriptionChange(
                        group=new_subscription.group,
                        id=new_subscription.id,
                        event_source=event_source,
                    )
                )

        for old_subscription in old_subscriptions:
            for event_source in old_subscription.event_sources:
                revocations.add(
                    EventSubscriptionChange(
                        group=old_subscription.group,
                        id=old_subscription.id,
                        event_source=event_source,
                    )
                )

        for updated_subscription in updated_subscriptions:
            existing_subscription = existing_map[updated_subscription.key]
            existing_event_sources = set(existing_subscription.event_sources)
            updated_event_sources = set(updated_subscription.event_sources)

            new_event_sources = updated_event_sources - existing_event_sources
            old_event_sources = existing_event_sources - updated_event_sources

            for new_event_source in new_event_sources:
                allocations.add(
                    EventSubscriptionChange(
                        group=updated_subscription.group,
                        id=updated_subscription.id,
                        event_source=new_event_source,
                    )
                )

            for old_event_source in old_event_sources:
                revocations.add(
                    EventSubscriptionChange(
                        group=updated_subscription.group,
                        id=updated_subscription.id,
                        event_source=old_event_source,
                    )
                )

        return EventSubscriptionChangeset(allocations, revocations)
