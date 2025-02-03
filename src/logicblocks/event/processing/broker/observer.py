from collections.abc import Sequence

from .difference import EventSubscriptionDifference
from .sources import EventSourceFactory
from .subscribers import EventSubscriberStore
from .subscriptions import EventSubscriptionState, EventSubscriptionStateStore


class EventSubscriptionObserver:
    _existing_subscriptions: Sequence[EventSubscriptionState]

    def __init__(
        self,
        subscriber_store: EventSubscriberStore,
        subscription_state_store: EventSubscriptionStateStore,
        subscription_difference: EventSubscriptionDifference,
        event_source_factory: EventSourceFactory,
    ):
        self._subscriber_store = subscriber_store
        self._subscription_state_store = subscription_state_store
        self._subscription_difference = subscription_difference
        self._event_source_factory = event_source_factory

        self._existing_subscriptions = []

    async def observe(self):
        pass

    async def synchronise(self):
        existing = self._existing_subscriptions
        updated = await self._subscription_state_store.list()

        changeset = self._subscription_difference.diff(existing, updated)

        for revocation in changeset.revocations:
            subscriber = await self._subscriber_store.get(
                revocation.subscriber_key
            )
            if subscriber is not None:
                event_source = self._event_source_factory.construct(
                    revocation.event_source
                )
                await subscriber.withdraw(event_source)

        for allocation in changeset.allocations:
            subscriber = await self._subscriber_store.get(
                allocation.subscriber_key
            )
            if subscriber is not None:
                event_source = self._event_source_factory.construct(
                    allocation.event_source
                )
                await subscriber.accept(event_source)

        self._existing_subscriptions = updated
