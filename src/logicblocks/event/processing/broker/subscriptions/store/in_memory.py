from collections.abc import Sequence

from .base import (
    EventSubscriptionChange,
    EventSubscriptionChangeType,
    EventSubscriptionKey,
    EventSubscriptionState,
    EventSubscriptionStore,
)


class InMemoryEventSubscriptionStore(EventSubscriptionStore):
    def __init__(self):
        self._subscriptions: dict[
            EventSubscriptionKey, EventSubscriptionState
        ] = {}

    async def list(self) -> Sequence[EventSubscriptionState]:
        return list(self._subscriptions.values())

    async def get(
        self, key: EventSubscriptionKey
    ) -> EventSubscriptionState | None:
        return self._subscriptions.get(key, None)

    async def add(self, subscription: EventSubscriptionState) -> None:
        existing = await self.get(subscription.key)

        if existing is not None:
            raise ValueError("Can't add existing subscription.")

        self._subscriptions[subscription.key] = subscription

    async def remove(self, subscription: EventSubscriptionState) -> None:
        existing = await self.get(subscription.key)

        if existing is None:
            raise ValueError("Can't remove missing subscription.")

        self._subscriptions.pop(subscription.key)

    async def replace(self, subscription: EventSubscriptionState) -> None:
        existing = await self.get(subscription.key)

        if existing is None:
            raise ValueError("Can't replace missing subscription.")

        self._subscriptions[subscription.key] = subscription

    async def apply(self, changes: Sequence[EventSubscriptionChange]) -> None:
        keys = set(change.state.key for change in changes)
        if len(keys) != len(changes):
            raise ValueError(
                "Multiple changes present for same subscription key."
            )

        for change in changes:
            match change.type:
                case EventSubscriptionChangeType.ADD:
                    await self.add(change.state)
                case EventSubscriptionChangeType.REPLACE:
                    await self.replace(change.state)
                case EventSubscriptionChangeType.REMOVE:
                    await self.remove(change.state)
