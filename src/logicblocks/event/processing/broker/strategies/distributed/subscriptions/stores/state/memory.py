from collections.abc import Sequence

from logicblocks.event.types import BaseEvent

from .base import (
    EventSubscriptionKey,
    EventSubscriptionState,
    EventSubscriptionStateChange,
    EventSubscriptionStateChangeType,
    EventSubscriptionStateStore,
)


class InMemoryEventSubscriptionStateStore(EventSubscriptionStateStore):
    def __init__(self, node_id: str):
        self.node_id = node_id
        self._subscriptions: dict[
            EventSubscriptionKey[BaseEvent], EventSubscriptionState[BaseEvent]
        ] = {}

    async def list(self) -> Sequence[EventSubscriptionState[BaseEvent]]:
        return list(self._subscriptions.values())

    async def get[E: BaseEvent](
        self, key: EventSubscriptionKey[E]
    ) -> EventSubscriptionState[E] | None:
        return self._subscriptions.get(key, None)

    async def add[E: BaseEvent](
        self, subscription: EventSubscriptionState[E]
    ) -> None:
        existing = await self.get(subscription.key)

        if existing is not None:
            raise ValueError("Can't add existing subscription.")

        self._subscriptions[subscription.key] = EventSubscriptionState(
            group=subscription.group,
            id=subscription.id,
            node_id=self.node_id,
            event_sources=subscription.event_sources,
        )

    async def remove[E: BaseEvent](
        self, subscription: EventSubscriptionState[E]
    ) -> None:
        existing = await self.get(subscription.key)

        if existing is None:
            raise ValueError("Can't remove missing subscription.")

        self._subscriptions.pop(subscription.key)

    async def replace[E: BaseEvent](
        self, subscription: EventSubscriptionState[E]
    ) -> None:
        existing = await self.get(subscription.key)

        if existing is None:
            raise ValueError("Can't replace missing subscription.")

        self._subscriptions[subscription.key] = EventSubscriptionState(
            group=subscription.group,
            id=subscription.id,
            node_id=self.node_id,
            event_sources=subscription.event_sources,
        )

    async def apply(
        self, changes: Sequence[EventSubscriptionStateChange[BaseEvent]]
    ) -> None:
        keys = set(change.subscription.key for change in changes)
        if len(keys) != len(changes):
            raise ValueError(
                "Multiple changes present for same subscription key."
            )

        for change in changes:
            match change.type:
                case EventSubscriptionStateChangeType.ADD:
                    await self.add(change.subscription)
                case EventSubscriptionStateChangeType.REPLACE:
                    await self.replace(change.subscription)
                case EventSubscriptionStateChangeType.REMOVE:
                    await self.remove(change.subscription)
