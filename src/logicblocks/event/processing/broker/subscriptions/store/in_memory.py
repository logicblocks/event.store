from collections.abc import Sequence

from .base import (
    EventSubscriptionChange,
    EventSubscriptionChangeType,
    EventSubscriptionState,
    EventSubscriptionStore,
)


class InMemoryEventSubscriptionStore(EventSubscriptionStore):
    def __init__(self):
        self._subscriptions: list[EventSubscriptionState] = []

    async def list(self) -> Sequence[EventSubscriptionState]:
        return list(self._subscriptions)

    async def apply(self, changes: Sequence[EventSubscriptionChange]) -> None:
        for remove in [
            change
            for change in changes
            if change.type == EventSubscriptionChangeType.REMOVE
        ]:
            existing = next(
                (
                    subscription
                    for subscription in self._subscriptions
                    if subscription.key == (remove.state.name, remove.state.id)
                ),
                None,
            )

            if existing is None:
                raise ValueError(
                    "Can't remove missing subscription. Add first."
                )

            self._subscriptions.remove(existing)

        for add in [
            change
            for change in changes
            if change.type == EventSubscriptionChangeType.ADD
        ]:
            existing = next(
                (
                    subscription
                    for subscription in self._subscriptions
                    if subscription.key == (add.state.name, add.state.id)
                ),
                None,
            )

            if existing is not None:
                raise ValueError(
                    "Can't add existing subscription. Remove first."
                )

            self._subscriptions.append(add.state)
