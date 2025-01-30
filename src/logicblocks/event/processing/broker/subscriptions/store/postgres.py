from collections.abc import Sequence

from ... import EventSubscriptionKey
from .base import (
    EventSubscriptionChange,
    EventSubscriptionState,
    EventSubscriptionStore,
)


class PostgresEventSubscriptionStore(EventSubscriptionStore):
    async def list(self) -> Sequence[EventSubscriptionState]:
        raise NotImplementedError()

    async def get(
        self, key: EventSubscriptionKey
    ) -> EventSubscriptionState | None:
        raise NotImplementedError()

    async def add(self, subscription: EventSubscriptionState) -> None:
        raise NotImplementedError()

    async def remove(self, subscription: EventSubscriptionState) -> None:
        raise NotImplementedError()

    async def replace(self, subscription: EventSubscriptionState) -> None:
        raise NotImplementedError()

    async def apply(self, changes: Sequence[EventSubscriptionChange]) -> None:
        raise NotImplementedError()
