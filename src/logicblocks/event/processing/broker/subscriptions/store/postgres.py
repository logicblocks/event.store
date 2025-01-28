from collections.abc import Sequence

from .base import (
    EventSubscriptionChange,
    EventSubscriptionState,
    EventSubscriptionStore,
)


class PostgresEventSubscriptionStore(EventSubscriptionStore):
    async def list(self) -> Sequence[EventSubscriptionState]:
        raise NotImplementedError()

    async def apply(self, changes: Sequence[EventSubscriptionChange]) -> None:
        raise NotImplementedError()
