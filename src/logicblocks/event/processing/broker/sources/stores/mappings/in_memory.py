from collections.abc import Sequence

from logicblocks.event.types import EventSequenceIdentifier

from .base import (
    EventSubscriptionSourceMapping,
    EventSubscriptionSourceMappingStore,
)


class InMemoryEventSubscriptionSourceMappingStore(
    EventSubscriptionSourceMappingStore
):
    def __init__(self):
        self.event_subscription_sources: dict[
            str, Sequence[EventSequenceIdentifier]
        ] = {}

    async def add(
        self,
        subscriber_group: str,
        event_sources: Sequence[EventSequenceIdentifier],
    ) -> None:
        if subscriber_group in self.event_subscription_sources:
            raise ValueError(
                "Can't add event sources for existing subscription."
            )

        self.event_subscription_sources[subscriber_group] = tuple(
            event_sources
        )

    async def remove(self, subscriber_group: str) -> None:
        if subscriber_group not in self.event_subscription_sources:
            raise ValueError(
                "Can't remove event sources for missing subscriber group."
            )

        self.event_subscription_sources.pop(subscriber_group)

    async def list(self) -> Sequence[EventSubscriptionSourceMapping]:
        return [
            EventSubscriptionSourceMapping(
                subscriber_group=subscriber_group, event_sources=event_sources
            )
            for subscriber_group, event_sources in self.event_subscription_sources.items()
        ]
