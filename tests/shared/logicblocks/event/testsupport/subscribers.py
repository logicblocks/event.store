from collections import defaultdict
from collections.abc import Sequence

from logicblocks.event.processing import EventSubscriber, EventSubscriberHealth
from logicblocks.event.sources import EventSource
from logicblocks.event.testing import data
from logicblocks.event.types import (
    CategoryIdentifier,
    Event,
    EventSourceIdentifier,
)


class CapturingEventSubscriber(EventSubscriber):
    sources: list[EventSource]
    counts: dict[str, int]

    def __init__(
        self,
        group: str,
        id: str,
        subscription_requests: Sequence[EventSourceIdentifier] = (),
        health: EventSubscriberHealth = EventSubscriberHealth.HEALTHY,
    ):
        self.sources = []
        self.counts = defaultdict(lambda: 0)
        self._group = group
        self._id = id
        self._subscription_requests = subscription_requests
        self._health = health

    @property
    def group(self) -> str:
        return self._group

    @property
    def id(self) -> str:
        return self._id

    @property
    def subscription_requests(self) -> Sequence[EventSourceIdentifier]:
        return self._subscription_requests

    def health(self) -> EventSubscriberHealth:
        self.counts["health"] += 1
        return self._health

    async def accept(self, source: EventSource) -> None:
        self.counts["accept"] += 1
        self.sources.append(source)

    async def withdraw(self, source: EventSource) -> None:
        self.counts["withdraw"] += 1
        self.sources.remove(source)


class DummyEventSubscriber(EventSubscriber):
    def __init__(
        self,
        group: str,
        id: str,
        subscription_requests: Sequence[EventSourceIdentifier] = (),
    ):
        self._group = group
        self._id = id
        self._subscription_requests = subscription_requests

    @property
    def group(self) -> str:
        return self._group

    @property
    def id(self) -> str:
        return self._id

    @property
    def subscription_requests(self) -> Sequence[EventSourceIdentifier]:
        return self._subscription_requests

    def health(self) -> EventSubscriberHealth:
        return EventSubscriberHealth.HEALTHY

    async def accept(
        self, source: EventSource[EventSourceIdentifier, Event]
    ) -> None:
        pass

    async def withdraw(
        self, source: EventSource[EventSourceIdentifier, Event]
    ) -> None:
        pass


def random_capturing_subscriber(
    subscriber_group: str | None = None,
    subscription_requests: Sequence[EventSourceIdentifier] | None = None,
) -> CapturingEventSubscriber:
    subscriber_id = data.random_subscriber_id()
    if subscriber_group is None:
        subscriber_group = data.random_subscriber_group()
    if subscription_requests is None:
        subscription_requests = [
            CategoryIdentifier(category=data.random_event_category_name()),
        ]

    return CapturingEventSubscriber(
        group=subscriber_group,
        id=subscriber_id,
        subscription_requests=subscription_requests,
    )
