from collections.abc import Callable, MutableMapping

from logicblocks.event.store import EventSource
from logicblocks.event.types import (
    EventSequenceIdentifier,
    EventSourceIdentifier,
)

from ..broker import EventSubscriber, EventSubscriberHealth
from . import EventConsumerStateStore, EventCount, EventSourceConsumer
from .types import EventConsumer, EventProcessor


def make_subscriber(
    *,
    subscriber_group: str,
    subscriber_id: str = uuid4().hex,
    subscriber_sequence: EventSequenceIdentifier,
    subscriber_state_category: EventCategory,
    subscriber_state_persistence_interval: EventCount = EventCount(100),
    event_processor: EventProcessor,
) -> "EventSubscriptionConsumer":
    state_store = EventConsumerStateStore(
        category=subscriber_state_category,
        persistence_interval=subscriber_state_persistence_interval,
    )

    def delegate_factory(source: EventSource) -> EventSourceConsumer:
        return EventSourceConsumer(
            source=source, processor=event_processor, state_store=state_store
        )

    return EventSubscriptionConsumer(
        group=subscriber_group,
        id=subscriber_id,
        sequence=subscriber_sequence,
        delegate_factory=delegate_factory,
    )


class EventSubscriptionConsumer(EventConsumer, EventSubscriber):
    def __init__(
        self,
        group: str,
        id: str,
        sequence: EventSequenceIdentifier,
        delegate_factory: Callable[[EventSource], EventConsumer],
    ):
        self._group = group
        self._id = id
        self._sequence = sequence
        self._delegate_factory = delegate_factory
        self._delegates: MutableMapping[
            EventSourceIdentifier, EventConsumer
        ] = {}

    @property
    def group(self) -> str:
        return self._group

    @property
    def id(self) -> str:
        return self._id

    def health(self) -> EventSubscriberHealth:
        return EventSubscriberHealth.HEALTHY

    async def accept(self, source: EventSource) -> None:
        self._delegates[source.identifier] = self._delegate_factory(source)

    async def withdraw(self, source: EventSource) -> None:
        self._delegates.pop(source.identifier)

    async def consume_all(self) -> None:
        for delegate in self._delegates.values():
            await delegate.consume_all()
