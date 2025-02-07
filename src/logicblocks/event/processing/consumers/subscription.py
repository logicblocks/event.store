from collections.abc import Callable, MutableMapping, Sequence

from logicblocks.event.store import EventSource
from logicblocks.event.types import (
    EventSequenceIdentifier,
    EventSourceIdentifier,
)

from ..broker import EventSubscriber, EventSubscriberHealth
from .types import EventConsumer


class EventSubscriptionConsumer(EventConsumer, EventSubscriber):
    def __init__(
        self,
        group: str,
        id: str,
        sequences: Sequence[EventSequenceIdentifier],
        delegate_factory: Callable[[EventSource], EventConsumer],
    ):
        self._group = group
        self._id = id
        self._sequences = sequences
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

    @property
    def sequences(self) -> Sequence[EventSequenceIdentifier]:
        return self._sequences

    async def accept(self, source: EventSource) -> None:
        self._delegates[source.identifier] = self._delegate_factory(source)

    async def withdraw(self, source: EventSource) -> None:
        self._delegates.pop(source.identifier)

    async def consume_all(self) -> None:
        for delegate in self._delegates.values():
            await delegate.consume_all()
