from collections.abc import MutableMapping, Callable
from logicblocks.event.types import EventSequenceIdentifier
from logicblocks.event.store import EventSource

from .types import EventConsumer, EventSubscriber
from .broker import EventBroker


class EventSubscriptionConsumer(EventConsumer, EventSubscriber):
    def __init__(
            self,
            sequence: EventSequenceIdentifier,
            delegate_factory: Callable[[EventSource], EventConsumer],
    ):
        self._sequence = sequence
        self._delegate_factory = delegate_factory
        self._delegates: \
            MutableMapping[EventSequenceIdentifier, EventConsumer] = {}

    async def subscribe(self, broker: EventBroker):
        await broker.register(self)

    async def receive(self, source: EventSource) -> None:
        self._delegates[source.identifier] = self._delegate_factory(source)

    async def revoke(self, source: EventSource) -> None:
        self._delegates.pop(source.identifier)

    async def consume_all(self) -> None:
        for delegate in self._delegates.values():
            await delegate.consume_all()
