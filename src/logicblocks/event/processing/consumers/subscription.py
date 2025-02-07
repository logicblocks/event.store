from collections.abc import Callable, MutableMapping, Sequence

from structlog.types import FilteringBoundLogger

from logicblocks.event.store import EventSource
from logicblocks.event.types import (
    EventSequenceIdentifier,
    EventSourceIdentifier,
)

from ..broker import EventSubscriber, EventSubscriberHealth
from .logger import default_logger
from .types import EventConsumer


class EventSubscriptionConsumer(EventConsumer, EventSubscriber):
    def __init__(
        self,
        group: str,
        id: str,
        sequences: Sequence[EventSequenceIdentifier],
        delegate_factory: Callable[[EventSource], EventConsumer],
        logger: FilteringBoundLogger = default_logger,
    ):
        self._group = group
        self._id = id
        self._sequences = sequences
        self._delegate_factory = delegate_factory
        self._logger = logger.bind(subscriber={"group": group, "id": id})
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
        await self._logger.ainfo(
            "event.consumer.subscription.accepting-source",
            source=source.identifier.dict(),
        )
        self._delegates[source.identifier] = self._delegate_factory(source)

    async def withdraw(self, source: EventSource) -> None:
        await self._logger.ainfo(
            "event.consumer.subscription.withdrawing-source",
            source=source.identifier.dict(),
        )
        self._delegates.pop(source.identifier)

    async def consume_all(self) -> None:
        await self._logger.ainfo(
            "event.consumer.subscription.starting-consume",
            sources=[
                identifier.dict() for identifier in self._delegates.keys()
            ],
        )

        for identifier, delegate in self._delegates.items():
            await self._logger.ainfo(
                "event.consumer.subscription.consuming-source",
                source=identifier.dict(),
            )
            await delegate.consume_all()

        await self._logger.ainfo(
            "event.consumer.subscription.completed-consume",
            sources=[
                identifier.dict() for identifier in self._delegates.keys()
            ],
        )
