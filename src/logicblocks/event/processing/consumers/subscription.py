from collections.abc import Callable, MutableMapping, Sequence
from uuid import uuid4

from structlog.types import FilteringBoundLogger

from logicblocks.event.store import EventCategory, EventSource
from logicblocks.event.types import (
    EventSourceIdentifier,
    str_serialisation_fallback,
)

from ..broker import EventSubscriber, EventSubscriberHealth
from .logger import default_logger
from .source import EventSourceConsumer
from .state import EventConsumerStateStore, EventCount
from .types import EventConsumer, EventProcessor


def make_subscriber(
    *,
    subscriber_group: str,
    subscriber_id: str | None = None,
    subscription_request: EventSourceIdentifier,
    subscriber_state_category: EventCategory,
    subscriber_state_persistence_interval: EventCount = EventCount(100),
    event_processor: EventProcessor,
    logger: FilteringBoundLogger = default_logger,
) -> "EventSubscriptionConsumer":
    subscriber_id = (
        subscriber_id if subscriber_id is not None else str(uuid4())
    )
    state_store = EventConsumerStateStore(
        category=subscriber_state_category,
        persistence_interval=subscriber_state_persistence_interval,
    )

    def delegate_factory[S: EventSource[EventSourceIdentifier]](
        source: S,
    ) -> EventSourceConsumer[S]:
        return EventSourceConsumer(
            source=source,
            processor=event_processor,
            state_store=state_store,
            logger=logger,
        )

    return EventSubscriptionConsumer(
        group=subscriber_group,
        id=subscriber_id,
        subscription_requests=[subscription_request],
        delegate_factory=delegate_factory,
        logger=logger,
    )


class EventSubscriptionConsumer(EventConsumer, EventSubscriber):
    def __init__(
        self,
        group: str,
        id: str,
        subscription_requests: Sequence[EventSourceIdentifier],
        delegate_factory: Callable[
            [EventSource[EventSourceIdentifier]], EventConsumer
        ],
        logger: FilteringBoundLogger = default_logger,
    ):
        self._group = group
        self._id = id
        self._subscription_requests = subscription_requests
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
    def subscription_requests(self) -> Sequence[EventSourceIdentifier]:
        return self._subscription_requests

    async def accept(self, source: EventSource[EventSourceIdentifier]) -> None:
        await self._logger.ainfo(
            "event.consumer.subscription.accepting-source",
            source=source.identifier.serialise(
                fallback=str_serialisation_fallback
            ),
        )
        self._delegates[source.identifier] = self._delegate_factory(source)

    async def withdraw(
        self, source: EventSource[EventSourceIdentifier]
    ) -> None:
        await self._logger.ainfo(
            "event.consumer.subscription.withdrawing-source",
            source=source.identifier.serialise(
                fallback=str_serialisation_fallback
            ),
        )
        self._delegates.pop(source.identifier)

    async def consume_all(self) -> None:
        await self._logger.adebug(
            "event.consumer.subscription.starting-consume",
            sources=[
                identifier.serialise(fallback=str_serialisation_fallback)
                for identifier in self._delegates.keys()
            ],
        )

        for identifier, delegate in dict(self._delegates).items():
            await self._logger.adebug(
                "event.consumer.subscription.consuming-source",
                source=identifier.serialise(
                    fallback=str_serialisation_fallback
                ),
            )
            await delegate.consume_all()

        await self._logger.adebug(
            "event.consumer.subscription.completed-consume",
            sources=[
                identifier.serialise(fallback=str_serialisation_fallback)
                for identifier in self._delegates.keys()
            ],
        )
