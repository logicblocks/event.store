from collections.abc import Callable, MutableMapping, Sequence
from typing import Any
from uuid import uuid4

from structlog.types import FilteringBoundLogger

from logicblocks.event.sources import EventSource
from logicblocks.event.store import (
    EventCategory,
)
from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
    StoredEvent,
    str_serialisation_fallback,
)

from ..broker import EventSubscriber, EventSubscriberHealth
from .logger import default_logger
from .source import EventSourceConsumer
from .state import (
    EventConsumerStateConverter,
    EventConsumerStateStore,
    EventCount,
    StoredEventEventConsumerStateConverter,
)
from .types import EventConsumer, EventProcessor


def make_event_store_subscriber[E: StoredEvent](
    *,
    subscriber_group: str,
    subscriber_id: str | None = None,
    subscription_request: EventSourceIdentifier,
    subscriber_state_category: EventCategory,
    subscriber_state_persistence_interval: EventCount = EventCount(100),
    event_processor: EventProcessor[E],
    logger: FilteringBoundLogger = default_logger,
    save_state_after_consumption: bool = True,
) -> "EventSubscriptionConsumer[E]":
    """Create a subscriber that consumes stored events from the event store.

    A convenience wrapper around `make_subscriber` that tracks progress by
    the sequence number of the last processed event.

    Args:
        subscriber_group: The group the subscriber belongs to. Subscribers
            in the same group share the work of consuming the requested
            sources.
        subscriber_id: The unique identifier of the subscriber. Defaults to
            a random UUID.
        subscription_request: The identifier of the event source to
            subscribe to.
        subscriber_state_category: The category in which to store
            subscriber state.
        subscriber_state_persistence_interval: The number of processed
            events after which subscriber state is saved.
        event_processor: The processor to pass events to.
        logger: The logger to use.
        save_state_after_consumption: Whether to save subscriber state after
            all available events have been consumed.

    Returns:
        the subscription consumer.
    """
    return make_subscriber(
        subscriber_group=subscriber_group,
        subscriber_id=subscriber_id,
        subscription_request=subscription_request,
        subscriber_state_category=subscriber_state_category,
        subscriber_state_persistence_interval=subscriber_state_persistence_interval,
        subscriber_state_converter=StoredEventEventConsumerStateConverter(),
        event_processor=event_processor,
        logger=logger,
        save_state_after_consumption=save_state_after_consumption,
    )


def make_subscriber[E: Event](
    *,
    subscriber_group: str,
    subscriber_id: str | None = None,
    subscription_request: EventSourceIdentifier,
    subscriber_state_category: EventCategory,
    subscriber_state_persistence_interval: EventCount = EventCount(100),
    subscriber_state_converter: EventConsumerStateConverter[E],
    event_processor: EventProcessor[E],
    logger: FilteringBoundLogger = default_logger,
    save_state_after_consumption: bool = True,
) -> "EventSubscriptionConsumer[E]":
    """Create a subscriber that consumes events from the sources it is given.

    The returned subscriber creates an `EventSourceConsumer` for each source
    it accepts, all sharing a single `EventConsumerStateStore`.

    Args:
        subscriber_group: The group the subscriber belongs to. Subscribers
            in the same group share the work of consuming the requested
            sources.
        subscriber_id: The unique identifier of the subscriber. Defaults to
            a random UUID.
        subscription_request: The identifier of the event source to
            subscribe to.
        subscriber_state_category: The category in which to store
            subscriber state.
        subscriber_state_persistence_interval: The number of processed
            events after which subscriber state is saved.
        subscriber_state_converter: The converter between events and
            subscriber state.
        event_processor: The processor to pass events to.
        logger: The logger to use.
        save_state_after_consumption: Whether to save subscriber state after
            all available events have been consumed.

    Returns:
        the subscription consumer.
    """
    subscriber_id = (
        subscriber_id if subscriber_id is not None else str(uuid4())
    )
    state_store = EventConsumerStateStore(
        category=subscriber_state_category,
        converter=subscriber_state_converter,
        persistence_interval=subscriber_state_persistence_interval,
    )

    def delegate_factory[I: EventSourceIdentifier](
        source: EventSource[I, E],
    ) -> EventSourceConsumer[I, E]:
        return EventSourceConsumer(
            source=source,
            processor=event_processor,
            state_store=state_store,
            logger=logger,
            save_state_after_consumption=save_state_after_consumption,
        )

    return EventSubscriptionConsumer(
        group=subscriber_group,
        id=subscriber_id,
        subscription_requests=[subscription_request],
        delegate_factory=delegate_factory,
        logger=logger,
    )


class EventSubscriptionConsumer[E: Event](EventConsumer, EventSubscriber[E]):
    """An event subscriber that consumes from the sources it is allocated.

    An event broker allocates sources to the subscriber via `accept` and
    removes them via `withdraw`. Each accepted source is consumed by a
    delegate consumer created by the delegate factory.

    Args:
        group: The group the subscriber belongs to.
        id: The unique identifier of the subscriber.
        subscription_requests: The identifiers of the event sources the
            subscriber requests.
        delegate_factory: Creates the consumer for an accepted source.
        logger: The logger to use.
    """

    def __init__(
        self,
        group: str,
        id: str,
        subscription_requests: Sequence[EventSourceIdentifier],
        delegate_factory: Callable[
            [EventSource[EventSourceIdentifier, Any]], EventConsumer
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
        """Return the health of the subscriber, which is always healthy."""
        return EventSubscriberHealth.HEALTHY

    @property
    def subscription_requests(self) -> Sequence[EventSourceIdentifier]:
        return self._subscription_requests

    async def accept(
        self, source: EventSource[EventSourceIdentifier, E]
    ) -> None:
        """Accept a source, creating a delegate consumer for it.

        If the source has already been accepted, the existing delegate
        consumer is retained.

        Args:
            source: The event source to accept.
        """
        if source.identifier in self._delegates:
            await self._logger.ainfo(
                "event.consumer.subscription.reaccepting-source",
                source=source.identifier.serialise(
                    fallback=str_serialisation_fallback
                ),
            )
        else:
            await self._logger.ainfo(
                "event.consumer.subscription.accepting-source",
                source=source.identifier.serialise(
                    fallback=str_serialisation_fallback
                ),
            )
            self._delegates[source.identifier] = self._delegate_factory(source)

    async def withdraw(
        self, source: EventSource[EventSourceIdentifier, E]
    ) -> None:
        """Withdraw a source, discarding its delegate consumer.

        Withdrawing a source that has not been accepted logs a warning.

        Args:
            source: The event source to withdraw.
        """
        if source.identifier in self._delegates:
            await self._logger.ainfo(
                "event.consumer.subscription.withdrawing-source",
                source=source.identifier.serialise(
                    fallback=str_serialisation_fallback
                ),
            )
            self._delegates.pop(source.identifier)
        else:
            await self._logger.awarning(
                "event.consumer.subscription.missing-source",
                source=source.identifier.serialise(
                    fallback=str_serialisation_fallback
                ),
            )

    async def consume_all(self) -> None:
        """Consume all available events from each accepted source in turn."""
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
