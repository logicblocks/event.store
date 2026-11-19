import asyncio
from types import NoneType

from logicblocks.event.types import Event

from ....process import ProcessStatus, determine_multi_process_status
from ....services import (
    CallableService,
    ErrorHandler,
    ErrorHandlingService,
    RetryErrorHandler,
)
from ...base import EventBroker
from ...types import EventSubscriber
from .coordinator import EventSubscriptionCoordinator
from .observer import EventSubscriptionObserver
from .subscribers import EventSubscriberManager


class DistributedEventBroker[E: Event](EventBroker[E]):
    """An event broker that coordinates event source allocation across nodes.

    While running, the broker concurrently:

    - maintains the registration of this node's subscribers in shared
      state, sending heartbeats for healthy subscribers and purging stale
      ones, via the subscriber manager;
    - competes for leadership and, while leader, distributes the event
      sources requested by each subscriber group across the live
      subscribers in that group, via the subscription coordinator;
    - applies the resulting allocations to this node's subscribers, calling
      `accept` and `withdraw` as they change, via the subscription observer.

    Subscribers are unregistered from shared state when execution ends.

    Args:
        event_subscriber_manager: Manages this node's subscribers.
        event_subscription_coordinator: Allocates event sources to
            subscribers when this node is leader.
        event_subscription_observer: Applies allocations to this node's
            subscribers.
        error_handler: The handler for errors raised during execution.
            Defaults to a `RetryErrorHandler`.
    """

    def __init__(
        self,
        event_subscriber_manager: EventSubscriberManager[E],
        event_subscription_coordinator: EventSubscriptionCoordinator,
        event_subscription_observer: EventSubscriptionObserver[E],
        error_handler: ErrorHandler[NoneType] = RetryErrorHandler(),
    ):
        self._event_subscriber_manager = event_subscriber_manager
        self._event_subscription_coordinator = event_subscription_coordinator
        self._event_subscription_observer = event_subscription_observer
        self._error_handling_service = ErrorHandlingService(
            service=CallableService(self._do_execute),
            error_handler=error_handler,
        )

    @property
    def status(self) -> ProcessStatus:
        """The combined status of the coordinator and observer."""
        return determine_multi_process_status(
            self._event_subscription_coordinator.status,
            self._event_subscription_observer.status,
        )

    async def register(self, subscriber: EventSubscriber[E]) -> None:
        await self._event_subscriber_manager.add(subscriber)

    async def execute(self) -> None:
        """Run the broker until cancelled.

        Errors are handled by the configured error handler.
        """
        return await self._error_handling_service.execute()

    async def _do_execute(self) -> None:
        subscriber_manager = self._event_subscriber_manager
        coordinator = self._event_subscription_coordinator
        observer = self._event_subscription_observer

        try:
            await subscriber_manager.start()

            async with asyncio.TaskGroup() as tg:
                tg.create_task(subscriber_manager.maintain())
                tg.create_task(coordinator.coordinate())
                tg.create_task(observer.observe())
        finally:
            await subscriber_manager.stop()
