import asyncio
from types import NoneType

from logicblocks.event.types import Event

from ....process import ProcessStatus, determine_multi_process_status
from ....services import (
    ErrorHandler,
    ErrorHandlingServiceMixin,
    RetryErrorHandler,
)
from ...base import EventBroker
from ...types import EventSubscriber
from .coordinator import EventSubscriptionCoordinator
from .observer import EventSubscriptionObserver
from .subscribers import EventSubscriberManager


class DistributedEventBroker[E: Event](
    EventBroker[E], ErrorHandlingServiceMixin[NoneType]
):
    def __init__(
        self,
        event_subscriber_manager: EventSubscriberManager[E],
        event_subscription_coordinator: EventSubscriptionCoordinator,
        event_subscription_observer: EventSubscriptionObserver[E],
        error_handler: ErrorHandler[NoneType] = RetryErrorHandler(),
    ):
        super().__init__(error_handler)
        self._event_subscriber_manager = event_subscriber_manager
        self._event_subscription_coordinator = event_subscription_coordinator
        self._event_subscription_observer = event_subscription_observer

    @property
    def status(self) -> ProcessStatus:
        return determine_multi_process_status(
            self._event_subscription_coordinator.status,
            self._event_subscription_observer.status,
        )

    async def register(self, subscriber: EventSubscriber[E]) -> None:
        await self._event_subscriber_manager.add(subscriber)

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
