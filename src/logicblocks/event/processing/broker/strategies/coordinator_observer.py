import asyncio

from ..coordinator import EventSubscriptionCoordinator
from ..observer import EventSubscriptionObserver
from ..process import ProcessStatus, determine_multi_process_status
from ..subscribers import EventSubscriberManager
from ..types import EventSubscriber
from .base import EventBroker


class CoordinatorObserverEventBroker(EventBroker):
    def __init__(
        self,
        event_subscriber_manager: EventSubscriberManager,
        event_subscription_coordinator: EventSubscriptionCoordinator,
        event_subscription_observer: EventSubscriptionObserver,
    ):
        self._event_subscriber_manager = event_subscriber_manager
        self._event_subscription_coordinator = event_subscription_coordinator
        self._event_subscription_observer = event_subscription_observer

    @property
    def status(self) -> ProcessStatus:
        return determine_multi_process_status(
            self._event_subscription_coordinator.status,
            self._event_subscription_observer.status,
        )

    async def register(self, subscriber: EventSubscriber) -> None:
        await self._event_subscriber_manager.add(subscriber)

    async def execute(self) -> None:
        try:
            await self._event_subscriber_manager.start()

            await asyncio.gather(
                self._event_subscriber_manager.maintain(),
                self._event_subscription_coordinator.coordinate(),
                self._event_subscription_observer.observe(),
                return_exceptions=True,
            )
        finally:
            await self._event_subscriber_manager.stop()
