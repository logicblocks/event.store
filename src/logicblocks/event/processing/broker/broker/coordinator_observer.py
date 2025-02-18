import asyncio

from ..coordinator import (
    EventSubscriptionCoordinator,
)
from ..nodes import NodeManager
from ..observer import (
    EventSubscriptionObserver,
)
from ..subscribers import (
    EventSubscriberManager,
)
from ..types import EventSubscriber
from .base import EventBroker


class CoordinatorObserverEventBroker(EventBroker):
    def __init__(
        self,
        node_manager: NodeManager,
        event_subscriber_manager: EventSubscriberManager,
        event_subscription_coordinator: EventSubscriptionCoordinator,
        event_subscription_observer: EventSubscriptionObserver,
    ):
        self._node_manager = node_manager
        self._event_subscriber_manager = event_subscriber_manager
        self._event_subscription_coordinator = event_subscription_coordinator
        self._event_subscription_observer = event_subscription_observer

    async def register(self, subscriber: EventSubscriber) -> None:
        await self._event_subscriber_manager.add(subscriber)

    async def execute(self) -> None:
        await asyncio.gather(
            self._node_manager.execute(),
            self._event_subscriber_manager.execute(),
            self._event_subscription_coordinator.coordinate(),
            self._event_subscription_observer.observe(),
            return_exceptions=True,
        )
