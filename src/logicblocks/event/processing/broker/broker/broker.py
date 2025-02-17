import asyncio
from abc import abstractmethod
from types import NoneType

from logicblocks.event.processing.broker.coordinator import (
    EventSubscriptionCoordinator,
)
from logicblocks.event.processing.broker.nodes import NodeManager
from logicblocks.event.processing.broker.observer import (
    EventSubscriptionObserver,
)
from logicblocks.event.processing.broker.subscribers import (
    EventSubscriberManager,
)
from logicblocks.event.processing.broker.types import EventSubscriber
from logicblocks.event.processing.services import Service


class EventBroker(Service[NoneType]):
    @abstractmethod
    async def register(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError


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
