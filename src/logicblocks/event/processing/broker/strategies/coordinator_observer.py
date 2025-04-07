import asyncio

from ..coordinator import (
    EventSubscriptionCoordinator,
    EventSubscriptionCoordinatorStatus,
)
from ..nodes import NodeManager
from ..observer import (
    EventSubscriptionObserver,
    EventSubscriptionObserverStatus,
)
from ..subscribers import (
    EventSubscriberManager,
)
from ..types import EventSubscriber
from .base import EventBroker, EventBrokerStatus


def determine_event_broker_status(
    coordinator_status: EventSubscriptionCoordinatorStatus,
    observer_status: EventSubscriptionObserverStatus,
) -> EventBrokerStatus:
    # if (coordinator_status == EventSubscriptionCoordinatorStatus.ERRORED or
    #         observer_status == EventSubscriptionObserverStatus.ERRORED):
    #     return EventBrokerStatus.ERRORED
    #
    # if (coordinator_status == EventSubscriptionCoordinatorStatus.STARTING and
    #         observer_status == EventSubscriptionObserverStatus.INITIALISED):
    #     return EventBrokerStatus.STARTING
    #
    # if (coordinator_status == EventSubscriptionCoordinatorStatus.STARTING and
    #         observer_status == EventSubscriptionObserverStatus.RUNNING):
    #     return EventBrokerStatus.RUNNING
    #
    # if (coordinator_status == EventSubscriptionCoordinatorStatus.STARTING and
    #         observer_status == EventSubscriptionObserverStatus.STOPPED):
    #     return EventBrokerStatus.STOPPING
    #
    # if (coordinator_status == EventSubscriptionCoordinatorStatus.RUNNING and
    #         observer_status == EventSubscriptionObserverStatus.RUNNING):
    #     return EventBrokerStatus.RUNNING
    #
    # if (coordinator_status == EventSubscriptionCoordinatorStatus.STOPPED and
    #         observer_status == EventSubscriptionObserverStatus.STOPPED):
    #     return EventBrokerStatus.STOPPED
    #
    # if (coordinator_status == EventSubscriptionCoordinatorStatus.STOPPED or
    #         observer_status == EventSubscriptionObserverStatus.STOPPED):
    #     return EventBrokerStatus.STOPPING
    #
    # if (coordinator_status == EventSubscriptionCoordinatorStatus.RUNNING or
    #         observer_status == EventSubscriptionObserverStatus.RUNNING):
    #     return EventBrokerStatus.STARTING
    #
    # return EventBrokerStatus.INITIALISED
    if EventSubscriptionCoordinatorStatus.ERRORED in (
        coordinator_status,
        observer_status,
    ):
        return EventBrokerStatus.ERRORED

    if coordinator_status == EventSubscriptionCoordinatorStatus.INITIALISED:
        if observer_status == EventSubscriptionObserverStatus.INITIALISED:
            return EventBrokerStatus.INITIALISED
        if observer_status == EventSubscriptionObserverStatus.RUNNING:
            return EventBrokerStatus.STARTING
        if observer_status == EventSubscriptionObserverStatus.STOPPED:
            return EventBrokerStatus.STOPPING

    if coordinator_status == EventSubscriptionCoordinatorStatus.STARTING:
        if observer_status == EventSubscriptionObserverStatus.INITIALISED:
            return EventBrokerStatus.STARTING
        if observer_status == EventSubscriptionObserverStatus.RUNNING:
            return EventBrokerStatus.RUNNING
        if observer_status == EventSubscriptionObserverStatus.STOPPED:
            return EventBrokerStatus.STOPPING

    if coordinator_status == EventSubscriptionCoordinatorStatus.RUNNING:
        if observer_status == EventSubscriptionObserverStatus.RUNNING:
            return EventBrokerStatus.RUNNING
        if observer_status == EventSubscriptionObserverStatus.STOPPED:
            return EventBrokerStatus.STOPPING
        return EventBrokerStatus.STARTING

    if coordinator_status == EventSubscriptionCoordinatorStatus.STOPPED:
        if observer_status == EventSubscriptionObserverStatus.STOPPED:
            return EventBrokerStatus.STOPPED
        return EventBrokerStatus.STOPPING

    return EventBrokerStatus.INITIALISED


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

    @property
    def status(self) -> EventBrokerStatus:
        return determine_event_broker_status(
            self._event_subscription_coordinator.status,
            self._event_subscription_observer.status,
        )

    async def register(self, subscriber: EventSubscriber) -> None:
        await self._event_subscriber_manager.add(subscriber)

    async def execute(self) -> None:
        try:
            await self._node_manager.start()
            await self._event_subscriber_manager.start()

            await asyncio.gather(
                self._node_manager.maintain(),
                self._event_subscriber_manager.maintain(),
                self._event_subscription_coordinator.coordinate(),
                self._event_subscription_observer.observe(),
                return_exceptions=True,
            )
        finally:
            await self._event_subscriber_manager.stop()
            await self._node_manager.stop()
