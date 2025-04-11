from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Self, TypedDict

from ..coordinator import EventSubscriptionCoordinator
from ..locks import LockManager
from ..observer import EventSubscriptionObserver
from ..sources import EventStoreEventSourceFactory
from ..subscribers import (
    EventSubscriberManager,
    EventSubscriberStateStore,
    InMemoryEventSubscriberStore,
)
from ..subscriptions import EventSubscriptionStateStore
from .base import EventBroker
from .coordinator_observer import CoordinatorObserverEventBroker


@dataclass(frozen=True)
class EventBrokerSettings:
    subscriber_manager_heartbeat_interval: timedelta = timedelta(seconds=10)
    subscriber_manager_purge_interval: timedelta = timedelta(minutes=1)
    subscriber_manager_subscriber_max_age: timedelta = timedelta(minutes=10)
    coordinator_subscriber_max_time_since_last_seen: timedelta = timedelta(
        seconds=60
    )
    coordinator_distribution_interval: timedelta = timedelta(seconds=20)
    observer_synchronisation_interval: timedelta = timedelta(seconds=20)


class EventBrokerDependencies(TypedDict):
    lock_manager: LockManager
    event_source_factory: EventStoreEventSourceFactory
    event_subscriber_state_store: EventSubscriberStateStore
    event_subscription_state_store: EventSubscriptionStateStore


class EventBrokerBuilder[**P = ...](ABC):
    node_id: str

    event_subscriber_state_store: EventSubscriberStateStore
    event_subscription_state_store: EventSubscriptionStateStore
    lock_manager: LockManager
    event_source_factory: EventStoreEventSourceFactory

    def __init__(self, node_id: str):
        self.node_id = node_id

    @abstractmethod
    def dependencies(
        self, *args: P.args, **kwargs: P.kwargs
    ) -> EventBrokerDependencies:
        pass

    def prepare(self, *args: P.args, **kwargs: P.kwargs) -> Self:
        prepare = self.dependencies(*args, **kwargs)
        self.lock_manager = prepare["lock_manager"]
        self.event_source_factory = prepare["event_source_factory"]
        self.event_subscriber_state_store = prepare[
            "event_subscriber_state_store"
        ]
        self.event_subscription_state_store = prepare[
            "event_subscription_state_store"
        ]

        return self

    def build(
        self,
        settings: EventBrokerSettings,
    ) -> EventBroker:
        event_subscriber_store = InMemoryEventSubscriberStore()

        event_subscriber_manager = EventSubscriberManager(
            node_id=self.node_id,
            subscriber_store=event_subscriber_store,
            subscriber_state_store=self.event_subscriber_state_store,
            heartbeat_interval=settings.subscriber_manager_heartbeat_interval,
            purge_interval=settings.subscriber_manager_purge_interval,
            subscriber_max_age=settings.subscriber_manager_subscriber_max_age,
        )

        event_subscription_coordinator = EventSubscriptionCoordinator(
            node_id=self.node_id,
            lock_manager=self.lock_manager,
            subscriber_state_store=self.event_subscriber_state_store,
            subscription_state_store=self.event_subscription_state_store,
            subscriber_max_time_since_last_seen=settings.coordinator_subscriber_max_time_since_last_seen,
            distribution_interval=settings.coordinator_distribution_interval,
        )

        event_subscription_observer = EventSubscriptionObserver(
            node_id=self.node_id,
            subscriber_store=event_subscriber_store,
            subscription_state_store=self.event_subscription_state_store,
            event_source_factory=self.event_source_factory,
            synchronisation_interval=settings.observer_synchronisation_interval,
        )

        return CoordinatorObserverEventBroker(
            event_subscriber_manager=event_subscriber_manager,
            event_subscription_coordinator=event_subscription_coordinator,
            event_subscription_observer=event_subscription_observer,
        )
