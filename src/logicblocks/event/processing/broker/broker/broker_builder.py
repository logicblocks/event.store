from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Self, TypedDict

from ..coordinator import EventSubscriptionCoordinator
from ..locks import LockManager
from ..nodes import NodeManager, NodeStateStore
from ..observer import EventSubscriptionObserver
from ..sources import (
    EventStoreEventSourceFactory,
    InMemoryEventSubscriptionSourceMappingStore,
)
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
    node_manager_heartbeat_interval: timedelta = timedelta(seconds=10)
    node_manager_purge_interval: timedelta = timedelta(minutes=1)
    node_manager_node_max_age: timedelta = timedelta(minutes=10)
    subscriber_manager_heartbeat_interval: timedelta = timedelta(seconds=10)
    subscriber_manager_purge_interval: timedelta = timedelta(minutes=1)
    subscriber_manager_subscriber_max_age: timedelta = timedelta(minutes=10)
    coordinator_subscriber_max_time_since_last_seen: timedelta = timedelta(
        seconds=60
    )
    coordinator_distribution_interval: timedelta = timedelta(seconds=20)
    observer_synchronisation_interval: timedelta = timedelta(seconds=20)


class EventBrokerDependencies(TypedDict):
    node_state_store: NodeStateStore
    event_subscriber_state_store: EventSubscriberStateStore
    event_subscription_state_store: EventSubscriptionStateStore
    lock_manager: LockManager
    event_source_factory: EventStoreEventSourceFactory


class EventBrokerBuilder[**P = ...](ABC):
    node_id: str

    node_state_store: NodeStateStore
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
        self.node_state_store = prepare["node_state_store"]
        self.event_subscriber_state_store = prepare[
            "event_subscriber_state_store"
        ]
        self.event_subscription_state_store = prepare[
            "event_subscription_state_store"
        ]
        self.lock_manager = prepare["lock_manager"]
        self.event_source_factory = prepare["event_source_factory"]

        return self

    def build(
        self,
        settings: EventBrokerSettings,
    ) -> EventBroker:
        node_manager = NodeManager(
            node_id=self.node_id,
            node_state_store=self.node_state_store,
            heartbeat_interval=settings.node_manager_heartbeat_interval,
            purge_interval=settings.node_manager_purge_interval,
            node_max_age=settings.node_manager_node_max_age,
        )

        event_subscriber_store = InMemoryEventSubscriberStore()
        event_subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        event_subscriber_manager = EventSubscriberManager(
            node_id=self.node_id,
            subscriber_store=event_subscriber_store,
            subscriber_state_store=self.event_subscriber_state_store,
            subscription_source_mapping_store=event_subscription_source_mapping_store,
            heartbeat_interval=settings.subscriber_manager_heartbeat_interval,
            purge_interval=settings.subscriber_manager_purge_interval,
            subscriber_max_age=settings.subscriber_manager_subscriber_max_age,
        )

        event_subscription_coordinator = EventSubscriptionCoordinator(
            node_id=self.node_id,
            lock_manager=self.lock_manager,
            subscriber_state_store=self.event_subscriber_state_store,
            subscription_state_store=self.event_subscription_state_store,
            subscription_source_mapping_store=event_subscription_source_mapping_store,
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
            node_manager=node_manager,
            event_subscriber_manager=event_subscriber_manager,
            event_subscription_coordinator=event_subscription_coordinator,
            event_subscription_observer=event_subscription_observer,
        )
