import asyncio
from abc import abstractmethod
from datetime import timedelta
from types import NoneType

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from ...db import PostgresConnectionSettings
from ...store import PostgresEventStorageAdapter
from ..services import Service
from .coordinator import EventSubscriptionCoordinator
from .locks import PostgresLockManager
from .nodes import NodeManager, PostgresNodeStateStore
from .observer import EventSubscriptionObserver
from .sources import (
    InMemoryEventSubscriptionSourceMappingStore,
    PostgresEventStoreEventSourceFactory,
)
from .subscribers import (
    EventSubscriberManager,
    InMemoryEventSubscriberStore,
    PostgresEventSubscriberStateStore,
)
from .subscriptions import PostgresEventSubscriptionStateStore
from .types import EventSubscriber


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


def make_postgres_event_broker(
    node_id: str,
    connection_settings: PostgresConnectionSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    node_manager_heartbeat_interval: timedelta = timedelta(seconds=10),
    node_manager_purge_interval: timedelta = timedelta(minutes=1),
    node_manager_node_max_age: timedelta = timedelta(minutes=10),
    subscriber_manager_heartbeat_interval: timedelta = timedelta(seconds=10),
    subscriber_manager_purge_interval: timedelta = timedelta(minutes=1),
    subscriber_manager_subscriber_max_age: timedelta = timedelta(minutes=10),
    coordinator_subscriber_max_time_since_last_seen: timedelta = timedelta(
        seconds=60
    ),
    coordinator_distribution_interval: timedelta = timedelta(seconds=20),
    observer_synchronisation_interval: timedelta = timedelta(seconds=20),
) -> EventBroker:
    event_storage_adapter = PostgresEventStorageAdapter(
        connection_source=connection_pool
    )

    node_state_store = PostgresNodeStateStore(connection_pool)
    node_manager = NodeManager(
        node_id=node_id,
        node_state_store=node_state_store,
        heartbeat_interval=node_manager_heartbeat_interval,
        purge_interval=node_manager_purge_interval,
        node_max_age=node_manager_node_max_age,
    )

    event_subscriber_store = InMemoryEventSubscriberStore()
    event_subscriber_state_store = PostgresEventSubscriberStateStore(
        node_id=node_id, connection_source=connection_pool
    )
    event_subscription_state_store = PostgresEventSubscriptionStateStore(
        node_id=node_id, connection_source=connection_pool
    )
    event_subscription_source_mapping_store = (
        InMemoryEventSubscriptionSourceMappingStore()
    )

    event_subscriber_manager = EventSubscriberManager(
        node_id=node_id,
        subscriber_store=event_subscriber_store,
        subscriber_state_store=event_subscriber_state_store,
        subscription_source_mapping_store=event_subscription_source_mapping_store,
        heartbeat_interval=subscriber_manager_heartbeat_interval,
        purge_interval=subscriber_manager_purge_interval,
        subscriber_max_age=subscriber_manager_subscriber_max_age,
    )

    lock_manager = PostgresLockManager(connection_settings=connection_settings)

    event_subscription_coordinator = EventSubscriptionCoordinator(
        node_id=node_id,
        lock_manager=lock_manager,
        subscriber_state_store=event_subscriber_state_store,
        subscription_state_store=event_subscription_state_store,
        subscription_source_mapping_store=event_subscription_source_mapping_store,
        subscriber_max_time_since_last_seen=coordinator_subscriber_max_time_since_last_seen,
        distribution_interval=coordinator_distribution_interval,
    )
    event_source_factory = PostgresEventStoreEventSourceFactory(
        adapter=event_storage_adapter
    )
    event_subscription_observer = EventSubscriptionObserver(
        node_id=node_id,
        subscriber_store=event_subscriber_store,
        subscription_state_store=event_subscription_state_store,
        event_source_factory=event_source_factory,
        synchronisation_interval=observer_synchronisation_interval,
    )

    event_broker = CoordinatorObserverEventBroker(
        node_manager=node_manager,
        event_subscriber_manager=event_subscriber_manager,
        event_subscription_coordinator=event_subscription_coordinator,
        event_subscription_observer=event_subscription_observer,
    )

    return event_broker
