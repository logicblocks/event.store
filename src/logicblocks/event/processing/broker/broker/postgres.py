from datetime import timedelta

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from ....db import PostgresConnectionSettings
from ....store import PostgresEventStorageAdapter
from ..locks import PostgresLockManager
from ..nodes import PostgresNodeStateStore
from ..sources import (
    PostgresEventStoreEventSourceFactory,
)
from ..subscribers import (
    PostgresEventSubscriberStateStore,
)
from ..subscriptions import PostgresEventSubscriptionStateStore
from .broker import EventBroker
from .broker_builder import EventBrokerBuilder, PrepareResults


class _PostgresEventBrokerBuilder(
    EventBrokerBuilder[
        (PostgresConnectionSettings, AsyncConnectionPool[AsyncConnection])
    ]
):
    def _prepare(
        self,
        connection_settings: PostgresConnectionSettings,
        connection_pool: AsyncConnectionPool[AsyncConnection],
    ) -> PrepareResults:
        return PrepareResults(
            node_state_store=PostgresNodeStateStore(connection_pool),
            event_subscriber_state_store=PostgresEventSubscriberStateStore(
                node_id=self.node_id, connection_source=connection_pool
            ),
            event_subscription_state_store=PostgresEventSubscriptionStateStore(
                node_id=self.node_id, connection_source=connection_pool
            ),
            lock_manager=PostgresLockManager(
                connection_settings=connection_settings
            ),
            event_source_factory=PostgresEventStoreEventSourceFactory(
                adapter=PostgresEventStorageAdapter(
                    connection_source=connection_pool
                )
            ),
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
    return (
        _PostgresEventBrokerBuilder(node_id)
        .prepare(connection_settings, connection_pool)
        .build(
            node_manager_heartbeat_interval=node_manager_heartbeat_interval,
            node_manager_purge_interval=node_manager_purge_interval,
            node_manager_node_max_age=node_manager_node_max_age,
            subscriber_manager_heartbeat_interval=subscriber_manager_heartbeat_interval,
            subscriber_manager_purge_interval=subscriber_manager_purge_interval,
            subscriber_manager_subscriber_max_age=subscriber_manager_subscriber_max_age,
            coordinator_subscriber_max_time_since_last_seen=coordinator_subscriber_max_time_since_last_seen,
            coordinator_distribution_interval=coordinator_distribution_interval,
            observer_synchronisation_interval=observer_synchronisation_interval,
        )
    )
