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
from .base import EventBroker
from .broker_builder import (
    EventBrokerBuilder,
    EventBrokerDependencies,
    EventBrokerSettings,
)


class PostgresEventBrokerBuilder(
    EventBrokerBuilder[
        (PostgresConnectionSettings, AsyncConnectionPool[AsyncConnection])
    ]
):
    def dependencies(
        self,
        connection_settings: PostgresConnectionSettings,
        connection_pool: AsyncConnectionPool[AsyncConnection],
    ) -> EventBrokerDependencies:
        return EventBrokerDependencies(
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
    settings: EventBrokerSettings,
) -> EventBroker:
    return (
        PostgresEventBrokerBuilder(node_id)
        .prepare(connection_settings, connection_pool)
        .build(settings)
    )
