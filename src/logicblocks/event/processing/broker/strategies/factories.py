from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.store import (
    InMemoryEventStorageAdapter,
    PostgresEventStorageAdapter,
)

from ..locks import InMemoryLockManager, PostgresLockManager
from ..sources import (
    InMemoryEventStoreEventSourceFactory,
    PostgresEventStoreEventSourceFactory,
)
from ..subscribers import (
    InMemoryEventSubscriberStateStore,
    PostgresEventSubscriberStateStore,
)
from ..subscriptions import (
    InMemoryEventSubscriptionStateStore,
    PostgresEventSubscriptionStateStore,
)
from .base import EventBroker
from .builder import (
    EventBrokerBuilder,
    EventBrokerDependencies,
    EventBrokerSettings,
)


class InMemoryEventBrokerBuilder(
    EventBrokerBuilder[(InMemoryEventStorageAdapter,)]
):
    def dependencies(
        self, adapter: InMemoryEventStorageAdapter
    ) -> EventBrokerDependencies:
        return EventBrokerDependencies(
            lock_manager=InMemoryLockManager(),
            event_source_factory=InMemoryEventStoreEventSourceFactory(
                adapter=adapter
            ),
            event_subscriber_state_store=InMemoryEventSubscriberStateStore(
                node_id=self.node_id,
            ),
            event_subscription_state_store=InMemoryEventSubscriptionStateStore(
                node_id=self.node_id
            ),
        )


class PostgresEventBrokerBuilder(
    EventBrokerBuilder[
        (ConnectionSettings, AsyncConnectionPool[AsyncConnection])
    ]
):
    def dependencies(
        self,
        connection_settings: ConnectionSettings,
        connection_pool: AsyncConnectionPool[AsyncConnection],
    ) -> EventBrokerDependencies:
        return EventBrokerDependencies(
            lock_manager=PostgresLockManager(
                connection_settings=connection_settings
            ),
            event_source_factory=PostgresEventStoreEventSourceFactory(
                adapter=PostgresEventStorageAdapter(
                    connection_source=connection_pool
                )
            ),
            event_subscriber_state_store=PostgresEventSubscriberStateStore(
                node_id=self.node_id, connection_source=connection_pool
            ),
            event_subscription_state_store=PostgresEventSubscriptionStateStore(
                node_id=self.node_id, connection_source=connection_pool
            ),
        )


def make_in_memory_event_broker(
    node_id: str,
    settings: EventBrokerSettings,
    adapter: InMemoryEventStorageAdapter,
) -> EventBroker:
    return InMemoryEventBrokerBuilder(node_id).prepare(adapter).build(settings)


def make_postgres_event_broker(
    node_id: str,
    connection_settings: ConnectionSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    settings: EventBrokerSettings,
) -> EventBroker:
    return (
        PostgresEventBrokerBuilder(node_id)
        .prepare(connection_settings, connection_pool)
        .build(settings)
    )
