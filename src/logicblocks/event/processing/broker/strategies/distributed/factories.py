from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.sources import (
    InMemoryEventStoreEventSourceFactory,
    PostgresEventStoreEventSourceFactory,
)
from logicblocks.event.store import (
    InMemoryEventStorageAdapter,
    PostgresEventStorageAdapter,
)

from ....locks import InMemoryLockManager, PostgresLockManager
from ...base import EventBroker
from .builder import (
    DistributedEventBrokerBuilder,
    DistributedEventBrokerDependencies,
    DistributedEventBrokerSettings,
)
from .subscribers import (
    InMemoryEventSubscriberStateStore,
    PostgresEventSubscriberStateStore,
)
from .subscriptions import (
    InMemoryEventSubscriptionStateStore,
    PostgresEventSubscriptionStateStore,
)


class InMemoryDistributedEventBrokerBuilder(
    DistributedEventBrokerBuilder[(InMemoryEventStorageAdapter,)]
):
    def dependencies(
        self, adapter: InMemoryEventStorageAdapter
    ) -> DistributedEventBrokerDependencies:
        return DistributedEventBrokerDependencies(
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


class PostgresDistributedEventBrokerBuilder(
    DistributedEventBrokerBuilder[
        (ConnectionSettings, AsyncConnectionPool[AsyncConnection])
    ]
):
    def dependencies(
        self,
        connection_settings: ConnectionSettings,
        connection_pool: AsyncConnectionPool[AsyncConnection],
    ) -> DistributedEventBrokerDependencies:
        return DistributedEventBrokerDependencies(
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


def make_in_memory_distributed_event_broker(
    node_id: str,
    settings: DistributedEventBrokerSettings,
    adapter: InMemoryEventStorageAdapter,
) -> EventBroker:
    return (
        InMemoryDistributedEventBrokerBuilder(node_id)
        .prepare(adapter)
        .build(settings)
    )


def make_postgres_distributed_event_broker(
    node_id: str,
    connection_settings: ConnectionSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    settings: DistributedEventBrokerSettings,
) -> EventBroker:
    return (
        PostgresDistributedEventBrokerBuilder(node_id)
        .prepare(connection_settings, connection_pool)
        .build(settings)
    )
