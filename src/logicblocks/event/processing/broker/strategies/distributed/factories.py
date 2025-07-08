from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.sources import (
    EventSourcePartitioner,
    EventStoreEventSourceFactory,
)
from logicblocks.event.sources.partitioner import NoOpEventSourcePartitioner
from logicblocks.event.store import (
    EventStorageAdapter,
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
    DistributedEventBrokerBuilder[
        (EventStorageAdapter, EventSourcePartitioner | None)
    ]
):
    def dependencies(
        self,
        adapter: EventStorageAdapter,
        partitioner: EventSourcePartitioner | None = None,
    ) -> DistributedEventBrokerDependencies:
        return DistributedEventBrokerDependencies(
            lock_manager=InMemoryLockManager(),
            event_source_factory=EventStoreEventSourceFactory(adapter=adapter),
            event_subscriber_state_store=InMemoryEventSubscriberStateStore(
                node_id=self.node_id,
            ),
            event_subscription_state_store=InMemoryEventSubscriptionStateStore(
                node_id=self.node_id
            ),
            event_source_partitioner=(
                partitioner
                if partitioner is not None
                else NoOpEventSourcePartitioner()
            ),
        )


class PostgresDistributedEventBrokerBuilder(
    DistributedEventBrokerBuilder[
        (
            ConnectionSettings,
            AsyncConnectionPool[AsyncConnection],
            EventStorageAdapter | None,
            EventSourcePartitioner | None,
        )
    ]
):
    def dependencies(
        self,
        connection_settings: ConnectionSettings,
        connection_pool: AsyncConnectionPool[AsyncConnection],
        adapter: EventStorageAdapter | None = None,
        partitioner: EventSourcePartitioner | None = None,
    ) -> DistributedEventBrokerDependencies:
        event_storage_adapter = adapter or PostgresEventStorageAdapter(
            connection_source=connection_pool
        )
        return DistributedEventBrokerDependencies(
            lock_manager=PostgresLockManager(
                connection_settings=connection_settings
            ),
            event_source_factory=EventStoreEventSourceFactory(
                event_storage_adapter
            ),
            event_subscriber_state_store=PostgresEventSubscriberStateStore(
                node_id=self.node_id, connection_source=connection_pool
            ),
            event_subscription_state_store=PostgresEventSubscriptionStateStore(
                node_id=self.node_id, connection_source=connection_pool
            ),
            event_source_partitioner=(
                partitioner
                if partitioner is not None
                else NoOpEventSourcePartitioner()
            ),
        )


def make_in_memory_distributed_event_broker(
    node_id: str,
    settings: DistributedEventBrokerSettings,
    adapter: EventStorageAdapter,
    partitioner: EventSourcePartitioner | None = None,
) -> EventBroker:
    return (
        InMemoryDistributedEventBrokerBuilder(node_id)
        .prepare(adapter, partitioner)
        .build(settings)
    )


def make_postgres_distributed_event_broker(
    node_id: str,
    connection_settings: ConnectionSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    settings: DistributedEventBrokerSettings,
    adapter: EventStorageAdapter | None = None,
    partitioner: EventSourcePartitioner | None = None,
) -> EventBroker:
    return (
        PostgresDistributedEventBrokerBuilder(node_id)
        .prepare(connection_settings, connection_pool, adapter, partitioner)
        .build(settings)
    )
