from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.sources import EventStoreEventSourceFactory
from logicblocks.event.store import (
    EventStorageAdapter,
    PostgresEventStorageAdapter,
)
from logicblocks.event.types import StoredEvent

from ...base import EventBroker
from .builder import (
    SingletonEventBrokerBuilder,
    SingletonEventBrokerDependencies,
    SingletonEventBrokerSettings,
)


class InMemorySingletonEventBrokerBuilder(
    SingletonEventBrokerBuilder[(EventStorageAdapter,)]
):
    def dependencies(
        self, adapter: EventStorageAdapter
    ) -> SingletonEventBrokerDependencies:
        return SingletonEventBrokerDependencies(
            event_source_factory=EventStoreEventSourceFactory(adapter=adapter)
        )


class PostgresSingletonEventBrokerBuilder(
    SingletonEventBrokerBuilder[
        (
            ConnectionSettings,
            AsyncConnectionPool[AsyncConnection],
            EventStorageAdapter | None,
        )
    ]
):
    def dependencies(
        self,
        connection_settings: ConnectionSettings,
        connection_pool: AsyncConnectionPool[AsyncConnection],
        adapter: EventStorageAdapter | None = None,
    ) -> SingletonEventBrokerDependencies:
        event_storage_adapter = adapter or PostgresEventStorageAdapter(
            connection_source=connection_pool
        )
        return SingletonEventBrokerDependencies(
            event_source_factory=EventStoreEventSourceFactory(
                adapter=event_storage_adapter
            )
        )


def make_in_memory_singleton_event_broker(
    node_id: str,
    settings: SingletonEventBrokerSettings,
    adapter: EventStorageAdapter,
) -> EventBroker[StoredEvent]:
    return (
        InMemorySingletonEventBrokerBuilder(node_id)
        .prepare(adapter)
        .build(settings)
    )


def make_postgres_singleton_event_broker(
    node_id: str,
    connection_settings: ConnectionSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    settings: SingletonEventBrokerSettings,
    adapter: EventStorageAdapter | None,
) -> EventBroker[StoredEvent]:
    return (
        PostgresSingletonEventBrokerBuilder(node_id)
        .prepare(connection_settings, connection_pool, adapter)
        .build(settings)
    )
