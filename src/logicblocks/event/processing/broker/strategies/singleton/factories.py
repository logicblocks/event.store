from types import NoneType

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.store import (
    EventStorageAdapter,
    EventStoreEventSourceFactory,
    PostgresEventStorageAdapter,
)
from logicblocks.event.types import StoredEvent

from ....services import ErrorHandler, RetryErrorHandler
from ...base import EventBroker
from .builder import (
    SingletonEventBrokerBuilder,
    SingletonEventBrokerDependencies,
    SingletonEventBrokerSettings,
)


class InMemorySingletonEventBrokerBuilder(
    SingletonEventBrokerBuilder[(EventStorageAdapter,)]
):
    """Builder for singleton event brokers reading from a given adapter."""

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
    """Builder for singleton event brokers backed by Postgres.

    Event sources are read from the given adapter, or from a
    `PostgresEventStorageAdapter` on the connection pool if none is given.
    """

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
    error_handler: ErrorHandler[NoneType] = RetryErrorHandler(),
) -> EventBroker[StoredEvent]:
    """Construct a singleton event broker reading from the given adapter.

    Args:
        node_id: The ID of the node on which the broker runs.
        settings: The settings for the broker.
        adapter: The storage adapter from which event sources are read.
        error_handler: The handler for errors raised during execution.

    Returns:
        the constructed event broker.
    """
    return (
        InMemorySingletonEventBrokerBuilder(node_id)
        .prepare(adapter)
        .build(settings, error_handler)
    )


def make_postgres_singleton_event_broker(
    node_id: str,
    connection_settings: ConnectionSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    settings: SingletonEventBrokerSettings,
    adapter: EventStorageAdapter | None = None,
    error_handler: ErrorHandler[NoneType] = RetryErrorHandler(),
) -> EventBroker[StoredEvent]:
    """Construct a singleton event broker backed by Postgres.

    Args:
        node_id: The ID of the node on which the broker runs.
        connection_settings: The Postgres connection settings. Currently
            unused by the singleton broker.
        connection_pool: The pool used to construct the default adapter.
        settings: The settings for the broker.
        adapter: The storage adapter from which event sources are read.
            Defaults to a `PostgresEventStorageAdapter` on
            `connection_pool`.
        error_handler: The handler for errors raised during execution.

    Returns:
        the constructed event broker.
    """
    return (
        PostgresSingletonEventBrokerBuilder(node_id)
        .prepare(connection_settings, connection_pool, adapter)
        .build(settings, error_handler)
    )
