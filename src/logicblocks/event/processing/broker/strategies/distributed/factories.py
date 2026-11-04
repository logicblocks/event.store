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

from ....locks import InMemoryLockManager, PostgresLockManager
from ....services import ErrorHandler, RetryErrorHandler
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
    DistributedEventBrokerBuilder[(EventStorageAdapter,)]
):
    """Builder for distributed event brokers with in-memory state.

    Locks and subscriber and subscription state are held in memory by each
    built broker, so it does not coordinate with any other broker.
    """

    def dependencies(
        self, adapter: EventStorageAdapter
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
        )


class PostgresDistributedEventBrokerBuilder(
    DistributedEventBrokerBuilder[
        (
            ConnectionSettings,
            AsyncConnectionPool[AsyncConnection],
            EventStorageAdapter | None,
        )
    ]
):
    """Builder for distributed event brokers with Postgres-backed state.

    Event sources are read from the given adapter, or from a
    `PostgresEventStorageAdapter` on the connection pool if none is given.
    """

    def dependencies(
        self,
        connection_settings: ConnectionSettings,
        connection_pool: AsyncConnectionPool[AsyncConnection],
        adapter: EventStorageAdapter | None = None,
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
        )


def make_in_memory_distributed_event_broker(
    node_id: str,
    settings: DistributedEventBrokerSettings,
    adapter: EventStorageAdapter,
    error_handler: ErrorHandler[NoneType] = RetryErrorHandler(),
) -> EventBroker[StoredEvent]:
    """Construct a distributed event broker with in-memory state.

    Args:
        node_id: The ID of the node on which the broker runs.
        settings: The settings for the broker.
        adapter: The storage adapter from which event sources are read.
        error_handler: The handler for errors raised during execution.

    Returns:
        the constructed event broker.
    """
    return (
        InMemoryDistributedEventBrokerBuilder(node_id)
        .prepare(adapter)
        .build(settings, error_handler)
    )


def make_postgres_distributed_event_broker(
    node_id: str,
    connection_settings: ConnectionSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    settings: DistributedEventBrokerSettings,
    adapter: EventStorageAdapter | None = None,
    error_handler: ErrorHandler[NoneType] = RetryErrorHandler(),
) -> EventBroker[StoredEvent]:
    """Construct a distributed event broker with Postgres-backed state.

    Args:
        node_id: The ID of the node on which the broker runs.
        connection_settings: The settings used to connect for leadership
            locking.
        connection_pool: The pool used for subscriber and subscription
            state, and for the default adapter.
        settings: The settings for the broker.
        adapter: The storage adapter from which event sources are read.
            Defaults to a `PostgresEventStorageAdapter` on
            `connection_pool`.
        error_handler: The handler for errors raised during execution.

    Returns:
        the constructed event broker.
    """
    return (
        PostgresDistributedEventBrokerBuilder(node_id)
        .prepare(connection_settings, connection_pool, adapter)
        .build(settings, error_handler)
    )
