from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.store import (
    InMemoryEventStorageAdapter,
)

from .base import EventBroker
from .strategies import (
    make_in_memory_distributed_event_broker,
    make_postgres_distributed_event_broker,
)
from .strategies.distributed import DistributedEventBrokerSettings


def make_in_memory_event_broker(
    node_id: str,
    settings: DistributedEventBrokerSettings,
    adapter: InMemoryEventStorageAdapter,
) -> EventBroker:
    return make_in_memory_distributed_event_broker(node_id, settings, adapter)


def make_postgres_event_broker(
    node_id: str,
    connection_settings: ConnectionSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    settings: DistributedEventBrokerSettings,
) -> EventBroker:
    return make_postgres_distributed_event_broker(
        node_id, connection_settings, connection_pool, settings
    )
