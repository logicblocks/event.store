from abc import ABC
from typing import TypedDict, Unpack, cast, overload
from warnings import deprecated

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.store import (
    EventStorageAdapter,
)

from .base import EventBroker
from .strategies import (
    DistributedEventBrokerSettings,
    SingletonEventBrokerSettings,
    make_in_memory_distributed_event_broker,
    make_in_memory_singleton_event_broker,
    make_postgres_distributed_event_broker,
    make_postgres_singleton_event_broker,
)


class BrokerType(ABC):
    Singleton: "type[_SingletonBrokerType]"
    Distributed: "type[_DistributedBrokerType]"


class _SingletonBrokerType(BrokerType): ...


class _DistributedBrokerType(BrokerType): ...


BrokerType.Singleton = _SingletonBrokerType
BrokerType.Distributed = _DistributedBrokerType


class StorageType(ABC):
    InMemory: "type[_InMemoryStorageType]"
    Postgres: "type[_PostgresStorageType]"


class _InMemoryStorageType(StorageType): ...


class _PostgresStorageType(StorageType): ...


StorageType.InMemory = _InMemoryStorageType
StorageType.Postgres = _PostgresStorageType


class InMemoryDistributedBrokerParams(TypedDict):
    settings: DistributedEventBrokerSettings
    adapter: EventStorageAdapter


class PostgresDistributedBrokerParams(TypedDict):
    connection_settings: ConnectionSettings
    connection_pool: AsyncConnectionPool[AsyncConnection]
    settings: DistributedEventBrokerSettings
    adapter: EventStorageAdapter | None


class InMemorySingletonBrokerParams(TypedDict):
    settings: SingletonEventBrokerSettings
    adapter: EventStorageAdapter


class PostgresSingletonBrokerParams(TypedDict):
    connection_settings: ConnectionSettings
    connection_pool: AsyncConnectionPool[AsyncConnection]
    settings: SingletonEventBrokerSettings
    adapter: EventStorageAdapter | None


class CombinedBrokerParams(TypedDict, total=False):
    settings: DistributedEventBrokerSettings | SingletonEventBrokerSettings
    connection_settings: ConnectionSettings
    connection_pool: AsyncConnectionPool[AsyncConnection]
    adapter: EventStorageAdapter | None


@overload
def make_event_broker(
    node_id: str,
    broker_type: type[_DistributedBrokerType],
    storage_type: type[_InMemoryStorageType],
    **kwargs: Unpack[InMemoryDistributedBrokerParams],
) -> EventBroker: ...


@overload
def make_event_broker(
    node_id: str,
    broker_type: type[_DistributedBrokerType],
    storage_type: type[_PostgresStorageType],
    **kwargs: Unpack[PostgresDistributedBrokerParams],
) -> EventBroker: ...


@overload
def make_event_broker(
    node_id: str,
    broker_type: type[_SingletonBrokerType],
    storage_type: type[_InMemoryStorageType],
    **kwargs: Unpack[InMemorySingletonBrokerParams],
) -> EventBroker: ...


@overload
def make_event_broker(
    node_id: str,
    broker_type: type[_SingletonBrokerType],
    storage_type: type[_PostgresStorageType],
    **kwargs: Unpack[PostgresSingletonBrokerParams],
) -> EventBroker: ...


def make_event_broker(
    node_id: str,
    broker_type: type[_SingletonBrokerType] | type[_DistributedBrokerType],
    storage_type: type[_InMemoryStorageType] | type[_PostgresStorageType],
    **kwargs: Unpack[CombinedBrokerParams],
) -> EventBroker:
    match broker_type, storage_type:
        case BrokerType.Distributed, StorageType.InMemory:
            return make_in_memory_distributed_event_broker(
                node_id, **cast(InMemoryDistributedBrokerParams, kwargs)
            )
        case BrokerType.Distributed, StorageType.Postgres:
            return make_postgres_distributed_event_broker(
                node_id, **cast(PostgresDistributedBrokerParams, kwargs)
            )
        case BrokerType.Singleton, StorageType.InMemory:
            return make_in_memory_singleton_event_broker(
                node_id, **cast(InMemorySingletonBrokerParams, kwargs)
            )
        case BrokerType.Singleton, StorageType.Postgres:
            return make_postgres_singleton_event_broker(
                node_id, **cast(PostgresSingletonBrokerParams, kwargs)
            )
        case _:
            raise ValueError("Invalid broker or storage type")


@deprecated("This function is deprecated, use make_event_broker instead.")
def make_in_memory_event_broker(
    node_id: str,
    settings: DistributedEventBrokerSettings,
    adapter: EventStorageAdapter,
) -> EventBroker:
    return make_in_memory_distributed_event_broker(node_id, settings, adapter)


@deprecated("This function is deprecated, use make_event_broker instead.")
def make_postgres_event_broker(
    node_id: str,
    connection_settings: ConnectionSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    settings: DistributedEventBrokerSettings,
) -> EventBroker:
    return make_postgres_distributed_event_broker(
        node_id, connection_settings, connection_pool, settings
    )
