from abc import ABC
from typing import NotRequired, TypedDict, Unpack, cast, overload
from warnings import deprecated

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.store import (
    EventStorageAdapter,
)
from logicblocks.event.types import StoredEvent

from .base import EventBroker
from .strategies import (
    DistributedEventBrokerSettings,
    SingletonEventBrokerSettings,
    make_in_memory_distributed_event_broker,
    make_in_memory_singleton_event_broker,
    make_postgres_distributed_event_broker,
    make_postgres_singleton_event_broker,
)


class EventBrokerType(ABC):
    Singleton: "type[_SingletonEventBrokerType]"
    Distributed: "type[_DistributedEventBrokerType]"


class _SingletonEventBrokerType(EventBrokerType): ...


class _DistributedEventBrokerType(EventBrokerType): ...


SingletonEventBrokerTypeType = type[_SingletonEventBrokerType]
DistributedEventBrokerTypeType = type[_DistributedEventBrokerType]

EventBrokerType.Singleton = _SingletonEventBrokerType
EventBrokerType.Distributed = _DistributedEventBrokerType


class EventBrokerStorageType(ABC):
    InMemory: "type[_InMemoryEventBrokerStorageType]"
    Postgres: "type[_PostgresEventBrokerStorageType]"


class _InMemoryEventBrokerStorageType(EventBrokerStorageType): ...


class _PostgresEventBrokerStorageType(EventBrokerStorageType): ...


InMemoryEventBrokerStorageTypeType = type[_InMemoryEventBrokerStorageType]
PostgresEventBrokerStorageTypeType = type[_PostgresEventBrokerStorageType]

EventBrokerStorageType.InMemory = _InMemoryEventBrokerStorageType
EventBrokerStorageType.Postgres = _PostgresEventBrokerStorageType


class InMemoryDistributedBrokerParams(TypedDict):
    settings: DistributedEventBrokerSettings
    adapter: EventStorageAdapter


class PostgresDistributedBrokerParams(TypedDict):
    connection_settings: ConnectionSettings
    connection_pool: AsyncConnectionPool[AsyncConnection]
    settings: DistributedEventBrokerSettings
    adapter: NotRequired[EventStorageAdapter | None]


class InMemorySingletonBrokerParams(TypedDict):
    settings: SingletonEventBrokerSettings
    adapter: EventStorageAdapter


class PostgresSingletonBrokerParams(TypedDict):
    connection_settings: ConnectionSettings
    connection_pool: AsyncConnectionPool[AsyncConnection]
    settings: SingletonEventBrokerSettings
    adapter: NotRequired[EventStorageAdapter | None]


class CombinedBrokerParams(TypedDict, total=False):
    settings: DistributedEventBrokerSettings | SingletonEventBrokerSettings
    connection_settings: ConnectionSettings
    connection_pool: AsyncConnectionPool[AsyncConnection]
    adapter: NotRequired[EventStorageAdapter | None]


@overload
def make_event_broker(
    node_id: str,
    broker_type: DistributedEventBrokerTypeType,
    storage_type: InMemoryEventBrokerStorageTypeType,
    **kwargs: Unpack[InMemoryDistributedBrokerParams],
) -> EventBroker[StoredEvent]: ...


@overload
def make_event_broker(
    node_id: str,
    broker_type: DistributedEventBrokerTypeType,
    storage_type: PostgresEventBrokerStorageTypeType,
    **kwargs: Unpack[PostgresDistributedBrokerParams],
) -> EventBroker[StoredEvent]: ...


@overload
def make_event_broker(
    node_id: str,
    broker_type: SingletonEventBrokerTypeType,
    storage_type: InMemoryEventBrokerStorageTypeType,
    **kwargs: Unpack[InMemorySingletonBrokerParams],
) -> EventBroker[StoredEvent]: ...


@overload
def make_event_broker(
    node_id: str,
    broker_type: SingletonEventBrokerTypeType,
    storage_type: PostgresEventBrokerStorageTypeType,
    **kwargs: Unpack[PostgresSingletonBrokerParams],
) -> EventBroker[StoredEvent]: ...


def make_event_broker(
    node_id: str,
    broker_type: (
        SingletonEventBrokerTypeType | DistributedEventBrokerTypeType
    ),
    storage_type: (
        InMemoryEventBrokerStorageTypeType | PostgresEventBrokerStorageTypeType
    ),
    **kwargs: Unpack[CombinedBrokerParams],
) -> EventBroker[StoredEvent]:
    match broker_type, storage_type:
        case EventBrokerType.Distributed, EventBrokerStorageType.InMemory:
            return make_in_memory_distributed_event_broker(
                node_id, **cast(InMemoryDistributedBrokerParams, kwargs)
            )
        case EventBrokerType.Distributed, EventBrokerStorageType.Postgres:
            return make_postgres_distributed_event_broker(
                node_id, **cast(PostgresDistributedBrokerParams, kwargs)
            )
        case EventBrokerType.Singleton, EventBrokerStorageType.InMemory:
            return make_in_memory_singleton_event_broker(
                node_id, **cast(InMemorySingletonBrokerParams, kwargs)
            )
        case EventBrokerType.Singleton, EventBrokerStorageType.Postgres:
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
) -> EventBroker[StoredEvent]:
    return make_in_memory_distributed_event_broker(node_id, settings, adapter)


@deprecated("This function is deprecated, use make_event_broker instead.")
def make_postgres_event_broker(
    node_id: str,
    connection_settings: ConnectionSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    settings: DistributedEventBrokerSettings,
) -> EventBroker[StoredEvent]:
    return make_postgres_distributed_event_broker(
        node_id, connection_settings, connection_pool, settings
    )
