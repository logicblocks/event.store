from abc import ABC
from types import NoneType
from typing import NotRequired, TypedDict, Unpack, cast, overload
from warnings import deprecated

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.store import (
    EventStorageAdapter,
)
from logicblocks.event.types import StoredEvent

from ..services import ErrorHandler
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
    """Marker types selecting the broker strategy for `make_event_broker`.

    Attributes:
        Singleton: Selects a broker that allocates every requested event
            source to every registered subscriber on the local node.
        Distributed: Selects a broker that coordinates across nodes,
            sharing the requested event sources between the subscribers in
            each subscriber group.
    """

    Singleton: "type[_SingletonEventBrokerType]"
    Distributed: "type[_DistributedEventBrokerType]"


class _SingletonEventBrokerType(EventBrokerType): ...


class _DistributedEventBrokerType(EventBrokerType): ...


SingletonEventBrokerTypeType = type[_SingletonEventBrokerType]
DistributedEventBrokerTypeType = type[_DistributedEventBrokerType]

EventBrokerType.Singleton = _SingletonEventBrokerType
EventBrokerType.Distributed = _DistributedEventBrokerType


class EventBrokerStorageType(ABC):
    """Marker types selecting the broker storage for `make_event_broker`.

    Attributes:
        InMemory: Selects in-memory storage, suitable for tests and single
            process deployments.
        Postgres: Selects PostgreSQL storage, required for coordination
            between multiple nodes.
    """

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
    error_handler: NotRequired[ErrorHandler[NoneType]]


class PostgresDistributedBrokerParams(TypedDict):
    connection_settings: ConnectionSettings
    connection_pool: AsyncConnectionPool[AsyncConnection]
    settings: DistributedEventBrokerSettings
    adapter: NotRequired[EventStorageAdapter | None]
    error_handler: NotRequired[ErrorHandler[NoneType]]


class InMemorySingletonBrokerParams(TypedDict):
    settings: SingletonEventBrokerSettings
    adapter: EventStorageAdapter
    error_handler: NotRequired[ErrorHandler[NoneType]]


class PostgresSingletonBrokerParams(TypedDict):
    connection_settings: ConnectionSettings
    connection_pool: AsyncConnectionPool[AsyncConnection]
    settings: SingletonEventBrokerSettings
    adapter: NotRequired[EventStorageAdapter | None]
    error_handler: NotRequired[ErrorHandler[NoneType]]


class CombinedBrokerParams(TypedDict, total=False):
    settings: DistributedEventBrokerSettings | SingletonEventBrokerSettings
    connection_settings: ConnectionSettings
    connection_pool: AsyncConnectionPool[AsyncConnection]
    adapter: NotRequired[EventStorageAdapter | None]
    error_handler: NotRequired[ErrorHandler[NoneType]]


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
    """Construct an event broker of the given type and storage.

    The keyword arguments required depend on the combination of broker and
    storage type:

    - in-memory storage requires `settings` and `adapter`;
    - Postgres storage requires `settings`, `connection_settings` and
      `connection_pool`, and optionally accepts `adapter`, defaulting to a
      `PostgresEventStorageAdapter` using `connection_pool`.

    All combinations optionally accept an `error_handler`, defaulting to a
    `RetryErrorHandler`.

    Args:
        node_id: The ID of the node on which the broker runs.
        broker_type: The broker strategy, one of `EventBrokerType.Singleton`
            or `EventBrokerType.Distributed`.
        storage_type: The storage for broker state, one of
            `EventBrokerStorageType.InMemory` or
            `EventBrokerStorageType.Postgres`.
        **kwargs: The settings and dependencies for the chosen combination,
            where `settings` must be a `SingletonEventBrokerSettings` or
            `DistributedEventBrokerSettings` to match `broker_type`.

    Returns:
        the constructed, not yet running, event broker.

    Raises:
        ValueError: If the broker or storage type is not recognised.
    """
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
    """Construct an in-memory distributed event broker.

    Deprecated: use `make_event_broker` with `EventBrokerType.Distributed`
    and `EventBrokerStorageType.InMemory` instead.

    Args:
        node_id: The ID of the node on which the broker runs.
        settings: The settings for the distributed broker.
        adapter: The storage adapter from which event sources are read.

    Returns:
        the constructed event broker.
    """
    return make_in_memory_distributed_event_broker(node_id, settings, adapter)


@deprecated("This function is deprecated, use make_event_broker instead.")
def make_postgres_event_broker(
    node_id: str,
    connection_settings: ConnectionSettings,
    connection_pool: AsyncConnectionPool[AsyncConnection],
    settings: DistributedEventBrokerSettings,
) -> EventBroker[StoredEvent]:
    """Construct a Postgres-backed distributed event broker.

    Deprecated: use `make_event_broker` with `EventBrokerType.Distributed`
    and `EventBrokerStorageType.Postgres` instead.

    Args:
        node_id: The ID of the node on which the broker runs.
        connection_settings: The settings used to connect for locking.
        connection_pool: The pool used for broker state and event storage.
        settings: The settings for the distributed broker.

    Returns:
        the constructed event broker.
    """
    return make_postgres_distributed_event_broker(
        node_id, connection_settings, connection_pool, settings
    )
