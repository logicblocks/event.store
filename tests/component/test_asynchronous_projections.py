import asyncio
import os
import time
from dataclasses import dataclass
from datetime import timedelta
from types import NoneType
from typing import Any, Awaitable, Callable, Mapping

import pytest_asyncio
from psycopg import AsyncConnection, abc, sql
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db import PostgresConnectionSettings
from logicblocks.event.processing.broker import make_postgres_event_broker
from logicblocks.event.processing.consumers import (
    EventCount,
    ProjectionEventProcessor,
    make_subscriber,
)
from logicblocks.event.processing.services import Service, ServiceManager
from logicblocks.event.projection import (
    PostgresProjectionStorageAdapter,
    ProjectionStore,
    Projector,
)
from logicblocks.event.store import (
    EventStore,
    PostgresEventStorageAdapter,
)
from logicblocks.event.testing import NewEventBuilder, data
from logicblocks.event.types import (
    CategoryIdentifier,
    EventSequenceIdentifier,
    StoredEvent,
    StreamIdentifier,
)

connection_settings = PostgresConnectionSettings(
    user="admin",
    password="super-secret",
    host="localhost",
    port=5432,
    dbname="some-database",
)

project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)


def relative_to_root(*path_parts: str) -> str:
    return os.path.join(project_root, *path_parts)


def read_sql(sql_file_name: str) -> abc.Query:
    with open(relative_to_root("sql", f"{sql_file_name}.sql")) as f:
        return f.read().encode()


def create_table_query(table: str) -> abc.Query:
    return read_sql(f"create_{table}_table")


def create_indices_query(table: str) -> abc.Query:
    return read_sql(f"create_{table}_indices")


def drop_table_query(table_name: str) -> abc.Query:
    return sql.SQL("DROP TABLE IF EXISTS {0}").format(
        sql.Identifier(table_name)
    )


def truncate_table_query(table_name: str) -> abc.Query:
    return sql.SQL("TRUNCATE {0}").format(sql.Identifier(table_name))


async def create_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(create_table_query(table))
        await connection.execute(create_indices_query(table))


async def clear_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(truncate_table_query(table))


async def drop_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(drop_table_query(table))


@pytest_asyncio.fixture
async def open_connection_pool():
    conninfo = connection_settings.to_connection_string()
    pool = AsyncConnectionPool[AsyncConnection](conninfo, open=False)

    await pool.open()

    try:
        yield pool
    finally:
        await pool.close()


def to_dict(thing: object) -> Mapping[str, Any]:
    return thing.__dict__


def from_dict[T](target_type: type[T]) -> Callable[[Mapping[str, Any]], T]:
    def converter(mapping: Mapping[str, Any]) -> T:
        return target_type(**mapping)

    return converter


class TestAsynchronousProjections:
    connection_pool: AsyncConnectionPool[AsyncConnection]
    service_manager: ServiceManager

    @pytest_asyncio.fixture(autouse=True)
    async def initialise_service_manager(self):
        self.service_manager = ServiceManager()

        yield

        await self.service_manager.stop()

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.connection_pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "events")
        await drop_table(open_connection_pool, "projections")
        await drop_table(open_connection_pool, "nodes")
        await drop_table(open_connection_pool, "subscribers")
        await drop_table(open_connection_pool, "subscriptions")
        await create_table(open_connection_pool, "events")
        await create_table(open_connection_pool, "projections")
        await create_table(open_connection_pool, "nodes")
        await create_table(open_connection_pool, "subscribers")
        await create_table(open_connection_pool, "subscriptions")

    async def test_projects_from_category(self):
        node_id = data.random_node_id()
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        event_store = EventStore(
            adapter=PostgresEventStorageAdapter(
                connection_source=self.connection_pool
            )
        )

        thing_projection_store = ProjectionStore(
            adapter=PostgresProjectionStorageAdapter(
                connection_source=self.connection_pool
            )
        )

        @dataclass
        class Thing:
            value: int = 5

        class ThingProjector(Projector[Thing]):
            name = "specific-thing"

            def initial_state_factory(self) -> Thing:
                return Thing()

            def id_factory(
                self, state: Thing, coordinates: EventSequenceIdentifier
            ):
                match coordinates:
                    case StreamIdentifier(stream=stream):
                        return stream
                    case _:
                        raise ValueError("Unexpected coordinates.")

            @staticmethod
            def thing_got_value(state: Thing, event: StoredEvent) -> Thing:
                state.value = event.payload["value"]
                return state

        thing_projector = ThingProjector()

        event_processor = ProjectionEventProcessor(
            projector=thing_projector,
            projection_store=thing_projection_store,
            from_dict_converter=from_dict(Thing),
            to_dict_converter=to_dict,
        )

        subscriber_group = data.random_subscriber_group()
        subscriber_state_category = event_store.category(
            category=f"subscriber-{subscriber_group}"
        )

        subscriber = make_subscriber(
            subscriber_group=subscriber_group,
            subscriber_sequence=CategoryIdentifier(category=category_name),
            subscriber_state_category=subscriber_state_category,
            subscriber_state_persistence_interval=EventCount(1),
            event_processor=event_processor,
        )

        event_broker = make_postgres_event_broker(
            node_id=node_id,
            connection_settings=connection_settings,
            connection_pool=self.connection_pool,
        )

        await event_broker.register(subscriber=subscriber)

        class PollingService(Service[NoneType]):
            _callable: Callable[[], Awaitable]
            _poll_interval: timedelta = timedelta(milliseconds=200)

            def __init__(
                self,
                callable: Callable[[], Awaitable],
                poll_interval: timedelta = timedelta(milliseconds=200),
            ):
                self._callable = callable
                self._poll_interval = poll_interval

            async def execute(self):
                while True:
                    await self._callable()
                    await asyncio.sleep(self._poll_interval.total_seconds())

        subscriber_service = PollingService(
            callable=subscriber.consume_all,
        )

        self.service_manager.register(event_broker)
        self.service_manager.register(subscriber_service)

        await self.service_manager.start()

        value = data.random_int()

        await event_store.stream(
            category=category_name,
            stream=stream_name,
        ).publish(
            events=[
                (
                    NewEventBuilder()
                    .with_name("thing-got-value")
                    .with_payload({"value": value})
                    .build()
                )
            ]
        )

        projected_thing = None

        timeout_start = time.time()
        timeout_seconds = 5
        while time.time() < timeout_start + timeout_seconds:
            projected_thing = await thing_projection_store.locate(
                source=StreamIdentifier(
                    category=data.random_event_category_name(),
                    stream=data.random_event_stream_name(),
                ),
                name=data.random_projection_name(),
                converter=from_dict(Thing),
            )
            if projected_thing:
                break

        assert projected_thing
        assert projected_thing.state.value == value
