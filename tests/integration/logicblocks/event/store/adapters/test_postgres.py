import asyncio
import random
import sys
from collections.abc import AsyncIterator, Sequence

import pytest
import pytest_asyncio
from psycopg import AsyncConnection, abc, sql
from psycopg.rows import class_row
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.adaptertests import cases
from logicblocks.event.adaptertests.cases import ConcurrencyParameters
from logicblocks.event.store.adapters import (
    PostgresConnectionSettings,
    PostgresQuerySettings,
    PostgresStorageAdapter,
    PostgresTableSettings,
    StorageAdapter,
)
from logicblocks.event.testing import NewEventBuilder
from logicblocks.event.testing.data import (
    random_event_category_name,
    random_event_stream_name,
)
from logicblocks.event.types import StoredEvent, identifier

connection_settings = PostgresConnectionSettings(
    user="admin",
    password="super-secret",
    host="localhost",
    port=5432,
    dbname="some-database",
)


def create_table_query(table: str) -> abc.Query:
    with open("sql/create_events_table.sql") as f:
        create_table_sql = f.read().replace("events", "{0}")

        return create_table_sql.format(table).encode()


def create_indices_query(table: str) -> abc.Query:
    with open("sql/create_events_indices.sql") as f:
        create_indices_sql = f.read().replace("events", "{0}")

        return create_indices_sql.format(table).encode()


def drop_table_query(table_name: str) -> abc.Query:
    return sql.SQL("DROP TABLE IF EXISTS {0}").format(
        sql.Identifier(table_name)
    )


def truncate_table_query(table_name: str) -> abc.Query:
    return sql.SQL("TRUNCATE {0}").format(sql.Identifier(table_name))


def reset_sequence_query(table_name: str, field_name: str) -> abc.Query:
    return sql.SQL("ALTER SEQUENCE {0} RESTART WITH 1").format(
        sql.Identifier("{0}_{1}_seq".format(table_name, field_name))
    )


def read_events_query(table: str) -> abc.Query:
    return sql.SQL("SELECT * FROM {0} ORDER BY sequence_number").format(
        sql.Identifier(table)
    )


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
        await connection.execute(
            reset_sequence_query(table, "sequence_number")
        )


async def drop_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(drop_table_query(table))


async def read_events(
    pool: AsyncConnectionPool[AsyncConnection],
    table: str,
    category: str | None = None,
    stream: str | None = None,
) -> list[StoredEvent]:
    async with pool.connection() as connection:
        async with connection.cursor(
            row_factory=class_row(StoredEvent)
        ) as cursor:
            results = await cursor.execute(read_events_query(table))
            events = await results.fetchall()
            events = (
                [event for event in events if event.category == category]
                if category
                else events
            )
            events = (
                [event for event in events if event.stream == stream]
                if stream
                else events
            )

            return events


async def save_random_events(
    *,
    adapter: StorageAdapter,
    number_of_events: int,
    streams: Sequence[tuple[str, str]],
) -> Sequence[StoredEvent]:
    return [
        event
        for _ in range(number_of_events)
        for event_category, event_stream in [random.choice(streams)]
        for event in await adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[NewEventBuilder().build()],
        )
    ]


async def read_iterator_events(
    *, iterator: AsyncIterator[StoredEvent], number_of_events: int
):
    return [await anext(iterator) for _ in range(number_of_events)]


@pytest_asyncio.fixture
async def open_connection_pool():
    conninfo = connection_settings.to_connection_string()
    pool = AsyncConnectionPool[AsyncConnection](conninfo, open=False)

    await pool.open()

    try:
        yield pool
    finally:
        await pool.close()


class TestPostgresStorageAdapterCommonCases(cases.StorageAdapterCases):
    pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "events")
        await create_table(open_connection_pool, "events")

    @property
    def concurrency_parameters(self):
        return ConcurrencyParameters(concurrent_writes=3, repeats=5)

    @property
    def default_page_size(self) -> int:
        return PostgresQuerySettings().scan_query_page_size

    def construct_storage_adapter(self) -> StorageAdapter:
        return PostgresStorageAdapter(connection_source=self.pool)

    async def clear_storage(self) -> None:
        await clear_table(self.pool, "events")

    async def retrieve_events(
        self,
        *,
        adapter: StorageAdapter,
        category: str | None = None,
        stream: str | None = None,
    ) -> Sequence[StoredEvent]:
        return await read_events(
            self.pool,
            table="events",
            category=category,
            stream=stream,
        )


class TestPostgresStorageAdapterCustomTableName(object):
    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    async def test_uses_table_name_of_events_by_default(self):
        await drop_table(pool=self.pool, table="events")
        await create_table(pool=self.pool, table="events")

        adapter = PostgresStorageAdapter(connection_source=self.pool)

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().build()

        stored_events = await adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[new_event],
        )

        retrieved_events = await read_events(pool=self.pool, table="events")

        assert retrieved_events == stored_events

    async def test_allows_events_table_name_to_be_overridden(self):
        table_name = "event_log"
        table_settings = PostgresTableSettings(events_table_name=table_name)

        await drop_table(pool=self.pool, table="event_log")
        await create_table(pool=self.pool, table="event_log")

        adapter = PostgresStorageAdapter(
            connection_source=self.pool,
            table_settings=table_settings,
        )

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().build()

        stored_events = await adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[new_event],
        )

        retrieved_events = await read_events(pool=self.pool, table=table_name)

        assert retrieved_events == stored_events


class TestPostgresStorageAdapterScanPaging(object):
    pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "events")
        await create_table(open_connection_pool, "events")

    @pytest_asyncio.fixture(autouse=True)
    async def shutdown_async_generators(self):
        yield

        await asyncio.get_event_loop().shutdown_asyncgens()

    async def test_pages_log_scan_using_default_page_size(self):
        default_page_size = PostgresQuerySettings().scan_query_page_size

        adapter = PostgresStorageAdapter(connection_source=self.pool)

        event_category_1 = random_event_category_name()
        event_category_2 = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        streams = [
            (event_category_1, event_stream_1),
            (event_category_2, event_stream_2),
        ]

        first_page_stored_events = await save_random_events(
            number_of_events=default_page_size,
            adapter=adapter,
            streams=streams,
        )

        iterator = adapter.scan(target=identifier.Log())

        first_page_scanned_events = await read_iterator_events(
            number_of_events=default_page_size, iterator=iterator
        )
        second_page_stored_events = await save_random_events(
            number_of_events=default_page_size,
            adapter=adapter,
            streams=streams,
        )
        second_page_scanned_events = await read_iterator_events(
            number_of_events=default_page_size, iterator=iterator
        )

        last_page_stored_events = await save_random_events(
            number_of_events=int(default_page_size / 2),
            adapter=adapter,
            streams=streams,
        )
        last_page_scanned_events = await read_iterator_events(
            number_of_events=int(default_page_size / 2), iterator=iterator
        )

        stored_events = (
            list(first_page_stored_events)
            + list(second_page_stored_events)
            + list(last_page_stored_events)
        )
        scanned_events = (
            first_page_scanned_events
            + second_page_scanned_events
            + last_page_scanned_events
        )

        with pytest.raises(StopAsyncIteration):
            await anext(iterator)

        assert scanned_events == stored_events

    async def test_pages_log_scan_using_overridden_page_size(self):
        page_size = 25

        adapter = PostgresStorageAdapter(
            connection_source=self.pool,
            query_settings=PostgresQuerySettings(
                scan_query_page_size=page_size
            ),
        )

        event_category_1 = random_event_category_name()
        event_category_2 = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        streams = [
            (event_category_1, event_stream_1),
            (event_category_2, event_stream_2),
        ]

        first_page_stored_events = await save_random_events(
            number_of_events=page_size,
            adapter=adapter,
            streams=streams,
        )

        iterator = adapter.scan(target=identifier.Log())

        first_page_scanned_events = await read_iterator_events(
            number_of_events=page_size, iterator=iterator
        )
        second_page_stored_events = await save_random_events(
            number_of_events=page_size,
            adapter=adapter,
            streams=streams,
        )
        second_page_scanned_events = await read_iterator_events(
            number_of_events=page_size, iterator=iterator
        )

        last_page_stored_events = await save_random_events(
            number_of_events=int(page_size / 2),
            adapter=adapter,
            streams=streams,
        )
        last_page_scanned_events = await read_iterator_events(
            number_of_events=int(page_size / 2), iterator=iterator
        )

        stored_events = (
            list(first_page_stored_events)
            + list(second_page_stored_events)
            + list(last_page_stored_events)
        )
        scanned_events = (
            first_page_scanned_events
            + second_page_scanned_events
            + last_page_scanned_events
        )

        with pytest.raises(StopAsyncIteration):
            await anext(iterator)

        assert scanned_events == stored_events

    async def test_pages_category_scan_using_default_page_size(self):
        default_page_size = PostgresQuerySettings().scan_query_page_size

        adapter = PostgresStorageAdapter(connection_source=self.pool)

        event_category = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        streams = [
            (event_category, event_stream_1),
            (event_category, event_stream_2),
        ]

        first_page_stored_events = await save_random_events(
            number_of_events=default_page_size,
            adapter=adapter,
            streams=streams,
        )

        iterator = adapter.scan(
            target=identifier.Category(category=event_category)
        )

        first_page_scanned_events = await read_iterator_events(
            number_of_events=default_page_size, iterator=iterator
        )

        second_page_stored_events = await save_random_events(
            number_of_events=default_page_size,
            adapter=adapter,
            streams=streams,
        )
        second_page_scanned_events = await read_iterator_events(
            number_of_events=default_page_size, iterator=iterator
        )

        last_page_stored_events = await save_random_events(
            number_of_events=20, adapter=adapter, streams=streams
        )
        last_page_scanned_events = await read_iterator_events(
            number_of_events=20, iterator=iterator
        )

        stored_events = (
            list(first_page_stored_events)
            + list(second_page_stored_events)
            + list(last_page_stored_events)
        )
        scanned_events = (
            first_page_scanned_events
            + second_page_scanned_events
            + last_page_scanned_events
        )

        with pytest.raises(StopAsyncIteration):
            await anext(iterator)

        assert scanned_events == stored_events

    async def test_pages_category_scan_using_overridden_page_size(self):
        page_size = 25

        adapter = PostgresStorageAdapter(
            connection_source=self.pool,
            query_settings=PostgresQuerySettings(
                scan_query_page_size=page_size
            ),
        )

        event_category = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        streams = [
            (event_category, event_stream_1),
            (event_category, event_stream_2),
        ]

        first_page_stored_events = await save_random_events(
            number_of_events=page_size,
            adapter=adapter,
            streams=streams,
        )

        iterator = adapter.scan(
            target=identifier.Category(category=event_category)
        )

        first_page_scanned_events = await read_iterator_events(
            number_of_events=page_size, iterator=iterator
        )

        second_page_stored_events = await save_random_events(
            number_of_events=page_size,
            adapter=adapter,
            streams=streams,
        )
        second_page_scanned_events = await read_iterator_events(
            number_of_events=page_size, iterator=iterator
        )

        last_page_stored_events = await save_random_events(
            number_of_events=int(page_size / 2),
            adapter=adapter,
            streams=streams,
        )
        last_page_scanned_events = await read_iterator_events(
            number_of_events=int(page_size / 2), iterator=iterator
        )

        stored_events = (
            list(first_page_stored_events)
            + list(second_page_stored_events)
            + list(last_page_stored_events)
        )
        scanned_events = (
            first_page_scanned_events
            + second_page_scanned_events
            + last_page_scanned_events
        )

        with pytest.raises(StopAsyncIteration):
            await anext(iterator)

        assert scanned_events == stored_events

    async def test_pages_stream_scan_using_default_page_size(self):
        default_page_size = PostgresQuerySettings().scan_query_page_size

        adapter = PostgresStorageAdapter(connection_source=self.pool)

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        streams = [(event_category, event_stream)]

        first_page_stored_events = await save_random_events(
            number_of_events=default_page_size,
            adapter=adapter,
            streams=streams,
        )

        iterator = adapter.scan(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            )
        )

        first_page_scanned_events = await read_iterator_events(
            number_of_events=default_page_size, iterator=iterator
        )

        second_page_stored_events = await save_random_events(
            number_of_events=default_page_size,
            adapter=adapter,
            streams=streams,
        )
        second_page_scanned_events = await read_iterator_events(
            number_of_events=default_page_size, iterator=iterator
        )

        last_page_stored_events = await save_random_events(
            number_of_events=20, adapter=adapter, streams=streams
        )
        last_page_scanned_events = await read_iterator_events(
            number_of_events=20, iterator=iterator
        )

        stored_events = (
            list(first_page_stored_events)
            + list(second_page_stored_events)
            + list(last_page_stored_events)
        )
        scanned_events = (
            first_page_scanned_events
            + second_page_scanned_events
            + last_page_scanned_events
        )

        with pytest.raises(StopAsyncIteration):
            await anext(iterator)

        assert scanned_events == stored_events

    async def test_pages_stream_scan_using_overridden_page_size(self):
        page_size = 25

        adapter = PostgresStorageAdapter(
            connection_source=self.pool,
            query_settings=PostgresQuerySettings(
                scan_query_page_size=page_size
            ),
        )

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        streams = [(event_category, event_stream)]

        first_page_stored_events = await save_random_events(
            number_of_events=page_size,
            adapter=adapter,
            streams=streams,
        )

        iterator = adapter.scan(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            )
        )

        first_page_scanned_events = await read_iterator_events(
            number_of_events=page_size, iterator=iterator
        )

        second_page_stored_events = await save_random_events(
            number_of_events=page_size,
            adapter=adapter,
            streams=streams,
        )
        second_page_scanned_events = await read_iterator_events(
            number_of_events=page_size, iterator=iterator
        )

        last_page_stored_events = await save_random_events(
            number_of_events=int(page_size / 2),
            adapter=adapter,
            streams=streams,
        )
        last_page_scanned_events = await read_iterator_events(
            number_of_events=int(page_size / 2), iterator=iterator
        )

        stored_events = (
            list(first_page_stored_events)
            + list(second_page_stored_events)
            + list(last_page_stored_events)
        )
        scanned_events = (
            first_page_scanned_events
            + second_page_scanned_events
            + last_page_scanned_events
        )

        with pytest.raises(StopAsyncIteration):
            await anext(iterator)

        assert scanned_events == stored_events


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
