import asyncio
import os
import random
import sys
from collections.abc import AsyncIterator, Sequence
from dataclasses import fields

import pytest
import pytest_asyncio
from logicblocks.event.testcases.store.adapters import (
    ConcurrencyParameters,
    EventStorageAdapterCases,
)
from logicblocks.event.testsupport import (
    clear_table,
    connection_pool,
    create_table,
    drop_table,
)
from psycopg import AsyncConnection, abc, sql
from psycopg.rows import class_row
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import (
    Condition,
    ConnectionSettings,
    Constant,
    Operator,
    Query,
    QueryApplier,
    TableSettings,
)
from logicblocks.event.persistence.postgres.query import ColumnReference
from logicblocks.event.sources.constraints import QueryConstraint
from logicblocks.event.store.adapters import (
    AnyEventSerialisationGuarantee,
    EventSerialisationGuarantee,
    EventStorageAdapter,
    PostgresEventStorageAdapter,
    PostgresQuerySettings,
)
from logicblocks.event.testing import NewEventBuilder
from logicblocks.event.testing.data import (
    random_event_category_name,
    random_event_stream_name,
)
from logicblocks.event.types import Converter, StoredEvent, identifier

connection_settings = ConnectionSettings(
    user="admin",
    password="super-secret",
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname="some-database",
)


def read_events_query(table: str) -> abc.Query:
    columns = sql.SQL(", ").join(
        sql.Identifier(field.name) for field in fields(StoredEvent)
    )
    return sql.SQL("SELECT {0} FROM {1} ORDER BY sequence_number").format(
        columns, sql.Identifier(table)
    )


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


async def add_unknown_column(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(
            sql.SQL(
                "ALTER TABLE {0} "
                "ADD COLUMN unknown_column TEXT NOT NULL DEFAULT 'unknown'"
            ).format(sql.Identifier(table))
        )


for_each_read_target = pytest.mark.parametrize(
    "target_for",
    [
        lambda category, stream: identifier.LogIdentifier(),
        lambda category, stream: identifier.CategoryIdentifier(
            category=category
        ),
        lambda category, stream: identifier.StreamIdentifier(
            category=category, stream=stream
        ),
    ],
    ids=["log", "category", "stream"],
)


async def save_random_events(
    *,
    adapter: EventStorageAdapter,
    number_of_events: int,
    streams: Sequence[tuple[str, str]],
) -> Sequence[StoredEvent]:
    return [
        event
        for _ in range(number_of_events)
        for event_category, event_stream in [random.choice(streams)]
        for event in await adapter.save(
            target=identifier.StreamIdentifier(
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
    async with connection_pool(connection_settings) as pool:
        yield pool


class TestPostgresEventStorageAdapterCommonCases(EventStorageAdapterCases):
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

    def construct_storage_adapter(
        self,
        *,
        serialisation_guarantee: AnyEventSerialisationGuarantee = EventSerialisationGuarantee.LOG,
    ) -> EventStorageAdapter:
        return PostgresEventStorageAdapter(
            connection_source=self.pool,
            serialisation_guarantee=serialisation_guarantee,
            max_insert_batch_size=1,
        )

    async def clear_storage(self) -> None:
        await clear_table(self.pool, "events")

    async def retrieve_events(
        self,
        *,
        adapter: EventStorageAdapter,
        category: str | None = None,
        stream: str | None = None,
    ) -> Sequence[StoredEvent]:
        return await read_events(
            self.pool,
            table="events",
            category=category,
            stream=stream,
        )


class TestPostgresStorageAdapterCustomTableName:
    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    async def test_uses_table_name_of_events_by_default(self):
        await drop_table(pool=self.pool, table="events")
        await create_table(pool=self.pool, table="events")

        adapter = PostgresEventStorageAdapter(connection_source=self.pool)

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().build()

        stored_events = await adapter.save(
            target=identifier.StreamIdentifier(
                category=event_category, stream=event_stream
            ),
            events=[new_event],
        )

        retrieved_events = await read_events(pool=self.pool, table="events")

        assert retrieved_events == stored_events

    async def test_allows_events_table_name_to_be_overridden(self):
        table_name = "event_log"
        table_settings = TableSettings(table_name=table_name)

        await drop_table(pool=self.pool, table="event_log")
        await create_table(
            pool=self.pool, table="events", renames={"events": "event_log"}
        )

        adapter = PostgresEventStorageAdapter(
            connection_source=self.pool,
            table_settings=table_settings,
        )

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().build()

        stored_events = await adapter.save(
            target=identifier.StreamIdentifier(
                category=event_category, stream=event_stream
            ),
            events=[new_event],
        )

        retrieved_events = await read_events(pool=self.pool, table=table_name)

        assert retrieved_events == stored_events


class TestPostgresStorageAdapterScanPaging:
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

        adapter = PostgresEventStorageAdapter(connection_source=self.pool)

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

        iterator = adapter.scan(target=identifier.LogIdentifier())

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

        adapter = PostgresEventStorageAdapter(
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

        iterator = adapter.scan(target=identifier.LogIdentifier())

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

        adapter = PostgresEventStorageAdapter(connection_source=self.pool)

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
            target=identifier.CategoryIdentifier(category=event_category)
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

        adapter = PostgresEventStorageAdapter(
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
            target=identifier.CategoryIdentifier(category=event_category)
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

        adapter = PostgresEventStorageAdapter(connection_source=self.pool)

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        streams = [(event_category, event_stream)]

        first_page_stored_events = await save_random_events(
            number_of_events=default_page_size,
            adapter=adapter,
            streams=streams,
        )

        iterator = adapter.scan(
            target=identifier.StreamIdentifier(
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

        adapter = PostgresEventStorageAdapter(
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
            target=identifier.StreamIdentifier(
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


class TestPostgresStorageAdapterUnknownColumns:
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

    async def test_saves_to_new_stream_when_table_has_unknown_column(self):
        await add_unknown_column(self.pool, "events")

        adapter = PostgresEventStorageAdapter(connection_source=self.pool)

        stored_events = await adapter.save(
            target=identifier.StreamIdentifier(
                category=random_event_category_name(),
                stream=random_event_stream_name(),
            ),
            events=[NewEventBuilder().build()],
        )

        assert stored_events == await read_events(self.pool, "events")

    @for_each_read_target
    async def test_reads_latest_when_table_has_unknown_column(
        self, target_for
    ):
        adapter = PostgresEventStorageAdapter(connection_source=self.pool)

        category = random_event_category_name()
        stream = random_event_stream_name()

        seeded_events = await adapter.save(
            target=identifier.StreamIdentifier(
                category=category, stream=stream
            ),
            events=[NewEventBuilder().build(), NewEventBuilder().build()],
        )

        await add_unknown_column(self.pool, "events")

        latest_event = await adapter.latest(
            target=target_for(category, stream)
        )

        assert latest_event == seeded_events[-1]

    async def test_saves_to_existing_stream_when_table_has_unknown_column(
        self,
    ):
        adapter = PostgresEventStorageAdapter(connection_source=self.pool)

        target = identifier.StreamIdentifier(
            category=random_event_category_name(),
            stream=random_event_stream_name(),
        )

        await adapter.save(target=target, events=[NewEventBuilder().build()])

        await add_unknown_column(self.pool, "events")

        stored_events = await adapter.save(
            target=target, events=[NewEventBuilder().build()]
        )

        assert stored_events == (await read_events(self.pool, "events"))[-1:]

    @for_each_read_target
    async def test_scans_when_table_has_unknown_column(self, target_for):
        adapter = PostgresEventStorageAdapter(connection_source=self.pool)

        category = random_event_category_name()
        stream = random_event_stream_name()

        seeded_events = await adapter.save(
            target=identifier.StreamIdentifier(
                category=category, stream=stream
            ),
            events=[NewEventBuilder().build(), NewEventBuilder().build()],
        )

        await add_unknown_column(self.pool, "events")

        scanned_events = [
            event
            async for event in adapter.scan(
                target=target_for(category, stream)
            )
        ]

        assert scanned_events == list(seeded_events)

    async def test_saves_to_existing_streams_in_category_when_table_has_unknown_column(
        self,
    ):
        adapter = PostgresEventStorageAdapter(connection_source=self.pool)

        category = random_event_category_name()
        streams = [random_event_stream_name(), random_event_stream_name()]

        seeded_events = [
            event
            for stream in streams
            for event in await adapter.save(
                target=identifier.StreamIdentifier(
                    category=category, stream=stream
                ),
                events=[NewEventBuilder().build()],
            )
        ]

        await add_unknown_column(self.pool, "events")

        stored_events = await adapter.save(
            target=identifier.CategoryIdentifier(category=category),
            streams={
                stream: {"events": [NewEventBuilder().build()]}
                for stream in streams
            },
        )

        seeded_ids = {event.id for event in seeded_events}
        appended_events = [
            event
            for event in await read_events(self.pool, "events")
            if event.id not in seeded_ids
        ]
        appended_events_by_stream = {
            stream: [
                event for event in appended_events if event.stream == stream
            ]
            for stream in {event.stream for event in appended_events}
        }

        assert appended_events_by_stream == stored_events


class TestPostgresStorageAdapterQueryConstraints:
    pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "events")
        await create_table(open_connection_pool, "events")

    async def test_raises_when_using_unknown_query_constraint(self):
        adapter = PostgresEventStorageAdapter(connection_source=self.pool)

        class UnknownQueryConstraint(QueryConstraint): ...

        with pytest.raises(ValueError):
            _ = [
                _
                async for _ in adapter.scan(
                    target=identifier.LogIdentifier(),
                    constraints={UnknownQueryConstraint()},
                )
            ]

    async def test_allows_custom_query_constraints_to_be_applied(self):
        class CustomQueryConstraint(QueryConstraint): ...

        class CustomQueryConstraintQueryApplier(QueryApplier):
            def apply(self, target: Query) -> Query:
                return target.where(
                    Condition()
                    .left(ColumnReference(field="sequence_number"))
                    .operator(Operator.EQUALS)
                    .right(Constant(5))
                )

        class CustomQueryConstraintConverter(
            Converter[QueryConstraint, QueryApplier]
        ):
            def convert(self, item: QueryConstraint) -> QueryApplier:
                return CustomQueryConstraintQueryApplier()

        adapter = PostgresEventStorageAdapter(
            connection_source=self.pool,
            constraint_converter=CustomQueryConstraintConverter(),
        )

        result = [
            event
            async for event in adapter.scan(
                target=identifier.LogIdentifier(),
                constraints={CustomQueryConstraint()},
            )
        ]

        assert result == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
