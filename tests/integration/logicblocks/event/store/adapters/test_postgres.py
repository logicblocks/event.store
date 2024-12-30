import sys
from collections.abc import Sequence

import pytest
from psycopg import Connection, abc, sql
from psycopg.rows import class_row
from psycopg_pool import ConnectionPool

from logicblocks.event.adaptertests import cases
from logicblocks.event.adaptertests.cases import ConcurrencyParameters
from logicblocks.event.store.adapters import (
    PostgresConnectionParameters,
    PostgresStorageAdapter,
    PostgresTableParameters,
    StorageAdapter,
)
from logicblocks.event.testing import NewEventBuilder
from logicblocks.event.testing.data import (
    random_event_category_name,
    random_event_stream_name,
)
from logicblocks.event.types import StoredEvent, identifier

connection_parameters = PostgresConnectionParameters(
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


def create_table(pool: ConnectionPool[Connection], table: str) -> None:
    with pool.connection() as connection:
        connection.execute(create_table_query(table))
        connection.execute(create_indices_query(table))


def clear_table(pool: ConnectionPool[Connection], table: str) -> None:
    with pool.connection() as connection:
        connection.execute(truncate_table_query(table))
        connection.execute(reset_sequence_query(table, "sequence_number"))


def drop_table(pool: ConnectionPool[Connection], table: str) -> None:
    with pool.connection() as connection:
        connection.execute(drop_table_query(table))


def read_events(
    pool: ConnectionPool[Connection],
    table: str,
    category: str | None = None,
    stream: str | None = None,
) -> list[StoredEvent]:
    with pool.connection() as connection:
        with connection.cursor(row_factory=class_row(StoredEvent)) as cursor:
            events = cursor.execute(read_events_query(table)).fetchall()
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


class TestPostgresStorageAdapterCommonCases(cases.StorageAdapterCases):
    pool: ConnectionPool[Connection]

    def setup_method(self):
        self.create_connection_pool()
        self.destroy_storage()
        self.initialise_storage()

    def teardown_method(self):
        self.destroy_connection_pool()

    @property
    def concurrency_parameters(self):
        return ConcurrencyParameters(concurrent_writes=3, repeats=5)

    def create_connection_pool(self):
        self.pool = ConnectionPool[Connection](
            connection_parameters.to_connection_string(), open=True
        )

    def destroy_connection_pool(self):
        self.pool.close()

    def initialise_storage(self) -> None:
        create_table(self.pool, "events")

    def clear_storage(self) -> None:
        clear_table(self.pool, "events")

    def destroy_storage(self) -> None:
        drop_table(self.pool, "events")

    def construct_storage_adapter(self) -> StorageAdapter:
        return PostgresStorageAdapter(connection_source=self.pool)

    def retrieve_events(
        self,
        *,
        adapter: StorageAdapter,
        category: str | None = None,
        stream: str | None = None,
    ) -> Sequence[StoredEvent]:
        return read_events(
            self.pool,
            table="events",
            category=category,
            stream=stream,
        )


class TestPostgresStorageAdapterCustomTableName(object):
    def setup_method(self):
        self.connection_pool = ConnectionPool[Connection](
            connection_parameters.to_connection_string(), open=True
        )

    def teardown_method(self):
        self.connection_pool.close()

    def test_uses_table_name_of_events_by_default(self):
        drop_table(pool=self.connection_pool, table="events")
        create_table(pool=self.connection_pool, table="events")

        adapter = PostgresStorageAdapter(
            connection_source=self.connection_pool
        )

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().build()

        stored_events = adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[new_event],
        )

        retrieved_events = read_events(
            pool=self.connection_pool, table="events"
        )

        assert retrieved_events == stored_events

    def test_allows_events_table_name_to_be_overridden(self):
        connection_pool = ConnectionPool[Connection](
            connection_parameters.to_connection_string(), open=True
        )

        table_name = "event_log"
        table_parameters = PostgresTableParameters(
            events_table_name=table_name
        )

        drop_table(pool=connection_pool, table="event_log")
        create_table(pool=connection_pool, table="event_log")

        adapter = PostgresStorageAdapter(
            connection_source=connection_pool,
            table_parameters=table_parameters,
        )

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().build()

        stored_events = adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[new_event],
        )

        retrieved_events = read_events(pool=connection_pool, table=table_name)

        assert retrieved_events == stored_events


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
