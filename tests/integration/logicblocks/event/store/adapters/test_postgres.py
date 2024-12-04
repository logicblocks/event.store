import sys
import pytest

from collections.abc import Sequence

from psycopg import Connection, sql, abc
from psycopg.rows import class_row
from psycopg_pool import ConnectionPool

from logicblocks.event.adaptertests import cases
from logicblocks.event.adaptertests.cases import ConcurrencyParameters
from logicblocks.event.store.adapters import (
    PostgresConnectionParameters,
    PostgresTableParameters,
    PostgresStorageAdapter,
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


def reset_table(pool: ConnectionPool[Connection], table: str) -> None:
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


class TestPostgresStorageAdapter(cases.StorageAdapterCases):
    pool: ConnectionPool[Connection]

    def setup_method(self):
        self.pool = ConnectionPool[Connection](
            connection_parameters.to_connection_string(), open=True
        )
        self.destroy_storage()
        self.initialise_storage()

    def teardown_method(self):
        self.pool.close()

    @property
    def concurrency_parameters(self):
        return ConcurrencyParameters(concurrent_writes=3, repeats=5)

    def initialise_storage(
        self,
        table_parameters: PostgresTableParameters = PostgresTableParameters(),
    ) -> None:
        create_table(self.pool, table_parameters.events_table_name)

    def reset_storage(
        self,
        table_parameters: PostgresTableParameters = PostgresTableParameters(),
    ) -> None:
        reset_table(self.pool, table_parameters.events_table_name)

    def destroy_storage(
        self,
        table_parameters: PostgresTableParameters = PostgresTableParameters(),
    ) -> None:
        drop_table(self.pool, table_parameters.events_table_name)

    def construct_storage_adapter(
        self,
        table_parameters: PostgresTableParameters = PostgresTableParameters(),
    ) -> StorageAdapter:
        return PostgresStorageAdapter(
            connection_source=self.pool, table_parameters=table_parameters
        )

    def retrieve_events(
        self,
        *,
        adapter: StorageAdapter,
        table_parameters: PostgresTableParameters = PostgresTableParameters(),
        category: str | None = None,
        stream: str | None = None,
    ) -> Sequence[StoredEvent]:
        return read_events(
            self.pool,
            table=table_parameters.events_table_name,
            category=category,
            stream=stream,
        )

    def test_allows_events_table_name_to_be_overridden(self):
        table_parameters = PostgresTableParameters(
            events_table_name="event_log"
        )

        try:
            self.initialise_storage(table_parameters=table_parameters)

            adapter = self.construct_storage_adapter(
                table_parameters=table_parameters
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

            retrieved_events = self.retrieve_events(
                adapter=adapter, table_parameters=table_parameters
            )

            assert retrieved_events == stored_events
        finally:
            self.destroy_storage(table_parameters=table_parameters)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
