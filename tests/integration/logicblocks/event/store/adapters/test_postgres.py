import sys
import pytest

from collections.abc import Sequence

from psycopg import Connection
from psycopg.rows import class_row
from psycopg_pool import ConnectionPool

from logicblocks.event.adaptertests import cases
from logicblocks.event.adaptertests.cases import ConcurrencyParameters
from logicblocks.event.store.adapters import (
    PostgresStorageAdapter,
    StorageAdapter,
)

from logicblocks.event.types import StoredEvent


def conninfo(
    username: str = "admin",
    password: str = "super-secret",
    host: str = "localhost",
    port: str = "5432",
    database: str = "some-database",
) -> str:
    return f"postgresql://{username}:{password}@{host}:{port}/{database}"


def read_events_from_table(
    pool: ConnectionPool[Connection],
    category: str | None = None,
    stream: str | None = None,
) -> list[StoredEvent]:
    with pool.connection() as connection:
        with connection.cursor(row_factory=class_row(StoredEvent)) as cursor:
            events = cursor.execute(
                """
                SELECT *
                FROM events
                ORDER BY sequence_number;
                """
            ).fetchall()
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
    _pool: ConnectionPool[Connection]

    def setup_method(self):
        self._pool = ConnectionPool[Connection](conninfo(), open=True)
        self.reset_storage()

    def teardown_method(self):
        self._pool.close()

    @property
    def concurrency_parameters(self):
        return ConcurrencyParameters(concurrent_writes=3, repeats=5)

    def reset_storage(self) -> None:
        with self._pool.connection() as connection:
            connection.execute("TRUNCATE events")

    def construct_storage_adapter(self) -> StorageAdapter:
        return PostgresStorageAdapter(connection_pool=self._pool)

    def retrieve_events(
        self,
        adapter: StorageAdapter,
        category: str | None = None,
        stream: str | None = None,
    ) -> Sequence[StoredEvent]:
        return read_events_from_table(
            self._pool, category=category, stream=stream
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
