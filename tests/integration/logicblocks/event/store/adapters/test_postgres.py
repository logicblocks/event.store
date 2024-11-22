import sys
import pytest

from collections.abc import Sequence

from psycopg import Connection
from psycopg.rows import class_row
from psycopg_pool import ConnectionPool

from logicblocks.event.adaptertests import cases
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
    _adapter: StorageAdapter

    def setup_method(self):
        pool = ConnectionPool[Connection](conninfo(), open=True)
        with pool.connection() as connection:
            connection.execute("TRUNCATE events")
        self._pool = pool
        self._adapter = PostgresStorageAdapter(connection_pool=pool)

    def teardown_method(self):
        self._pool.close()

    @property
    def adapter(self) -> StorageAdapter:
        return self._adapter

    def retrieve_events(
        self, category: str | None = None, stream: str | None = None
    ) -> Sequence[StoredEvent]:
        return read_events_from_table(
            self._pool, category=category, stream=stream
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
