import sys

import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.store.adapters import (
    PostgresConnectionSettings,
    PostgresEventStorageAdapter,
)

connection_settings = PostgresConnectionSettings(
    host="fake",
    port=1234,
    dbname="db",
    user="user",
    password="supersecret",
)


class TestPostgresConnectionSettings:
    def test_includes_all_settings_in_representation_obscuring_password(self):
        settings = PostgresConnectionSettings(
            host="localhost",
            port=5432,
            dbname="event_store",
            user="user",
            password="supersecret",
        )

        assert repr(settings) == (
            "ConnectionSettings("
            "host=localhost, "
            "port=5432, "
            "dbname=event_store, "
            "user=user, "
            "password=***********"
            ")"
        )

    def test_generates_connection_string_from_parameters(self):
        settings = PostgresConnectionSettings(
            host="localhost",
            port=5432,
            dbname="event_store",
            user="user",
            password="supersecret",
        )

        assert (
            settings.to_connection_string()
            == "postgresql://user:supersecret@localhost:5432/event_store"
        )


class TestPostgresStorageAdapter:
    def test_creates_connection_pool_when_connection_settings_provided(self):
        adapter = PostgresEventStorageAdapter(
            connection_source=connection_settings
        )

        assert adapter.connection_pool is not None

    def test_uses_supplied_connection_pool_when_provided(self):
        connection_pool = AsyncConnectionPool[AsyncConnection](
            connection_settings.to_connection_string(), open=False
        )

        adapter = PostgresEventStorageAdapter(
            connection_source=connection_pool
        )

        assert adapter.connection_pool is connection_pool

    async def test_opens_connection_pool_when_connection_pool_not_provided(
        self, monkeypatch
    ):
        opened = False

        async def mock_open(_):
            nonlocal opened
            opened = True

        monkeypatch.setattr(AsyncConnectionPool, "open", mock_open)

        adapter = PostgresEventStorageAdapter(
            connection_source=connection_settings
        )
        await adapter.open()

        assert opened

    async def test_does_not_open_connection_pool_when_connection_pool_provided(
        self, monkeypatch
    ):
        opened = False

        async def mock_open(_):
            nonlocal opened
            opened = True

        monkeypatch.setattr(AsyncConnectionPool, "open", mock_open)

        connection_pool = AsyncConnectionPool[AsyncConnection](
            connection_settings.to_connection_string(), open=False
        )
        adapter = PostgresEventStorageAdapter(
            connection_source=connection_pool
        )
        await adapter.open()

        assert not opened

    async def test_closes_connection_pool_when_connection_pool_not_provided(
        self, monkeypatch
    ):
        closed = False

        async def mock_close(_):
            nonlocal closed
            closed = True

        monkeypatch.setattr(AsyncConnectionPool, "close", mock_close)

        adapter = PostgresEventStorageAdapter(
            connection_source=connection_settings
        )
        await adapter.close()

        assert closed

    async def test_does_not_close_connection_pool_when_connection_pool_provided(
        self, monkeypatch
    ):
        closed = False

        async def mock_close(_):
            nonlocal closed
            closed = True

        monkeypatch.setattr(AsyncConnectionPool, "close", mock_close)

        connection_pool = AsyncConnectionPool[AsyncConnection](
            connection_settings.to_connection_string(), open=False
        )
        adapter = PostgresEventStorageAdapter(
            connection_source=connection_pool
        )

        await adapter.close()

        assert not closed


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
