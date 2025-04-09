import sys
from datetime import datetime, timezone
from typing import cast
from unittest.mock import AsyncMock

import pytest
from psycopg import AsyncConnection, sql
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db import PostgresConnectionSettings
from logicblocks.event.store.adapters import (
    PostgresEventStorageAdapter,
)
from logicblocks.event.store.adapters.postgres import (
    StoredEvent,
    TableSettings,
    insert_batch,
    insert_batch_query,
)
from logicblocks.event.types import (
    NewEvent,
    StreamIdentifier,
)

connection_settings = PostgresConnectionSettings(
    host="fake",
    port=1234,
    dbname="db",
    user="user",
    password="supersecret",
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


def normalize_whitespace(text: str) -> str:
    return " ".join(text.split())


def event(fixed_timestamp, name="event", value="value"):
    return NewEvent(
        name=name,
        payload={"data": value},
        observed_at=fixed_timestamp,
        occurred_at=fixed_timestamp,
    )


def stored_event(fixed_timestamp, i):
    return StoredEvent(
        id=f"id{i}",
        name=f"event{i}",
        stream="test-stream",
        category="test-category",
        position=10,
        sequence_number=100,
        payload={"data": f"value{i}"},
        observed_at=fixed_timestamp,
        occurred_at=fixed_timestamp,
    )


class TestBatchInsert:
    async def test_batch_insert_with_multiple_events(self):
        target = StreamIdentifier(
            category="test-category", stream="test-stream"
        )
        now = datetime.now(timezone.utc)
        events = [
            event(now, name="event1", value="value1"),
            event(now, name="event2", value="value2"),
            event(now, name="event3", value="value3"),
        ]

        cursor = AsyncMock()
        stub_stored_events = [
            stored_event(now, 0),
            stored_event(now, 1),
            stored_event(now, 2),
        ]
        cursor.fetchall.return_value = stub_stored_events

        returned_events = await insert_batch(
            cursor,
            target=target,
            events=events,
            start_position=10,
            table_settings=TableSettings(events_table_name="test_events"),
        )

        assert len(returned_events) == 3
        assert returned_events[0].name == "event1"
        assert returned_events[0].stream == "test-stream"
        assert returned_events[0].category == "test-category"
        assert returned_events[0].position == 10
        assert returned_events[0].sequence_number == 100
        assert returned_events[0].payload == {"data": "value1"}
        assert returned_events[1].name == "event2"
        assert returned_events[2].name == "event3"

    async def test_batch_insert_with_empty_events(self):
        cursor = AsyncMock()
        target = StreamIdentifier(
            category="test-category", stream="test-stream"
        )

        empty_events = []

        result = await insert_batch(
            cursor,
            target=target,
            events=empty_events,
            start_position=10,
            table_settings=TableSettings(events_table_name="test_events"),
        )

        assert result == []

    def test_batch_insert_query_builds_correct_sql(self):
        target = StreamIdentifier(
            category="test-category", stream="test-stream"
        )
        fixed_timestamp = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        events = [
            event(fixed_timestamp, name="event1", value="value1"),
            event(fixed_timestamp, name="event2", value="value2"),
        ]
        query, params = insert_batch_query(
            target=target,
            events=events,
            start_position=10,
            table_settings=TableSettings(events_table_name="test_events"),
        )
        # Convert the query to a real SQL string
        query = cast(sql.SQL, query)
        query_str = query.as_string(None)

        expected_query_str = """
         INSERT INTO "test_events" (
           id, 
           name, 
           stream, 
           category, 
           position, 
           payload, 
           observed_at, 
           occurred_at
         )
         VALUES
          (%s, %s, %s, %s, %s, %s, %s, %s), (%s, %s, %s, %s, %s, %s, %s, %s)
           RETURNING *;
        """
        assert normalize_whitespace(query_str) == normalize_whitespace(
            expected_query_str
        )

        # Make sure it has a placeholder for each event
        assert query_str.count("(%s, %s, %s, %s, %s, %s, %s, %s)") == 2

        assert len(params) == 16
        assert len(params[0]) == 32  # id is a UUID
        assert params[1] == "event1"
        assert params[2] == "test-stream"
        assert params[3] == "test-category"
        assert params[4] == 10
        assert params[5].obj == {"data": "value1"}
        assert params[6] == fixed_timestamp
        assert params[7] == fixed_timestamp

        assert len(params[8]) == 32  # id is a UUID
        assert params[9] == "event2"
        assert params[10] == "test-stream"
        assert params[11] == "test-category"
        assert params[12] == 11
        assert params[13].obj == {"data": "value2"}
        assert params[14] == fixed_timestamp
        assert params[15] == fixed_timestamp


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
