import sys
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Sequence, cast
from unittest.mock import AsyncMock

import pytest
from psycopg import AsyncConnection, sql
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import (
    ConnectionSettings,
    TableSettings,
)
from logicblocks.event.store import (
    PostgresEventStorageAdapter,
)
from logicblocks.event.store.adapters.postgres.adapter import (
    StreamInsertDefinition,
    insert_batch,
    insert_batch_query,
)
from logicblocks.event.testing import NewEventBuilder, StoredEventBuilder
from logicblocks.event.types import (
    NewEvent,
    StreamIdentifier,
)

connection_settings = ConnectionSettings(
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


class TestBatchInsert:
    async def test_batch_insert_with_multiple_events(self):
        target = StreamIdentifier(
            category="test-category", stream="test-stream"
        )
        now = datetime.now(timezone.utc)
        events = [
            NewEventBuilder(
                name="event1",
                payload={"data": "value1"},
                occurred_at=now,
                observed_at=now,
            ).build(),
            NewEventBuilder(
                name="event2",
                payload={"data": "value2"},
                occurred_at=now,
                observed_at=now,
            ).build(),
            NewEventBuilder(
                name="event3",
                payload={"data": "value3"},
                occurred_at=now,
                observed_at=now,
            ).build(),
        ]

        cursor = AsyncMock()
        stub_stored_events = [
            StoredEventBuilder(
                id=f"id{0}",
                name=f"event{0}",
                stream="test-stream",
                category="test-category",
                position=10,
                sequence_number=100,
                payload={"data": f"value{0}"},
                observed_at=now,
                occurred_at=now,
            ).build(),
            StoredEventBuilder(
                id=f"id{1}",
                name=f"event{1}",
                stream="test-stream",
                category="test-category",
                position=10,
                sequence_number=100,
                payload={"data": f"value{1}"},
                observed_at=now,
                occurred_at=now,
            ).build(),
            StoredEventBuilder(
                id=f"id{2}",
                name=f"event{2}",
                stream="test-stream",
                category="test-category",
                position=10,
                sequence_number=100,
                payload={"data": f"value{2}"},
                observed_at=now,
                occurred_at=now,
            ).build(),
        ]
        cursor.fetchall.return_value = stub_stored_events

        definitions = {
            target: StreamInsertDefinition(events=events, position=10)
        }

        returned_events_mapping = await insert_batch(
            cursor,
            definitions=definitions,
            table_settings=TableSettings(table_name="test_events"),
        )

        returned_events = returned_events_mapping[target]

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

        definitions = {
            target: StreamInsertDefinition(events=empty_events, position=10)
        }

        result = await insert_batch(
            cursor,
            definitions=definitions,
            table_settings=TableSettings(table_name="test_events"),
        )

        assert result == {target: []}

    def test_batch_insert_query_builds_correct_sql(self):
        target = StreamIdentifier(
            category="test-category", stream="test-stream"
        )
        fixed_timestamp = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

        events: Sequence[NewEvent[str, Mapping[str, str]]] = [
            NewEventBuilder(
                name="event1",
                payload={"data": "value1"},
                occurred_at=fixed_timestamp,
                observed_at=fixed_timestamp,
            ).build(),
            NewEventBuilder(
                name="event2",
                payload={"data": "value2"},
                occurred_at=fixed_timestamp,
                observed_at=fixed_timestamp,
            ).build(),
        ]
        query, params = insert_batch_query(
            definitions={
                target: StreamInsertDefinition(events=events, position=10)
            },
            table_settings=TableSettings(table_name="test_events"),
        )
        # Convert the query to a real SQL string
        query = cast(sql.SQL, query)
        query_str = query.as_string(None)

        expected_query_str = """
        INSERT INTO "test_events" 
        (id, name, stream, category, position, 
         payload, observed_at, occurred_at)
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
