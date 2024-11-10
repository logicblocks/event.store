import sys

import pytest

from psycopg import Connection
from psycopg.rows import class_row
from psycopg_pool import ConnectionPool

from logicblocks.event.store import conditions
from logicblocks.event.store.adapters import PostgresStorageAdapter
from logicblocks.event.store.exceptions import UnmetWriteConditionError

from logicblocks.event.testing import NewEventBuilder
from logicblocks.event.testing.data import (
    random_event_stream_name,
    random_event_category_name,
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


def read_events_table(
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


class TestBase(object):
    pool: ConnectionPool[Connection]

    def setup_method(self):
        pool = ConnectionPool[Connection](conninfo(), open=True)
        with pool.connection() as connection:
            connection.execute("TRUNCATE events")
        self.pool = pool

    def teardown_method(self):
        self.pool.close()


class TestSave(TestBase):
    def test_stores_single_event_for_later_retrieval(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()
        new_event = NewEventBuilder().build()

        stored_events = adapter.save(
            category=event_category, stream=event_stream, events=[new_event]
        )
        stored_event = stored_events[0]

        actual_events = read_events_table(self.pool)
        expected_events = [
            StoredEvent(
                id=stored_event.id,
                name=new_event.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event.payload,
                observed_at=new_event.observed_at,
                occurred_at=new_event.occurred_at,
            )
        ]

        assert actual_events == expected_events

    def test_stores_multiple_events_in_same_stream(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()
        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        stored_events = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event_1, new_event_2],
        )
        stored_event_1 = stored_events[0]
        stored_event_2 = stored_events[1]

        actual_records = read_events_table(self.pool)
        expected_records = [
            StoredEvent(
                id=stored_event_1.id,
                name=new_event_1.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event_1.sequence_number,
                payload=new_event_1.payload,
                observed_at=new_event_1.observed_at,
                occurred_at=new_event_1.occurred_at,
            ),
            StoredEvent(
                id=stored_event_2.id,
                name=new_event_2.name,
                category=event_category,
                stream=event_stream,
                position=1,
                sequence_number=stored_event_2.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            ),
        ]

        assert actual_records == expected_records

    def test_stores_multiple_events_in_sequential_saves(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        stored_events_1 = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event_1],
        )
        stored_event_1 = stored_events_1[0]

        stored_events_2 = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event_2],
        )
        stored_event_2 = stored_events_2[0]

        actual_records = read_events_table(self.pool)
        expected_records = [
            StoredEvent(
                id=stored_event_1.id,
                name=new_event_1.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event_1.sequence_number,
                payload=new_event_1.payload,
                observed_at=new_event_1.observed_at,
                occurred_at=new_event_1.occurred_at,
            ),
            StoredEvent(
                id=stored_event_2.id,
                name=new_event_2.name,
                category=event_category,
                stream=event_stream,
                position=1,
                sequence_number=stored_event_2.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            ),
        ]

        assert actual_records == expected_records


class TestWriteConditions(TestBase):
    def test_writes_when_empty_stream_condition_and_stream_empty(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().build()

        stored_events = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event],
            conditions={conditions.stream_is_empty()},
        )
        stored_event = stored_events[0]

        actual_records = read_events_table(
            self.pool, category=event_category, stream=event_stream
        )
        expected_records = [
            StoredEvent(
                id=stored_event.id,
                name=new_event.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event.payload,
                observed_at=new_event.observed_at,
                occurred_at=new_event.occurred_at,
            )
        ]

        assert actual_records == expected_records

    def test_writes_when_empty_stream_condition_and_category_not_empty(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_category = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        adapter.save(
            category=event_category,
            stream=event_stream_1,
            events=[new_event_1],
        )

        stored_events = adapter.save(
            category=event_category,
            stream=event_stream_2,
            events=[new_event_2],
            conditions={conditions.stream_is_empty()},
        )
        stored_event = stored_events[0]

        actual_records = read_events_table(
            self.pool, category=event_category, stream=event_stream_2
        )
        expected_records = [
            StoredEvent(
                id=stored_event.id,
                name=new_event_2.name,
                category=event_category,
                stream=event_stream_2,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            )
        ]

        assert actual_records == expected_records

    def test_writes_when_empty_stream_condition_and_log_not_empty(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_category_1 = random_event_category_name()
        event_category_2 = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        adapter.save(
            category=event_category_1,
            stream=event_stream_1,
            events=[new_event_1],
        )

        stored_events = adapter.save(
            category=event_category_2,
            stream=event_stream_2,
            events=[new_event_2],
            conditions={conditions.stream_is_empty()},
        )
        stored_event = stored_events[0]

        actual_records = read_events_table(
            self.pool, category=event_category_2, stream=event_stream_2
        )
        expected_records = [
            StoredEvent(
                id=stored_event.id,
                name=new_event_2.name,
                category=event_category_2,
                stream=event_stream_2,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            )
        ]

        assert actual_records == expected_records

    def test_raises_when_empty_stream_condition_and_stream_not_empty(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        adapter.save(
            category=event_category,
            stream=event_stream,
            events=[NewEventBuilder().build()],
        )

        with pytest.raises(UnmetWriteConditionError):
            adapter.save(
                category=event_category,
                stream=event_stream,
                events=[NewEventBuilder().build()],
                conditions={conditions.stream_is_empty()},
            )

    def test_stores_event_with_position_condition(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()
        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        stored_events_1 = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event_1],
        )
        stored_event_1 = stored_events_1[0]

        stored_events_2 = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event_2],
            conditions={conditions.position_is(0)},
        )

        stored_event_2 = stored_events_2[0]

        actual_records = read_events_table(self.pool)
        expected_records = [
            StoredEvent(
                id=stored_event_1.id,
                name=new_event_1.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event_1.sequence_number,
                payload=new_event_1.payload,
                observed_at=new_event_1.observed_at,
                occurred_at=new_event_1.occurred_at,
            ),
            StoredEvent(
                id=stored_event_2.id,
                name=new_event_2.name,
                category=event_category,
                stream=event_stream,
                position=1,
                sequence_number=stored_event_2.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            ),
        ]

        assert actual_records == expected_records

    def test_raises_if_unmet_position_condition(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        adapter.save(
            category=random_event_category_name(),
            stream=random_event_stream_name(),
            events=[NewEventBuilder().build()],
        )

        with pytest.raises(UnmetWriteConditionError):
            adapter.save(
                category=random_event_category_name(),
                stream=random_event_stream_name(),
                events=[NewEventBuilder().build()],
                conditions={conditions.position_is(1)},
            )


class TestScanLog(TestBase):
    def test_has_no_events_initially(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        events = list(adapter.scan_all())

        assert events == []

    def test_reads_single_published_event(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        stored_events = adapter.save(
            category=random_event_category_name(),
            stream=random_event_stream_name(),
            events=[NewEventBuilder().build()],
        )

        loaded_events = list(adapter.scan_all())

        assert stored_events == loaded_events

    def test_reads_multiple_published_events(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        stored_events = adapter.save(
            category=random_event_category_name(),
            stream=random_event_stream_name(),
            events=[NewEventBuilder().build(), NewEventBuilder().build()],
        )

        loaded_events = list(adapter.scan_all())

        assert stored_events == loaded_events


class TestScanCategory(TestBase):
    def test_has_no_events_in_category_initially(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        events = list(
            adapter.scan_category(category=random_event_category_name())
        )

        assert events == []

    def test_reads_multiple_published_events_by_category(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        category = random_event_category_name()

        event_to_find_1 = NewEventBuilder().build()
        event_to_find_2 = NewEventBuilder().build()

        stored_events_in_category = adapter.save(
            category=category,
            stream=random_event_stream_name(),
            events=[event_to_find_1, event_to_find_2],
        )

        adapter.save(
            category=random_event_category_name(),
            stream=random_event_stream_name(),
            events=[NewEventBuilder().build()],
        )

        loaded_events = list(adapter.scan_category(category=category))

        assert stored_events_in_category == loaded_events


class TestScanStream(TestBase):
    def test_has_no_events_in_stream_initially(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        events = list(
            adapter.scan_stream(
                category=random_event_category_name(),
                stream=random_event_stream_name(),
            )
        )

        assert events == []

    def test_reads_multiple_published_events_by_stream(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        category = random_event_category_name()
        stream = random_event_stream_name()

        event_to_find_1 = NewEventBuilder().build()
        event_to_find_2 = NewEventBuilder().build()

        stored_events_in_category = adapter.save(
            category=category,
            stream=stream,
            events=[event_to_find_1, event_to_find_2],
        )

        adapter.save(
            category=random_event_category_name(),
            stream=random_event_stream_name(),
            events=[NewEventBuilder().build()],
        )

        adapter.save(
            category=category,
            stream=random_event_stream_name(),
            events=[NewEventBuilder().build()],
        )

        loaded_events = list(
            adapter.scan_stream(category=category, stream=stream)
        )

        assert stored_events_in_category == loaded_events


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
