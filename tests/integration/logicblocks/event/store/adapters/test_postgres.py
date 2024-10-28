import unittest
from datetime import datetime

from psycopg import Connection
from psycopg_pool import ConnectionPool

from logicblocks.event.store.adapters import PostgresStorageAdapter

from logicblocks.event.testing import NewEventBuilder

from logicblocks.event.testing.data import (
    random_event_name,
    random_event_payload,
    random_event_stream_name,
    random_event_category_name,
)

from logicblocks.event.store import conditions

from logicblocks.event.store.exceptions import UnmetWriteConditionError


def conninfo(
    username="admin",
    password="super-secret",
    host="localhost",
    port="5432",
    database="some-database",
):
    return f"postgresql://{username}:{password}@{host}:{port}/{database}"


class TestPostgresStorageAdapter(unittest.TestCase):
    pool: ConnectionPool[Connection]

    def setUp(self):
        pool = ConnectionPool[Connection](conninfo(), open=True)
        with pool.connection() as connection:
            connection.execute("TRUNCATE events")
        self.pool = pool

    def tearDown(self):
        self.pool.close()

    def test_stores_single_event_for_later_retrieval(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_name = random_event_name()
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()
        event_payload = random_event_payload()
        event_observed_at = datetime.now()
        event_occurred_at = datetime.now()

        new_event = (
            NewEventBuilder()
            .with_name(event_name)
            .with_payload(event_payload)
            .with_observed_at(event_observed_at)
            .with_occurred_at(event_occurred_at)
            .build()
        )

        stored_events = adapter.save(
            category=event_category, stream=event_stream, events=[new_event]
        )
        stored_event = stored_events[0]

        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM events")
                records = cursor.fetchall()

                self.assertEqual(
                    records,
                    [
                        (
                            stored_event.id,
                            event_name,
                            event_stream,
                            event_category,
                            0,
                            event_payload,
                            event_observed_at,
                            event_occurred_at,
                        )
                    ],
                )

    def test_stores_multiple_events_in_same_stream(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_name_1 = random_event_name()
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()
        event_payload_1 = random_event_payload()
        event_observed_at_1 = datetime.now()
        event_occurred_at_1 = datetime.now()

        new_event_1 = (
            NewEventBuilder()
            .with_name(event_name_1)
            .with_payload(event_payload_1)
            .with_observed_at(event_observed_at_1)
            .with_occurred_at(event_occurred_at_1)
            .build()
        )

        event_name_2 = random_event_name()
        event_payload_2 = random_event_payload()
        event_observed_at_2 = datetime.now()
        event_occurred_at_2 = datetime.now()

        new_event_2 = (
            NewEventBuilder()
            .with_name(event_name_2)
            .with_payload(event_payload_2)
            .with_observed_at(event_observed_at_2)
            .with_occurred_at(event_occurred_at_2)
            .build()
        )

        stored_events = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event_1, new_event_2],
        )
        stored_event_1 = stored_events[0]
        stored_event_2 = stored_events[1]

        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM events")
                records = cursor.fetchall()

                self.assertEqual(
                    records,
                    [
                        (
                            stored_event_1.id,
                            event_name_1,
                            event_stream,
                            event_category,
                            0,
                            event_payload_1,
                            event_observed_at_1,
                            event_occurred_at_1,
                        ),
                        (
                            stored_event_2.id,
                            event_name_2,
                            event_stream,
                            event_category,
                            1,
                            event_payload_2,
                            event_observed_at_2,
                            event_occurred_at_2,
                        ),
                    ],
                )

    def test_stores_multiple_events_in_sequential_saves(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_name_1 = random_event_name()
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()
        event_payload_1 = random_event_payload()
        event_observed_at_1 = datetime.now()
        event_occurred_at_1 = datetime.now()

        new_event_1 = (
            NewEventBuilder()
            .with_name(event_name_1)
            .with_payload(event_payload_1)
            .with_observed_at(event_observed_at_1)
            .with_occurred_at(event_occurred_at_1)
            .build()
        )

        event_name_2 = random_event_name()
        event_payload_2 = random_event_payload()
        event_observed_at_2 = datetime.now()
        event_occurred_at_2 = datetime.now()

        new_event_2 = (
            NewEventBuilder()
            .with_name(event_name_2)
            .with_payload(event_payload_2)
            .with_observed_at(event_observed_at_2)
            .with_occurred_at(event_occurred_at_2)
            .build()
        )

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

        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM events")
                records = cursor.fetchall()

                self.assertEqual(
                    records,
                    [
                        (
                            stored_event_1.id,
                            event_name_1,
                            event_stream,
                            event_category,
                            0,
                            event_payload_1,
                            event_observed_at_1,
                            event_occurred_at_1,
                        ),
                        (
                            stored_event_2.id,
                            event_name_2,
                            event_stream,
                            event_category,
                            1,
                            event_payload_2,
                            event_observed_at_2,
                            event_occurred_at_2,
                        ),
                    ],
                )

    def test_stores_event_with_empty_stream_condition(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_name = random_event_name()
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()
        event_payload = random_event_payload()
        event_observed_at = datetime.now()
        event_occurred_at = datetime.now()

        new_event = (
            NewEventBuilder()
            .with_name(event_name)
            .with_payload(event_payload)
            .with_observed_at(event_observed_at)
            .with_occurred_at(event_occurred_at)
            .build()
        )

        stored_events = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event],
            conditions={conditions.stream_is_empty()},
        )
        stored_event = stored_events[0]

        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM events")
                records = cursor.fetchall()

                self.assertEqual(
                    records,
                    [
                        (
                            stored_event.id,
                            event_name,
                            event_stream,
                            event_category,
                            0,
                            event_payload,
                            event_observed_at,
                            event_occurred_at,
                        )
                    ],
                )

    def test_raises_if_unmet_stream_is_empty_condition(
        self,
    ):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        adapter.save(
            category=random_event_category_name(),
            stream=random_event_stream_name(),
            events=[NewEventBuilder().build()],
        )
        with self.assertRaises(UnmetWriteConditionError):
            adapter.save(
                category=random_event_category_name(),
                stream=random_event_stream_name(),
                events=[NewEventBuilder().build()],
                conditions={conditions.stream_is_empty()},
            )

    def test_stores_event_with_position_condition(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_name_1 = random_event_name()
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()
        event_payload_1 = random_event_payload()
        event_observed_at_1 = datetime.now()
        event_occurred_at_1 = datetime.now()

        new_event_1 = (
            NewEventBuilder()
            .with_name(event_name_1)
            .with_payload(event_payload_1)
            .with_observed_at(event_observed_at_1)
            .with_occurred_at(event_occurred_at_1)
            .build()
        )

        event_name_2 = random_event_name()
        event_payload_2 = random_event_payload()
        event_observed_at_2 = datetime.now()
        event_occurred_at_2 = datetime.now()

        new_event_2 = (
            NewEventBuilder()
            .with_name(event_name_2)
            .with_payload(event_payload_2)
            .with_observed_at(event_observed_at_2)
            .with_occurred_at(event_occurred_at_2)
            .build()
        )

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

        with self.pool.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM events")
                records = cursor.fetchall()

                self.assertEqual(
                    records,
                    [
                        (
                            stored_event_1.id,
                            event_name_1,
                            event_stream,
                            event_category,
                            0,
                            event_payload_1,
                            event_observed_at_1,
                            event_occurred_at_1,
                        ),
                        (
                            stored_event_2.id,
                            event_name_2,
                            event_stream,
                            event_category,
                            1,
                            event_payload_2,
                            event_observed_at_2,
                            event_occurred_at_2,
                        ),
                    ],
                )

    def test_raises_if_unmet_position_condition(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        adapter.save(
            category=random_event_category_name(),
            stream=random_event_stream_name(),
            events=[NewEventBuilder().build()],
        )

        with self.assertRaises(UnmetWriteConditionError):
            adapter.save(
                category=random_event_category_name(),
                stream=random_event_stream_name(),
                events=[NewEventBuilder().build()],
                conditions={conditions.position_is(1)},
            )

    def test_has_no_events_initially(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        events = list(adapter.scan_all())

        self.assertEqual(events, [])

    def test_reads_single_published_event(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        stored_events = adapter.save(
            category=random_event_category_name(),
            stream=random_event_stream_name(),
            events=[NewEventBuilder().build()],
        )

        loaded_events = list(adapter.scan_all())

        self.assertEqual(stored_events, loaded_events)

    def test_reads_multiple_published_events(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        stored_events = adapter.save(
            category=random_event_category_name(),
            stream=random_event_stream_name(),
            events=[NewEventBuilder().build(), NewEventBuilder().build()],
        )

        loaded_events = list(adapter.scan_all())

        self.assertEqual(stored_events, loaded_events)

    def test_has_no_events_in_category_initially(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        events = list(
            adapter.scan_category(category=random_event_category_name())
        )

        self.assertEqual(events, [])

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

        self.assertEqual(stored_events_in_category, loaded_events)

    def test_has_no_events_in_stream_initially(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        events = list(
            adapter.scan_stream(
                category=random_event_category_name(),
                stream=random_event_stream_name(),
            )
        )

        self.assertEqual(events, [])

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

        self.assertEqual(stored_events_in_category, loaded_events)


if __name__ == "__main__":
    unittest.main()
