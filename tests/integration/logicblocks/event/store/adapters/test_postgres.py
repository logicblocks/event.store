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

    def test_stores_multiple_events_for_later_retrieval(self):
        adapter = PostgresStorageAdapter(connection_pool=self.pool)

        event_name_1 = random_event_name()
        event_category_1 = random_event_category_name()
        event_stream_1 = random_event_stream_name()
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

        stored_events_1 = adapter.save(
            category=event_category_1,
            stream=event_stream_1,
            events=[new_event_1],
        )
        stored_event_1 = stored_events_1[0]

        event_name_2 = random_event_name()
        event_category_2 = random_event_category_name()
        event_stream_2 = random_event_stream_name()
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

        stored_events_2 = adapter.save(
            category=event_category_2,
            stream=event_stream_2,
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
                            event_stream_1,
                            event_category_1,
                            0,
                            event_payload_1,
                            event_observed_at_1,
                            event_occurred_at_1,
                        ),
                        (
                            stored_event_2.id,
                            event_name_2,
                            event_stream_2,
                            event_category_2,
                            0,
                            event_payload_2,
                            event_observed_at_2,
                            event_occurred_at_2,
                        ),
                    ],
                )


if __name__ == "__main__":
    unittest.main()
