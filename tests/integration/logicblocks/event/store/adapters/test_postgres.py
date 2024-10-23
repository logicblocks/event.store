import unittest
from datetime import datetime

from psycopg import Connection
from psycopg_pool import ConnectionPool

from logicblocks.event.store.types import NewEvent
from logicblocks.event.store.adapters import PostgresStorageAdapter


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

        event_name = "something-happened"
        event_category = "some-category"
        event_stream = "some-stream"
        event_payload = {"foo": "bar"}
        event_observed_at = datetime.now()
        event_occurred_at = datetime.now()

        new_event = NewEvent(
            name=event_name,
            payload=event_payload,
            observed_at=event_observed_at,
            occurred_at=event_occurred_at,
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


if __name__ == "__main__":
    unittest.main()
