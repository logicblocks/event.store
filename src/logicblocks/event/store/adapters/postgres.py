from collections.abc import Set
from typing import Sequence, Iterator
from uuid import uuid4

from psycopg import Connection, Cursor
from psycopg.rows import class_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from logicblocks.event.store.adapters import StorageAdapter
from logicblocks.event.store.conditions import WriteCondition
from logicblocks.event.types import NewEvent, StoredEvent


def _insert(
    cursor: Cursor[StoredEvent],
    stream: str,
    category: str,
    event: NewEvent,
    position: int,
):
    event_id = uuid4().hex
    cursor.execute(
        """
        INSERT INTO events (
          id, 
          name, 
          stream, 
          category, 
          position, 
          payload, 
          observed_at, 
          occurred_at
      )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
        """,
        (
            event_id,
            event.name,
            stream,
            category,
            position,
            Jsonb(event.payload),
            event.observed_at,
            event.occurred_at,
        ),
    )
    stored_event = cursor.fetchone()

    if not stored_event:
        raise Exception("Insert failed")
    return stored_event


def _read_last(
    cursor: Cursor[StoredEvent],
    *,
    category: str,
    stream: str,
):
    return cursor.execute(
        """
            SELECT * 
            FROM events
            WHERE category = (%s)
            AND stream = (%s)
            ORDER BY position DESC 
            LIMIT 1;
            """,
        [category, stream],
    ).fetchone()


class PostgresStorageAdapter(StorageAdapter):
    def __init__(self, *, connection_pool: ConnectionPool[Connection]):
        self.connection_pool = connection_pool

    def save(
        self,
        *,
        category: str,
        stream: str,
        events: Sequence[NewEvent],
        conditions: Set[WriteCondition] = frozenset(),
    ) -> Sequence[StoredEvent]:
        with self.connection_pool.connection() as connection:
            with connection.cursor(
                row_factory=class_row(StoredEvent)
            ) as cursor:
                last_event = _read_last(
                    cursor, category=category, stream=stream
                )

                for condition in conditions:
                    condition.evaluate(last_event)

                current_position = last_event.position + 1 if last_event else 0

                return [
                    _insert(cursor, stream, category, event, position)
                    for position, event in enumerate(events, current_position)
                ]

    def scan_stream(
        self, *, category: str, stream: str
    ) -> Iterator[StoredEvent]:
        with self.connection_pool.connection() as connection:
            with connection.cursor(
                row_factory=class_row(StoredEvent)
            ) as cursor:
                for record in cursor.execute(
                    """
                        SELECT *
                        FROM events
                        WHERE category = (%s)
                        AND stream = (%s)
                        ORDER BY position;
                        """,
                    [category, stream],
                ):
                    yield record

    def scan_category(self, *, category: str) -> Iterator[StoredEvent]:
        with self.connection_pool.connection() as connection:
            with connection.cursor(
                row_factory=class_row(StoredEvent)
            ) as cursor:
                for record in cursor.execute(
                    """
                        SELECT *  
                        FROM events
                        WHERE category = (%s)
                        ORDER BY position;
                        """,
                    [category],
                ):
                    yield record

    def scan_all(self) -> Iterator[StoredEvent]:
        with self.connection_pool.connection() as connection:
            with connection.cursor(
                row_factory=class_row(StoredEvent)
            ) as cursor:
                for record in cursor.execute(
                    """
                        SELECT *  
                        FROM events
                        ORDER BY position;
                        """
                ):
                    yield record
