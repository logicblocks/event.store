from collections.abc import Set
from typing import Sequence, Iterator
from uuid import uuid4
from datetime import timezone

from psycopg import Connection, Cursor
from psycopg.rows import TupleRow, class_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from logicblocks.event.store.adapters import StorageAdapter
from logicblocks.event.store.conditions import WriteCondition
from logicblocks.event.types import NewEvent, StoredEvent


def insert_event(
    cursor: Cursor, stream: str, category: str, event: NewEvent, position: int
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

    return StoredEvent(
        id=event_id,
        name=event.name,
        stream=stream,
        category=category,
        position=position,
        payload=event.payload,
        observed_at=event.observed_at,
        occurred_at=event.occurred_at,
    )


def to_event(event_row: TupleRow) -> StoredEvent:
    (
        id,
        name,
        stream,
        category,
        position,
        payload,
        observed_at,
        occurred_at,
    ) = event_row

    return StoredEvent(
        id=id,
        name=name,
        stream=stream,
        category=category,
        position=position,
        payload=payload,
        observed_at=observed_at.replace(tzinfo=timezone.utc),
        occurred_at=occurred_at.replace(tzinfo=timezone.utc),
    )


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
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT * 
                    FROM events
                    ORDER BY position
                    DESC LIMIT 1;
                    """
                )
                last_event_row = cursor.fetchone()
                last_event: StoredEvent | None = (
                    to_event(last_event_row) if last_event_row else None
                )

                for condition in conditions:
                    condition.evaluate(last_event)

                current_position = last_event.position + 1 if last_event else 0

                return [
                    insert_event(cursor, stream, category, event, position)
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
                    SELECT 
                      id, 
                      name, 
                      stream, 
                      category, 
                      position, 
                      payload, 
                      observed_at::timestamptz, 
                      occurred_at::timestamptz 
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
                    SELECT
                      id, 
                      name, 
                      stream, 
                      category, 
                      position, 
                      payload, 
                      observed_at::timestamptz, 
                      occurred_at::timestamptz  
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
                    SELECT 
                      id, 
                      name, 
                      stream, 
                      category, 
                      position, 
                      payload, 
                      observed_at::timestamptz, 
                      occurred_at::timestamptz  
                    FROM events
                    ORDER BY position;
                    """
                ):
                    yield record
