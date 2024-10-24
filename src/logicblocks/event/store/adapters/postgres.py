from collections.abc import Set
from typing import Sequence, Iterator
from uuid import uuid4

from psycopg import Connection, Cursor
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
                return [
                    insert_event(cursor, stream, category, event, position)
                    for position, event in enumerate(events)
                ]

    def scan_stream(
        self, *, category: str, stream: str
    ) -> Iterator[StoredEvent]:
        return iter([])

    def scan_category(self, *, category: str) -> Iterator[StoredEvent]:
        return iter([])

    def scan_all(self) -> Iterator[StoredEvent]:
        return iter([])
