from collections.abc import Set
from typing import Sequence, Iterator
from uuid import uuid4

from psycopg import Connection
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from logicblocks.event.store.adapters import StorageAdapter
from logicblocks.event.store.conditions import WriteCondition
from logicblocks.event.store.types import NewEvent, StoredEvent


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
        event = events[0]
        event_id = uuid4().hex
        event_position = 0

        with self.connection_pool.connection() as connection:
            with connection.cursor() as cursor:
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
                        event_position,
                        Jsonb(event.payload),
                        event.observed_at,
                        event.occurred_at,
                    ),
                )

        return [
            StoredEvent(
                id=event_id,
                name=event.name,
                stream=stream,
                category=category,
                position=event_position,
                payload=event.payload,
                observed_at=event.observed_at,
                occurred_at=event.occurred_at,
            )
        ]

    def scan_stream(
        self, *, category: str, stream: str
    ) -> Iterator[StoredEvent]:
        return iter([])

    def scan_category(self, *, category: str) -> Iterator[StoredEvent]:
        return iter([])

    def scan_all(self) -> Iterator[StoredEvent]:
        return iter([])
