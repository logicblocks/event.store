from collections.abc import Set
from functools import singledispatch
from typing import Sequence, Iterator, Any, Tuple, LiteralString
from uuid import uuid4

from psycopg import Connection, Cursor
from psycopg.rows import class_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from logicblocks.event.store.adapters import StorageAdapter
from logicblocks.event.store.adapters.base import Scannable, Saveable
from logicblocks.event.store.conditions import WriteCondition
from logicblocks.event.types import (
    NewEvent,
    StoredEvent,
    identifier,
)


Query = Tuple[LiteralString, Sequence[Any]]


@singledispatch
def scan_query(
    target: Scannable,
) -> Query:
    raise TypeError(f"No scan query for target: {target}")


@scan_query.register(identifier.Log)
def scan_query_log(
    _target: identifier.Log,
) -> Query:
    return (
        """
        SELECT * 
        FROM events
        ORDER BY sequence_number;
        """,
        [],
    )


@scan_query.register(identifier.Category)
def scan_query_category(
    target: identifier.Category,
) -> Query:
    return (
        """
        SELECT * 
        FROM events
        WHERE category = (%s)
        ORDER BY sequence_number;
        """,
        [target.category],
    )


@scan_query.register(identifier.Stream)
def scan_query_stream(
    target: identifier.Stream,
) -> Query:
    return (
        """
        SELECT * 
        FROM events
        WHERE category = (%s)
        AND stream = (%s)
        ORDER BY sequence_number;
        """,
        [target.category, target.stream],
    )


def lock_query() -> Query:
    return (
        """
        LOCK TABLE ONLY events IN EXCLUSIVE MODE;
        """,
        [],
    )


def read_last_query(target: identifier.Stream) -> Query:
    return (
        """
        SELECT * 
        FROM events
        WHERE category = (%s)
        AND stream = (%s)
        ORDER BY position DESC 
        LIMIT 1;
        """,
        [target.category, target.stream],
    )


def insert_query(target: Saveable, event: NewEvent, position: int) -> Query:
    return (
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
        [
            uuid4().hex,
            event.name,
            target.stream,
            target.category,
            position,
            Jsonb(event.payload),
            event.observed_at,
            event.occurred_at,
        ],
    )


def lock_table(cursor: Cursor[StoredEvent]):
    cursor.execute(*lock_query())


def read_last(
    cursor: Cursor[StoredEvent],
    *,
    target: identifier.Stream,
):
    cursor.execute(*read_last_query(target))
    return cursor.fetchone()


def insert(
    cursor: Cursor[StoredEvent],
    *,
    target: Saveable,
    event: NewEvent,
    position: int,
):
    cursor.execute(*insert_query(target, event, position))
    stored_event = cursor.fetchone()

    if not stored_event:
        raise Exception("Insert failed")

    return stored_event


class PostgresStorageAdapter(StorageAdapter):
    def __init__(self, *, connection_pool: ConnectionPool[Connection]):
        self.connection_pool = connection_pool

    def save(
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent],
        conditions: Set[WriteCondition] = frozenset(),
    ) -> Sequence[StoredEvent]:
        with self.connection_pool.connection() as connection:
            with connection.cursor(
                row_factory=class_row(StoredEvent)
            ) as cursor:
                lock_table(cursor)

                last_event = read_last(cursor, target=target)

                for condition in conditions:
                    condition.ensure(last_event)

                current_position = last_event.position + 1 if last_event else 0

                return [
                    insert(
                        cursor, target=target, event=event, position=position
                    )
                    for position, event in enumerate(events, current_position)
                ]

    def scan(
        self,
        *,
        target: Scannable = identifier.Log(),
    ) -> Iterator[StoredEvent]:
        with self.connection_pool.connection() as connection:
            with connection.cursor(
                row_factory=class_row(StoredEvent)
            ) as cursor:
                for record in cursor.execute(*scan_query(target)):
                    yield record
