from collections.abc import AsyncIterator, Set
from dataclasses import dataclass
from typing import Any, Sequence, Tuple
from uuid import uuid4

from psycopg import AsyncConnection, AsyncCursor, abc, sql
from psycopg.rows import class_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.store.adapters import StorageAdapter
from logicblocks.event.store.adapters.base import Saveable, Scannable
from logicblocks.event.store.conditions import WriteCondition
from logicblocks.event.types import (
    NewEvent,
    StoredEvent,
    identifier,
)


@dataclass(frozen=True)
class ConnectionSettings(object):
    host: str
    port: int
    dbname: str
    user: str
    password: str

    def __init__(
        self, *, host: str, port: int, dbname: str, user: str, password: str
    ):
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "dbname", dbname)
        object.__setattr__(self, "user", user)
        object.__setattr__(self, "password", password)

    def __repr__(self):
        return (
            f"ConnectionParameters("
            f"host={self.host}, "
            f"port={self.port}, "
            f"dbname={self.dbname}, "
            f"user={self.user}, "
            f"password={"*" * len(self.password)})"
        )

    def to_connection_string(self) -> str:
        userspec = f"{self.user}:{self.password}"
        hostspec = f"{self.host}:{self.port}"
        return f"postgresql://{userspec}@{hostspec}/{self.dbname}"


ConnectionSource = ConnectionSettings | AsyncConnectionPool[AsyncConnection]


@dataclass(frozen=True)
class TableSettings(object):
    events_table_name: str

    def __init__(self, *, events_table_name: str = "events"):
        object.__setattr__(self, "events_table_name", events_table_name)


@dataclass(frozen=True)
class QuerySettings(object):
    scan_query_page_size: int

    def __init__(self, *, scan_query_page_size: int = 100):
        object.__setattr__(self, "scan_query_page_size", scan_query_page_size)


@dataclass(frozen=True)
class ScanQueryParameters(object):
    target: Scannable
    page_size: int
    last_sequence_number: int | None

    def __init__(
        self,
        *,
        target: Scannable,
        page_size: int,
        last_sequence_number: int | None = None,
    ):
        object.__setattr__(self, "target", target)
        object.__setattr__(self, "page_size", page_size)
        object.__setattr__(self, "last_sequence_number", last_sequence_number)

    @property
    def category(self) -> str | None:
        match self.target:
            case identifier.Category(category):
                return category
            case identifier.Stream(category, _):
                return category
            case _:
                return None

    @property
    def stream(self) -> str | None:
        match self.target:
            case identifier.Stream(_, stream):
                return stream
            case _:
                return None


ParameterisedQuery = Tuple[abc.Query, Sequence[Any]]


def scan_query(
    parameters: ScanQueryParameters, table_settings: TableSettings
) -> ParameterisedQuery:
    table = table_settings.events_table_name

    category_condition = (
        sql.SQL("category = %s") if parameters.category is not None else None
    )
    stream_condition = (
        sql.SQL("stream = %s") if parameters.stream is not None else None
    )
    sequence_number_condition = (
        sql.SQL("sequence_number > %s")
        if parameters.last_sequence_number is not None
        else None
    )

    conditions = [
        condition
        for condition in [
            category_condition,
            stream_condition,
            sequence_number_condition,
        ]
        if condition is not None
    ]

    select_clause = sql.SQL("SELECT *")
    from_clause = sql.SQL("FROM {table}").format(table=sql.Identifier(table))
    where_clause = (
        sql.SQL("WHERE ") + sql.SQL(" AND ").join(conditions)
        if len(conditions) > 0
        else None
    )
    order_by_clause = sql.SQL("ORDER BY sequence_number ASC")
    limit_clause = sql.SQL("LIMIT %s")

    clauses = [
        clause
        for clause in [
            select_clause,
            from_clause,
            where_clause,
            order_by_clause,
            limit_clause,
        ]
        if clause is not None
    ]

    query = sql.SQL(" ").join(clauses)
    params = [
        param
        for param in [
            parameters.category,
            parameters.stream,
            parameters.last_sequence_number,
            parameters.page_size,
        ]
        if param is not None
    ]

    return (query, params)


def lock_query(table_settings: TableSettings) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            LOCK TABLE ONLY {0} IN EXCLUSIVE MODE;
            """
        ).format(sql.Identifier(table_settings.events_table_name)),
        [],
    )


def read_last_query(
    target: identifier.Stream, table_settings: TableSettings
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            SELECT * 
            FROM {0}
            WHERE category = (%s)
            AND stream = (%s)
            ORDER BY position DESC 
            LIMIT 1;
            """
        ).format(sql.Identifier(table_settings.events_table_name)),
        [target.category, target.stream],
    )


def insert_query(
    target: Saveable,
    event: NewEvent,
    position: int,
    table_settings: TableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            INSERT INTO {0} (
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
              RETURNING *;
            """
        ).format(sql.Identifier(table_settings.events_table_name)),
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


async def lock_table(
    cursor: AsyncCursor[StoredEvent], *, table_settings: TableSettings
):
    await cursor.execute(*lock_query(table_settings))


async def read_last(
    cursor: AsyncCursor[StoredEvent],
    *,
    target: identifier.Stream,
    table_settings: TableSettings,
):
    await cursor.execute(*read_last_query(target, table_settings))
    return await cursor.fetchone()


async def insert(
    cursor: AsyncCursor[StoredEvent],
    *,
    target: Saveable,
    event: NewEvent,
    position: int,
    table_settings: TableSettings,
):
    await cursor.execute(
        *insert_query(target, event, position, table_settings)
    )
    stored_event = await cursor.fetchone()

    if not stored_event:
        raise Exception("Insert failed")

    return stored_event


class PostgresStorageAdapter(StorageAdapter):
    connection_pool: AsyncConnectionPool[AsyncConnection]
    connection_pool_owner: bool

    query_settings: QuerySettings
    table_settings: TableSettings

    def __init__(
        self,
        *,
        connection_source: ConnectionSource,
        query_settings: QuerySettings = QuerySettings(),
        table_settings: TableSettings = TableSettings(),
    ):
        if isinstance(connection_source, ConnectionSettings):
            self.connection_pool_owner = True
            self.connection_pool = AsyncConnectionPool[AsyncConnection](
                connection_source.to_connection_string(), open=False
            )
        else:
            self.connection_pool_owner = False
            self.connection_pool = connection_source

        self.query_settings = query_settings
        self.table_settings = table_settings

    async def open(self) -> None:
        if self.connection_pool_owner:
            await self.connection_pool.open()

    async def close(self) -> None:
        if self.connection_pool_owner:
            await self.connection_pool.close()

    async def save(
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent],
        conditions: Set[WriteCondition] = frozenset(),
    ) -> Sequence[StoredEvent]:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(
                row_factory=class_row(StoredEvent)
            ) as cursor:
                await lock_table(cursor, table_settings=self.table_settings)

                last_event = await read_last(
                    cursor,
                    target=target,
                    table_settings=self.table_settings,
                )

                for condition in conditions:
                    condition.ensure(last_event)

                current_position = last_event.position + 1 if last_event else 0

                return [
                    await insert(
                        cursor,
                        target=target,
                        event=event,
                        position=position,
                        table_settings=self.table_settings,
                    )
                    for position, event in enumerate(events, current_position)
                ]

    async def scan(
        self,
        *,
        target: Scannable = identifier.Log(),
    ) -> AsyncIterator[StoredEvent]:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(
                row_factory=class_row(StoredEvent)
            ) as cursor:
                page_size = self.query_settings.scan_query_page_size
                last_sequence_number = None
                keep_querying = True

                while keep_querying:
                    parameters = ScanQueryParameters(
                        target=target,
                        page_size=page_size,
                        last_sequence_number=last_sequence_number,
                    )
                    results = await cursor.execute(
                        *scan_query(
                            parameters=parameters,
                            table_settings=self.table_settings,
                        )
                    )

                    keep_querying = results.rowcount == page_size

                    async for event in results:
                        yield event
                        last_sequence_number = event.sequence_number
