from collections.abc import AsyncIterator, Set
from dataclasses import dataclass
from functools import singledispatch
from typing import Any, Sequence, Tuple
from uuid import uuid4

from psycopg import AsyncConnection, AsyncCursor, abc, sql
from psycopg.rows import class_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db.postgres import (
    ConnectionSettings,
    ConnectionSource,
)
from logicblocks.event.store.adapters import EventStorageAdapter
from logicblocks.event.store.adapters.base import (
    Latestable,
    Saveable,
    Scannable,
)
from logicblocks.event.store.conditions import WriteCondition
from logicblocks.event.store.constraints import (
    QueryConstraint,
    SequenceNumberAfterConstraint,
)
from logicblocks.event.types import (
    CategoryIdentifier,
    LogIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
)


@dataclass(frozen=True)
class TableSettings:
    events_table_name: str

    def __init__(self, *, events_table_name: str = "events"):
        object.__setattr__(self, "events_table_name", events_table_name)


@dataclass(frozen=True)
class QuerySettings:
    scan_query_page_size: int

    def __init__(self, *, scan_query_page_size: int = 100):
        object.__setattr__(self, "scan_query_page_size", scan_query_page_size)


@dataclass(frozen=True)
class ScanQueryParameters:
    target: Scannable
    constraints: Set[QueryConstraint]
    page_size: int

    def __init__(
        self,
        *,
        target: Scannable,
        constraints: Set[QueryConstraint] = frozenset(),
        page_size: int,
    ):
        object.__setattr__(self, "target", target)
        object.__setattr__(self, "constraints", constraints)
        object.__setattr__(self, "page_size", page_size)

    @property
    def category(self) -> str | None:
        match self.target:
            case CategoryIdentifier(category):
                return category
            case StreamIdentifier(category, _):
                return category
            case _:
                return None

    @property
    def stream(self) -> str | None:
        match self.target:
            case StreamIdentifier(_, stream):
                return stream
            case _:
                return None


@dataclass(frozen=True)
class LatestQueryParameters:
    target: Latestable

    def __init__(
        self,
        *,
        target: Scannable,
    ):
        object.__setattr__(self, "target", target)

    @property
    def category(self) -> str | None:
        match self.target:
            case CategoryIdentifier(category):
                return category
            case StreamIdentifier(category, _):
                return category
            case _:
                return None

    @property
    def stream(self) -> str | None:
        match self.target:
            case StreamIdentifier(_, stream):
                return stream
            case _:
                return None


type ParameterisedQuery = Tuple[abc.Query, Sequence[Any]]
type ParameterisedQueryFragment = Tuple[sql.SQL, Sequence[Any]]


@singledispatch
def query_constraint_to_sql(
    constraint: QueryConstraint,
) -> ParameterisedQueryFragment:
    raise TypeError(f"No SQL converter for query constraint: {constraint}")


@query_constraint_to_sql.register(SequenceNumberAfterConstraint)
def sequence_number_after_query_constraint_to_sql(
    constraint: SequenceNumberAfterConstraint,
) -> ParameterisedQueryFragment:
    return sql.SQL("sequence_number > %s"), [constraint.sequence_number]


def scan_query(
    parameters: ScanQueryParameters, table_settings: TableSettings
) -> ParameterisedQuery:
    table = table_settings.events_table_name

    category_where_clause = (
        sql.SQL("category = %s") if parameters.category is not None else None
    )
    stream_where_clause = (
        sql.SQL("stream = %s") if parameters.stream is not None else None
    )

    extra_where_clauses: list[sql.SQL] = []
    extra_parameters: list[Any] = []
    for constraint in parameters.constraints:
        clause, params = query_constraint_to_sql(constraint)
        extra_where_clauses.append(clause)
        extra_parameters.extend(params)

    where_clauses = [
        clause
        for clause in [
            category_where_clause,
            stream_where_clause,
            *extra_where_clauses,
        ]
        if clause is not None
    ]

    select_clause = sql.SQL("SELECT *")
    from_clause = sql.SQL("FROM {table}").format(table=sql.Identifier(table))
    where_clause = (
        sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses)
        if len(where_clauses) > 0
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
            *extra_parameters,
            parameters.page_size,
        ]
        if param is not None
    ]

    return query, params


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
    parameters: LatestQueryParameters, table_settings: TableSettings
) -> ParameterisedQuery:
    table = table_settings.events_table_name

    select_clause = sql.SQL("SELECT *")
    from_clause = sql.SQL("FROM {table}").format(table=sql.Identifier(table))

    category_where_clause = (
        sql.SQL("category = %s") if parameters.category is not None else None
    )
    stream_where_clause = (
        sql.SQL("stream = %s") if parameters.stream is not None else None
    )
    where_clauses = [
        clause
        for clause in [
            category_where_clause,
            stream_where_clause,
        ]
        if clause is not None
    ]
    where_clause = (
        sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses)
        if len(where_clauses) > 0
        else None
    )

    order_by_clause = sql.SQL("ORDER BY sequence_number DESC")
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
        for param in [parameters.category, parameters.stream, 1]
        if param is not None
    ]

    return query, params


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
    parameters: LatestQueryParameters,
    table_settings: TableSettings,
):
    await cursor.execute(*read_last_query(parameters, table_settings))
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

    if stored_event is None:  # pragma: no cover
        raise RuntimeError("Insert failed")

    return stored_event


class PostgresEventStorageAdapter(EventStorageAdapter):
    def __init__(
        self,
        *,
        connection_source: ConnectionSource,
        query_settings: QuerySettings = QuerySettings(),
        table_settings: TableSettings = TableSettings(),
    ):
        if isinstance(connection_source, ConnectionSettings):
            self._connection_pool_owner = True
            self.connection_pool = AsyncConnectionPool[AsyncConnection](
                connection_source.to_connection_string(), open=False
            )
        else:
            self._connection_pool_owner = False
            self.connection_pool = connection_source

        self.query_settings: QuerySettings = query_settings
        self.table_settings: TableSettings = table_settings

    async def open(self) -> None:
        if self._connection_pool_owner:
            await self.connection_pool.open()

    async def close(self) -> None:
        if self._connection_pool_owner:
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
                    parameters=LatestQueryParameters(target=target),
                    table_settings=self.table_settings,
                )

                for condition in conditions:
                    condition.assert_met_by(last_event=last_event)

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

    async def latest(self, *, target: Latestable) -> StoredEvent | None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(
                row_factory=class_row(StoredEvent)
            ) as cursor:
                await cursor.execute(
                    *read_last_query(
                        parameters=LatestQueryParameters(target=target),
                        table_settings=self.table_settings,
                    )
                )
                return await cursor.fetchone()

    async def scan(
        self,
        *,
        target: Scannable = LogIdentifier(),
        constraints: Set[QueryConstraint] = frozenset(),
    ) -> AsyncIterator[StoredEvent]:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(
                row_factory=class_row(StoredEvent)
            ) as cursor:
                page_size = self.query_settings.scan_query_page_size
                last_sequence_number = None
                keep_querying = True

                while keep_querying:
                    sequence_number_constraint = (
                        SequenceNumberAfterConstraint(
                            sequence_number=last_sequence_number
                        )
                        if last_sequence_number is not None
                        else None
                    )
                    constraints = (
                        {*constraints, sequence_number_constraint}
                        if sequence_number_constraint
                        else constraints
                    )

                    parameters = ScanQueryParameters(
                        target=target,
                        page_size=page_size,
                        constraints=constraints,
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
