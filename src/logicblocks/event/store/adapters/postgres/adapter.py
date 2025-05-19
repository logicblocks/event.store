import hashlib
from collections.abc import AsyncIterator, Set
from dataclasses import dataclass
from typing import Sequence
from uuid import uuid4

from psycopg import AsyncConnection, AsyncCursor, sql
from psycopg.rows import class_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import (
    Condition,
    ConnectionSettings,
    ConnectionSource,
    Constant,
    Operator,
    ParameterisedQuery,
    Query,
    QueryApplier,
    TableSettings,
)
from logicblocks.event.persistence.postgres.query import (
    ColumnReference,
    SortBy,
)
from logicblocks.event.types import (
    CategoryIdentifier,
    Converter,
    JsonPersistable,
    JsonValue,
    LogIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
    StringPersistable,
    serialise_to_json_value,
    serialise_to_string,
)

from ...conditions import NoCondition, WriteCondition
from ...constraints import (
    QueryConstraint,
    SequenceNumberAfterConstraint,
)
from ..base import (
    EventSerialisationGuarantee,
    EventStorageAdapter,
    Latestable,
    Saveable,
    Scannable,
)
from .converters import (
    TypeRegistryConditionConverter,
    TypeRegistryConstraintConverter,
    WriteConditionEnforcer,
    WriteConditionEnforcerContext,
)


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


def get_digest(lock_id: str) -> int:
    return (
        int(hashlib.sha256(lock_id.encode("utf-8")).hexdigest(), 16) % 10**16
    )


def scan_query(
    parameters: ScanQueryParameters,
    constraint_converter: Converter[QueryConstraint, QueryApplier],
    table_settings: TableSettings,
) -> ParameterisedQuery:
    builder = Query().select_all().from_table(table_settings.table_name)

    if parameters.category:
        builder = builder.where(
            Condition()
            .left(ColumnReference(field="category"))
            .operator(Operator.EQUALS)
            .right(Constant(parameters.category))
        )

    if parameters.stream:
        builder = builder.where(
            Condition()
            .left(ColumnReference(field="stream"))
            .operator(Operator.EQUALS)
            .right(Constant(parameters.stream))
        )

    for constraint in parameters.constraints:
        applier = constraint_converter.convert(constraint)
        builder = applier.apply(builder)

    builder = builder.order_by(
        SortBy(expression=ColumnReference(field="sequence_number"))
    ).limit(parameters.page_size)

    return builder.build()


def obtain_write_lock_query(
    target: Saveable,
    serialisation_guarantee: EventSerialisationGuarantee,
    table_settings: TableSettings,
) -> ParameterisedQuery:
    lock_name = serialisation_guarantee.lock_name(
        namespace=table_settings.table_name, target=target
    )
    return (
        sql.SQL(
            """
            SELECT pg_advisory_xact_lock(%s);
            """
        ),
        [get_digest(lock_name)],
    )


def read_last_query(
    parameters: LatestQueryParameters, table_settings: TableSettings
) -> ParameterisedQuery:
    table = table_settings.table_name

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


def insert_batch_query(
    target: Saveable,
    events: Sequence[NewEvent[StringPersistable, JsonPersistable]],
    start_position: int,
    table_settings: TableSettings,
) -> ParameterisedQuery:
    value_lists = [sql.SQL("(%s, %s, %s, %s, %s, %s, %s, %s)") for _ in events]
    values_clause = sql.SQL(", ").join(value_lists)

    values = [
        param
        for i, event in enumerate(events)
        for param in [
            uuid4().hex,
            serialise_to_string(event.name),
            target.stream,
            target.category,
            start_position + i,
            Jsonb(serialise_to_json_value(event.payload)),
            event.observed_at,
            event.occurred_at,
        ]
    ]

    return (
        sql.SQL("""
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
                VALUES
                    {1}
                    RETURNING *;
                """).format(
            sql.Identifier(table_settings.table_name), values_clause
        ),
        values,
    )


async def obtain_write_lock(
    cursor: AsyncCursor[StoredEvent[str, JsonValue]],
    target: Saveable,
    *,
    serialisation_guarantee: EventSerialisationGuarantee,
    table_settings: TableSettings,
):
    query = obtain_write_lock_query(
        target, serialisation_guarantee, table_settings
    )
    await cursor.execute(*query)


async def read_last(
    cursor: AsyncCursor[StoredEvent[str, JsonValue]],
    *,
    parameters: LatestQueryParameters,
    table_settings: TableSettings,
):
    await cursor.execute(*read_last_query(parameters, table_settings))
    return await cursor.fetchone()


async def insert_batch[Name: StringPersistable, Payload: JsonPersistable](
    cursor: AsyncCursor[StoredEvent[str, JsonValue]],
    *,
    target: Saveable,
    events: Sequence[NewEvent[Name, Payload]],
    start_position: int,
    table_settings: TableSettings,
) -> list[StoredEvent[Name, Payload]]:
    if not events:
        return []

    await cursor.execute(
        *insert_batch_query(target, events, start_position, table_settings)
    )
    stored_events = await cursor.fetchall()

    if len(stored_events) != len(events):
        raise RuntimeError(
            f"Batch insert failed: expected {len(events)} rows, got {len(stored_events)}"
        )

    return [
        StoredEvent[Name, Payload](
            id=stored_event.id,
            name=events[i].name,
            stream=stored_event.stream,
            category=stored_event.category,
            position=stored_event.position,
            sequence_number=stored_event.sequence_number,
            payload=events[i].payload,
            observed_at=stored_event.observed_at,
            occurred_at=stored_event.occurred_at,
        )
        for i, stored_event in enumerate(stored_events)
    ]


class PostgresEventStorageAdapter(EventStorageAdapter):
    def __init__(
        self,
        *,
        connection_source: ConnectionSource,
        serialisation_guarantee: EventSerialisationGuarantee = EventSerialisationGuarantee.LOG,
        query_settings: QuerySettings = QuerySettings(),
        table_settings: TableSettings = TableSettings(table_name="events"),
        constraint_converter: Converter[QueryConstraint, QueryApplier]
        | None = None,
        condition_converter: Converter[WriteCondition, WriteConditionEnforcer]
        | None = None,
        max_insert_batch_size: int = 1000,
    ):
        if isinstance(connection_source, ConnectionSettings):
            self._connection_pool_owner = True
            self.connection_pool = AsyncConnectionPool[AsyncConnection](
                connection_source.to_connection_string(), open=False
            )
        else:
            self._connection_pool_owner = False
            self.connection_pool = connection_source

        self.serialisation_guarantee = serialisation_guarantee
        self.query_settings = query_settings
        self.table_settings = table_settings
        self.constraint_converter = (
            constraint_converter
            if constraint_converter is not None
            else (
                TypeRegistryConstraintConverter().with_default_constraint_converters()
            )
        )
        self.condition_converter = (
            condition_converter
            if condition_converter is not None
            else (
                TypeRegistryConditionConverter().with_default_condition_converters()
            )
        )
        self.max_insert_batch_size = max_insert_batch_size

    async def open(self) -> None:
        if self._connection_pool_owner:
            await self.connection_pool.open()

    async def close(self) -> None:
        if self._connection_pool_owner:
            await self.connection_pool.close()

    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent[Name, Payload]],
        condition: WriteCondition = NoCondition(),
    ) -> Sequence[StoredEvent[Name, Payload]]:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(
                row_factory=class_row(StoredEvent[str, JsonValue])
            ) as cursor:
                await obtain_write_lock(
                    cursor,
                    target,
                    serialisation_guarantee=self.serialisation_guarantee,
                    table_settings=self.table_settings,
                )

                latest_event = await read_last(
                    cursor,
                    parameters=LatestQueryParameters(target=target),
                    table_settings=self.table_settings,
                )

                condition_enforcer = self.condition_converter.convert(
                    condition
                )
                await condition_enforcer.assert_satisfied(
                    context=WriteConditionEnforcerContext(
                        identifier=target,
                        latest_event=latest_event,
                    ),
                    connection=connection,
                )

                current_position = (
                    latest_event.position + 1 if latest_event else 0
                )

                all_stored_events: list[StoredEvent[Name, Payload]] = []
                for i in range(0, len(events), self.max_insert_batch_size):
                    batch_events = events[i : i + self.max_insert_batch_size]
                    batch_position = current_position + i

                    batch_results = await insert_batch(
                        cursor,
                        target=target,
                        events=batch_events,
                        start_position=batch_position,
                        table_settings=self.table_settings,
                    )

                    all_stored_events.extend(batch_results)

                return all_stored_events

    async def latest(
        self, *, target: Latestable
    ) -> StoredEvent[str, JsonValue] | None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(
                row_factory=class_row(StoredEvent[str, JsonValue])
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
    ) -> AsyncIterator[StoredEvent[str, JsonValue]]:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(
                row_factory=class_row(StoredEvent[str, JsonValue])
            ) as cursor:
                page_size = self.query_settings.scan_query_page_size
                last_sequence_number = None
                keep_querying = True

                while keep_querying:
                    if last_sequence_number is not None:
                        constraint = SequenceNumberAfterConstraint(
                            sequence_number=last_sequence_number
                        )
                        constraints = {
                            constraint
                            for constraint in constraints
                            if not isinstance(
                                constraint, SequenceNumberAfterConstraint
                            )
                        }
                        constraints.add(constraint)

                    parameters = ScanQueryParameters(
                        target=target,
                        page_size=page_size,
                        constraints=constraints,
                    )
                    results = await cursor.execute(
                        *scan_query(
                            parameters=parameters,
                            constraint_converter=self.constraint_converter,
                            table_settings=self.table_settings,
                        )
                    )

                    keep_querying = results.rowcount == page_size

                    async for event in results:
                        yield event
                        last_sequence_number = event.sequence_number
