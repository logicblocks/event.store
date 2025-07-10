import hashlib
from collections.abc import AsyncIterator, Mapping, Set
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence, TypedDict, overload
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
from logicblocks.event.sources.constraints import (
    QueryConstraint,
    SequenceNumberAfterConstraint,
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
from ...types import StreamPublishDefinition
from ..base import (
    AnyEventSerialisationGuarantee,
    CategoryEventSerialisationGuarantee,
    EventSerialisationGuarantee,
    EventStorageAdapter,
    Latestable,
    LogEventSerialisationGuarantee,
    Saveable,
    Scannable,
    StreamEventSerialisationGuarantee,
)
from .converters import (
    TypeRegistryConditionConverter,
    TypeRegistryConstraintConverter,
    WriteConditionEnforcer,
    WriteConditionEnforcerContext,
)


class StreamInsertDefinition[
    Name: StringPersistable,
    Payload: JsonPersistable,
](TypedDict):
    events: Sequence[NewEvent[Name, Payload]]
    position: int


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


@dataclass(frozen=True)
class CategoryStreamsLatestQueryParameters:
    _target: CategoryIdentifier
    _streams: Sequence[str]

    def __init__(
        self,
        *,
        target: CategoryIdentifier,
        streams: Sequence[str] = (),
    ):
        object.__setattr__(self, "_target", target)
        object.__setattr__(self, "_streams", streams)

    @property
    def category(self) -> str:
        return self._target.category

    @property
    def streams(self) -> Sequence[str]:
        return self._streams


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


@overload
def obtain_write_locks_query(
    targets: StreamIdentifier,
    serialisation_guarantee: AnyEventSerialisationGuarantee,
    table_settings: TableSettings,
) -> ParameterisedQuery: ...


@overload
def obtain_write_locks_query(
    targets: CategoryIdentifier,
    serialisation_guarantee: CategoryEventSerialisationGuarantee,
    table_settings: TableSettings,
) -> ParameterisedQuery: ...


@overload
def obtain_write_locks_query(
    targets: CategoryIdentifier,
    serialisation_guarantee: LogEventSerialisationGuarantee,
    table_settings: TableSettings,
) -> ParameterisedQuery: ...


@overload
def obtain_write_locks_query(
    targets: Sequence[StreamIdentifier],
    serialisation_guarantee: StreamEventSerialisationGuarantee,
    table_settings: TableSettings,
) -> ParameterisedQuery: ...


def obtain_write_locks_query(
    targets: CategoryIdentifier
    | StreamIdentifier
    | Sequence[StreamIdentifier],
    serialisation_guarantee: AnyEventSerialisationGuarantee,
    table_settings: TableSettings,
) -> ParameterisedQuery:
    if not targets:
        return sql.SQL("SELECT 1;"), []

    match targets, serialisation_guarantee:
        case StreamIdentifier() as target, _:
            lock_name = serialisation_guarantee.lock_name(
                namespace=table_settings.table_name, target=target
            )
            lock_digest = get_digest(lock_name)
            return (
                sql.SQL("SELECT pg_advisory_xact_lock(%s);"),
                [lock_digest],
            )
        case (
            CategoryIdentifier() as target,
            LogEventSerialisationGuarantee()
            | CategoryEventSerialisationGuarantee(),
        ):
            lock_name = serialisation_guarantee.lock_name(
                namespace=table_settings.table_name, target=target
            )
            lock_digest = get_digest(lock_name)
            return (
                sql.SQL("SELECT pg_advisory_xact_lock(%s);"),
                [lock_digest],
            )
        case [*targets], StreamEventSerialisationGuarantee():
            lock_names = [
                serialisation_guarantee.lock_name(
                    namespace=table_settings.table_name, target=target
                )
                for target in targets
            ]
            lock_digests = [get_digest(lock_name) for lock_name in lock_names]
            lock_placeholders = sql.SQL(", ").join(
                [sql.SQL("pg_advisory_xact_lock(%s)") for _ in lock_digests]
            )

            return (
                sql.SQL("SELECT {0};").format(lock_placeholders),
                lock_digests,
            )
        case _:
            raise ValueError(
                "Invalid type for targets, expected StreamIdentifier, "
                "CategoryIdentifier, or a sequence of StreamIdentifiers."
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


def read_last_category_batch_query(
    parameters: CategoryStreamsLatestQueryParameters,
    table_settings: TableSettings,
) -> ParameterisedQuery:
    table = table_settings.table_name

    select_clause = sql.SQL("SELECT DISTINCT ON (category, stream ) *")
    from_clause = sql.SQL("FROM {table}").format(table=sql.Identifier(table))

    category_where_clause = sql.SQL("category = %s")
    stream_where_placeholders = sql.SQL(", ").join(
        [sql.Placeholder() for _ in parameters.streams]
    )
    stream_where_clause = sql.SQL("stream IN ({})").format(
        stream_where_placeholders
    )
    where_clauses = [
        clause
        for clause in [
            category_where_clause,
            stream_where_clause,
        ]
    ]
    where_clause = (
        sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_clauses)
        if len(where_clauses) > 0
        else None
    )

    order_by_clause = sql.SQL("ORDER BY category, stream, position DESC")

    clauses = [
        clause
        for clause in [
            select_clause,
            from_clause,
            where_clause,
            order_by_clause,
        ]
        if clause is not None
    ]

    query = sql.SQL(" ").join(clauses)
    params = [param for param in [parameters.category, *parameters.streams]]

    return query, params


def insert_batch_query[Name: StringPersistable, Payload: JsonPersistable](
    definitions: Mapping[
        StreamIdentifier, StreamInsertDefinition[Name, Payload]
    ],
    table_settings: TableSettings,
) -> ParameterisedQuery:
    rows: list[sql.SQL] = []
    values: list[str | int | Jsonb | datetime] = []

    for identifier, definition in definitions.items():
        events = definition["events"]
        start_position = definition["position"]

        for i, event in enumerate(events):
            rows.append(sql.SQL("(%s, %s, %s, %s, %s, %s, %s, %s)"))
            values.extend(
                [
                    uuid4().hex,
                    serialise_to_string(event.name),
                    identifier.stream,
                    identifier.category,
                    start_position + i,
                    Jsonb(serialise_to_json_value(event.payload)),
                    event.observed_at,
                    event.occurred_at,
                ]
            )

    rows_expression = sql.SQL(", ").join(rows)

    return (
        sql.SQL("""
                INSERT INTO {0} (id,
                                 name,
                                 stream,
                                 category,
                                 position,
                                 payload,
                                 observed_at,
                                 occurred_at)
                VALUES
                    {1}
                    RETURNING *;
                """).format(
            sql.Identifier(table_settings.table_name), rows_expression
        ),
        values,
    )


@overload
async def obtain_write_locks(
    cursor: AsyncCursor[StoredEvent[str, JsonValue]],
    targets: StreamIdentifier,
    serialisation_guarantee: AnyEventSerialisationGuarantee,
    table_settings: TableSettings,
) -> None: ...


@overload
async def obtain_write_locks(
    cursor: AsyncCursor[StoredEvent[str, JsonValue]],
    targets: CategoryIdentifier,
    serialisation_guarantee: CategoryEventSerialisationGuarantee,
    table_settings: TableSettings,
) -> None: ...


@overload
async def obtain_write_locks(
    cursor: AsyncCursor[StoredEvent[str, JsonValue]],
    targets: CategoryIdentifier,
    serialisation_guarantee: LogEventSerialisationGuarantee,
    table_settings: TableSettings,
) -> None: ...


@overload
async def obtain_write_locks(
    cursor: AsyncCursor[StoredEvent[str, JsonValue]],
    targets: Sequence[StreamIdentifier],
    serialisation_guarantee: StreamEventSerialisationGuarantee,
    table_settings: TableSettings,
) -> None: ...


async def obtain_write_locks(
    cursor: AsyncCursor[StoredEvent[str, JsonValue]],
    targets: StreamIdentifier
    | CategoryIdentifier
    | Sequence[StreamIdentifier],
    serialisation_guarantee: AnyEventSerialisationGuarantee,
    table_settings: TableSettings,
) -> None:
    match targets, serialisation_guarantee:
        case StreamIdentifier() as target, _:
            query = obtain_write_locks_query(
                target, serialisation_guarantee, table_settings
            )
        case (
            CategoryIdentifier() as target,
            LogEventSerialisationGuarantee()
            | CategoryEventSerialisationGuarantee(),
        ):
            query = obtain_write_locks_query(
                target, serialisation_guarantee, table_settings
            )
        case [*targets], StreamEventSerialisationGuarantee():
            query = obtain_write_locks_query(
                targets, serialisation_guarantee, table_settings
            )
        case _:
            raise ValueError(
                "Invalid type for targets, expected StreamIdentifier, "
                "CategoryIdentifier, or a sequence of StreamIdentifiers."
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


async def read_last_category_batch(
    cursor: AsyncCursor[StoredEvent[str, JsonValue]],
    *,
    parameters: CategoryStreamsLatestQueryParameters,
    table_settings: TableSettings,
):
    await cursor.execute(
        *read_last_category_batch_query(parameters, table_settings)
    )
    results = await cursor.fetchall()

    return {result.stream: result for result in results}


async def insert_batch[Name: StringPersistable, Payload: JsonPersistable](
    cursor: AsyncCursor[StoredEvent[str, JsonValue]],
    *,
    definitions: Mapping[
        StreamIdentifier, StreamInsertDefinition[Name, Payload]
    ],
    table_settings: TableSettings,
) -> Mapping[StreamIdentifier, Sequence[StoredEvent[Name, Payload]]]:
    if not definitions:
        return {}

    await cursor.execute(*insert_batch_query(definitions, table_settings))
    stored_events = await cursor.fetchall()

    expected_event_count = sum(
        len(definition["events"]) for definition in definitions.values()
    )
    if len(stored_events) != expected_event_count:
        raise RuntimeError(
            f"Batch insert failed: expected {expected_event_count} rows, got {len(stored_events)}"
        )

    results: dict[StreamIdentifier, list[StoredEvent[Name, Payload]]] = {}
    event_index = 0

    for identifier, definition in definitions.items():
        events = definition["events"]
        stream_stored_events: list[StoredEvent[Name, Payload]] = []

        for event in events:
            stored_event = stored_events[event_index]
            stream_stored_events.append(
                StoredEvent[Name, Payload](
                    id=stored_event.id,
                    name=event.name,
                    stream=stored_event.stream,
                    category=stored_event.category,
                    position=stored_event.position,
                    sequence_number=stored_event.sequence_number,
                    payload=event.payload,
                    observed_at=stored_event.observed_at,
                    occurred_at=stored_event.occurred_at,
                )
            )
            event_index += 1

        results[identifier] = stream_stored_events

    return results


class PostgresEventStorageAdapter(EventStorageAdapter):
    def __init__(
        self,
        *,
        connection_source: ConnectionSource,
        serialisation_guarantee: AnyEventSerialisationGuarantee = EventSerialisationGuarantee.LOG,
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

    @overload
    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: StreamIdentifier,
        events: Sequence[NewEvent[Name, Payload]],
        condition: WriteCondition = NoCondition(),
    ) -> Sequence[StoredEvent[Name, Payload]]: ...

    @overload
    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: CategoryIdentifier,
        streams: Mapping[str, StreamPublishDefinition[Name, Payload]],
    ) -> Mapping[str, Sequence[StoredEvent[Name, Payload]]]: ...

    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent[Name, Payload]] | None = None,
        condition: WriteCondition = NoCondition(),
        streams: Mapping[str, StreamPublishDefinition[Name, Payload]]
        | None = None,
    ) -> (
        Sequence[StoredEvent[Name, Payload]]
        | Mapping[str, Sequence[StoredEvent[Name, Payload]]]
    ):
        match target:
            case StreamIdentifier():
                if events is None:
                    raise ValueError(
                        "The `events` parameter must be provided for "
                        "stream level publish."
                    )
                return await self._save_to_stream(
                    target=target, events=events, condition=condition
                )
            case CategoryIdentifier():
                if streams is None:
                    raise ValueError(
                        "The `streams` parameter must be provided for "
                        "category level publish."
                    )
                return await self._save_to_category(
                    target=target, streams=streams
                )
            case _:
                raise ValueError(f"Unsupported target type: {type(target)}")

    async def _save_to_stream[
        Name: StringPersistable,
        Payload: JsonPersistable,
    ](
        self,
        *,
        target: StreamIdentifier,
        events: Sequence[NewEvent[Name, Payload]],
        condition: WriteCondition = NoCondition(),
    ) -> Sequence[StoredEvent[Name, Payload]]:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(
                row_factory=class_row(StoredEvent[str, JsonValue])
            ) as cursor:
                await obtain_write_locks(
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

                definitions = {
                    target: StreamInsertDefinition[Name, Payload](
                        events=events, position=current_position
                    )
                }

                batch_results = await insert_batch(
                    cursor,
                    definitions=definitions,
                    table_settings=self.table_settings,
                )

                return batch_results[target]

    async def _save_to_category[
        Name: StringPersistable,
        Payload: JsonPersistable,
    ](
        self,
        *,
        target: CategoryIdentifier,
        streams: Mapping[str, StreamPublishDefinition[Name, Payload]],
    ) -> Mapping[str, Sequence[StoredEvent[Name, Payload]]]:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(
                row_factory=class_row(StoredEvent[str, JsonValue])
            ) as cursor:
                if isinstance(
                    self.serialisation_guarantee,
                    StreamEventSerialisationGuarantee,
                ):
                    await obtain_write_locks(
                        cursor,
                        [
                            StreamIdentifier(
                                category=target.category, stream=stream_name
                            )
                            for stream_name in sorted(streams.keys())
                        ],
                        serialisation_guarantee=self.serialisation_guarantee,
                        table_settings=self.table_settings,
                    )
                else:
                    await obtain_write_locks(
                        cursor,
                        target,
                        serialisation_guarantee=self.serialisation_guarantee,
                        table_settings=self.table_settings,
                    )

                definitions: dict[
                    StreamIdentifier, StreamInsertDefinition[Name, Payload]
                ] = {}

                latest_events = await read_last_category_batch(
                    cursor,
                    parameters=CategoryStreamsLatestQueryParameters(
                        target=target, streams=list(streams.keys())
                    ),
                    table_settings=self.table_settings,
                )

                for stream_name, stream_request in streams.items():
                    identifier = StreamIdentifier(
                        category=target.category, stream=stream_name
                    )

                    condition = stream_request.get("condition", NoCondition())
                    events = stream_request["events"]

                    latest_event = latest_events.get(stream_name, None)

                    condition_enforcer = self.condition_converter.convert(
                        condition
                    )
                    await condition_enforcer.assert_satisfied(
                        context=WriteConditionEnforcerContext(
                            identifier=identifier,
                            latest_event=latest_event,
                        ),
                        connection=connection,
                    )

                    current_position = (
                        latest_event.position + 1 if latest_event else 0
                    )

                    definitions[identifier] = StreamInsertDefinition[
                        Name, Payload
                    ](events=events, position=current_position)

                batch_results = await insert_batch(
                    cursor,
                    definitions=definitions,
                    table_settings=self.table_settings,
                )

                results: dict[str, Sequence[StoredEvent[Name, Payload]]] = {}
                for stream_name in streams.keys():
                    identifier = StreamIdentifier(
                        category=target.category, stream=stream_name
                    )
                    results[stream_name] = batch_results[identifier]

                return results

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
