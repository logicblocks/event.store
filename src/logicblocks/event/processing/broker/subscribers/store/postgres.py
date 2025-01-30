from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Self

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db.postgres import (
    Column,
    Condition,
    ConnectionSettings,
    ConnectionSource,
    ParameterisedQuery,
    SortDirection,
    Value,
)
from logicblocks.event.db.postgres import (
    Query as DBQuery,
)
from logicblocks.event.processing.broker.types import (
    EventSubscriber,
    EventSubscriberKey,
)
from logicblocks.event.projection.store import (
    Clause,
    FilterClause,
    Lookup,
    Operator,
    Path,
    Query,
    Search,
    SortClause,
)
from logicblocks.event.projection.store.adapters.postgres import (
    column_for_query_path,
    operator_for_query_operator,
    sort_direction_for_query_sort_order,
)
from logicblocks.event.utils.clock import Clock, SystemClock

from .base import EventSubscriberState, EventSubscriberStore


@dataclass(frozen=True)
class PostgresTableSettings:
    subscribers_table_name: str

    def __init__(self, *, subscribers_table_name: str = "subscribers"):
        object.__setattr__(
            self, "subscribers_table_name", subscribers_table_name
        )


type PostgresClauseApplicator[C: Clause] = Callable[
    [C, DBQuery, PostgresTableSettings], DBQuery
]


def filter_clause_applicator(
    filter: FilterClause, query: DBQuery, table_settings: PostgresTableSettings
) -> DBQuery:
    return query.where(
        Condition()
        .left(Column(field=filter.path.top_level, path=filter.path.sub_levels))
        .operator(operator_for_query_operator(filter.operator))
        .right(
            Value(
                filter.value,
                wrapper="to_jsonb" if filter.path.is_nested() else None,
            )
        )
    )


def sort_clause_applicator(
    sort: SortClause, query: DBQuery, table_settings: PostgresTableSettings
) -> DBQuery:
    order_by_fields: list[tuple[Column, SortDirection]] = []
    for field in sort.fields:
        order_by_fields.append(
            (
                column_for_query_path(field.path),
                sort_direction_for_query_sort_order(field.order),
            )
        )

    return query.order_by(*order_by_fields)


class PostgresQueryConverter:
    def __init__(
        self, table_settings: PostgresTableSettings = PostgresTableSettings()
    ):
        self._registry: dict[type[Clause], PostgresClauseApplicator[Any]] = {}
        self._table_settings = table_settings

    def with_default_clause_applicators(self) -> Self:
        return self.register_clause_applicator(
            FilterClause, filter_clause_applicator
        ).register_clause_applicator(SortClause, sort_clause_applicator)

    def register_clause_applicator[C: Clause](
        self, clause_type: type[C], applicator: PostgresClauseApplicator[C]
    ) -> Self:
        self._registry[clause_type] = applicator
        return self

    def apply_clause(self, clause: Clause, query_builder: DBQuery) -> DBQuery:
        applicator = self._registry.get(type(clause))
        if applicator is None:
            raise ValueError(f"No converter registered for {type(clause)}")
        return applicator(clause, query_builder, self._table_settings)

    def convert_query(self, query: Query) -> ParameterisedQuery:
        builder = (
            DBQuery()
            .select_all()
            .from_table(self._table_settings.subscribers_table_name)
        )

        match query:
            case Lookup(filters):
                for filter in filters:
                    builder = self.apply_clause(filter, builder)
                return builder.build()
            case Search(filters, sort):
                for filter in filters:
                    builder = self.apply_clause(filter, builder)
                if sort is not None:
                    builder = self.apply_clause(sort, builder)
                return builder.build()
            case _:
                raise ValueError(f"Unsupported query: {query}")


def insert_query(
    subscriber: EventSubscriberState,
    table_settings: PostgresTableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            INSERT INTO {0} (
              "group", 
              id,
              last_seen
            )
            VALUES (%s, %s, %s)
              ON CONFLICT ("group", id) 
              DO UPDATE
            SET (last_seen) = ROW(%s);
            """
        ).format(sql.Identifier(table_settings.subscribers_table_name)),
        [
            subscriber.group,
            subscriber.id,
            subscriber.last_seen,
            subscriber.last_seen,
        ],
    )


def delete_query(
    key: EventSubscriberKey,
    table_settings: PostgresTableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            DELETE FROM {0}
            WHERE "group" = %s AND id = %s
            RETURNING *;;
            """
        ).format(sql.Identifier(table_settings.subscribers_table_name)),
        [key.group, key.id],
    )


def heartbeat_query(
    subscriber: EventSubscriberState,
    table_settings: PostgresTableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            UPDATE {0}
            SET last_seen = %s
            WHERE "group" = %s AND id = %s
            RETURNING *;
            """
        ).format(sql.Identifier(table_settings.subscribers_table_name)),
        [
            subscriber.last_seen,
            subscriber.group,
            subscriber.id,
        ],
    )


def purge_query(
    cutoff_time: datetime,
    table_settings: PostgresTableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            DELETE FROM {0}
            WHERE last_seen <= %s;
            """
        ).format(sql.Identifier(table_settings.subscribers_table_name)),
        [
            cutoff_time,
        ],
    )


class PostgresEventSubscriberStore(EventSubscriberStore):
    def __init__(
        self,
        *,
        connection_source: ConnectionSource,
        clock: Clock = SystemClock(),
        table_settings: PostgresTableSettings = PostgresTableSettings(),
        query_converter: PostgresQueryConverter | None = None,
    ):
        if isinstance(connection_source, ConnectionSettings):
            self._connection_pool_owner = True
            self.connection_pool = AsyncConnectionPool[AsyncConnection](
                connection_source.to_connection_string(), open=False
            )
        else:
            self._connection_pool_owner = False
            self.connection_pool = connection_source
        self.clock = clock
        self.table_settings = table_settings
        self.query_converter = (
            query_converter
            if query_converter is not None
            else (PostgresQueryConverter().with_default_clause_applicators())
        )

    async def add(self, subscriber: EventSubscriber) -> None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    *insert_query(
                        EventSubscriberState(
                            id=subscriber.id,
                            group=subscriber.group,
                            last_seen=self.clock.now(),
                        ),
                        self.table_settings,
                    )
                )

    async def remove(self, subscriber: EventSubscriber) -> None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                results = await cursor.execute(
                    *delete_query(subscriber.key, self.table_settings)
                )
                deleted_subscribers = await results.fetchall()
                if len(deleted_subscribers) == 0:
                    raise ValueError(
                        f"Unknown subscriber: {subscriber.group} {subscriber.id}"
                    )

    async def list(
        self,
        subscriber_group: str | None = None,
        max_time_since_last_seen: timedelta | None = None,
    ) -> Sequence[EventSubscriberState]:
        filters: list[FilterClause] = []
        if subscriber_group is not None:
            filters.append(
                FilterClause(Operator.EQUAL, Path("group"), subscriber_group)
            )
        if max_time_since_last_seen is not None:
            filters.append(
                FilterClause(
                    Operator.GREATER_THAN,
                    Path("last_seen"),
                    self.clock.now() - max_time_since_last_seen,
                )
            )
        query = self.query_converter.convert_query(Search(filters=filters))
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                results = await cursor.execute(*query)

                subscriber_state_dicts = await results.fetchall()

                return [
                    EventSubscriberState(
                        id=subscriber_state_dict["id"],
                        group=subscriber_state_dict["group"],
                        last_seen=subscriber_state_dict["last_seen"],
                    )
                    for subscriber_state_dict in subscriber_state_dicts
                ]

    async def heartbeat(self, subscriber: EventSubscriber) -> None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                results = await cursor.execute(
                    *heartbeat_query(
                        EventSubscriberState(
                            id=subscriber.id,
                            group=subscriber.group,
                            last_seen=self.clock.now(),
                        ),
                        self.table_settings,
                    )
                )
                updated_subscribers = await results.fetchall()
                if len(updated_subscribers) == 0:
                    raise ValueError(
                        f"Unknown subscriber: {subscriber.group} {subscriber.id}"
                    )

    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(seconds=300)
    ) -> None:
        cutoff_time = self.clock.now(UTC) - max_time_since_last_seen
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    *purge_query(
                        cutoff_time,
                        self.table_settings,
                    )
                )
