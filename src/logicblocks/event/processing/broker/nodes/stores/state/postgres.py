from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Self

from psycopg import AsyncConnection, AsyncCursor, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db.postgres import (
    Column,
    Condition,
    ParameterisedQuery,
    SortDirection,
    Value,
)
from logicblocks.event.db.postgres import (
    Query as DBQuery,
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

from .base import NodeState, NodeStateStore


@dataclass(frozen=True)
class TableSettings:
    nodes_table_name: str

    def __init__(self, *, nodes_table_name: str = "nodes"):
        object.__setattr__(self, "nodes_table_name", nodes_table_name)


type PostgresClauseApplicator[C: Clause] = Callable[
    [C, DBQuery, TableSettings], DBQuery
]


def filter_clause_applicator(
    filter: FilterClause, query: DBQuery, table_settings: TableSettings
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
    sort: SortClause, query: DBQuery, table_settings: TableSettings
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
    def __init__(self, table_settings: TableSettings = TableSettings()):
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
            .from_table(self._table_settings.nodes_table_name)
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


def upsert_query(
    node: NodeState,
    table_settings: TableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            INSERT INTO {0} (id, last_seen)
            VALUES (%s, %s)
            ON CONFLICT (id)
            DO UPDATE SET (last_seen) = ROW (%s);
            """
        ).format(sql.Identifier(table_settings.nodes_table_name)),
        [node.node_id, node.last_seen, node.last_seen],
    )


def delete_query(
    node_id: str,
    table_settings: TableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            DELETE
            FROM {0}
            WHERE id = %s
            RETURNING *;
            """
        ).format(sql.Identifier(table_settings.nodes_table_name)),
        [node_id],
    )


def heartbeat_query(
    node: NodeState,
    table_settings: TableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            UPDATE {0}
            SET last_seen = %s
            WHERE id = %s
                RETURNING *;
            """
        ).format(sql.Identifier(table_settings.nodes_table_name)),
        [
            node.last_seen,
            node.node_id,
        ],
    )


def purge_query(
    cutoff_time: datetime,
    table_settings: TableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            DELETE
            FROM {0}
            WHERE last_seen <= %s;
            """
        ).format(sql.Identifier(table_settings.nodes_table_name)),
        [
            cutoff_time,
        ],
    )


async def upsert(
    cursor: AsyncCursor[Any],
    *,
    node: NodeState,
    table_settings: TableSettings,
) -> None:
    await cursor.execute(*upsert_query(node, table_settings))


async def remove(
    cursor: AsyncCursor[Any],
    *,
    node_id: str,
    table_settings: TableSettings,
) -> None:
    results = await cursor.execute(*delete_query(node_id, table_settings))
    deleted_nodes = await results.fetchall()
    if len(deleted_nodes) == 0:
        raise ValueError("Can't remove missing node.")


async def heartbeat(
    cursor: AsyncCursor[Any],
    *,
    node: NodeState,
    table_settings: TableSettings,
) -> None:
    results = await cursor.execute(*heartbeat_query(node, table_settings))
    updated_subscribers = await results.fetchall()
    if len(updated_subscribers) == 0:
        raise ValueError("Can't heartbeat missing node.")


async def purge(
    cursor: AsyncCursor[Any],
    *,
    cutoff_time: datetime,
    table_settings: TableSettings,
) -> None:
    await cursor.execute(
        *purge_query(
            cutoff_time,
            table_settings,
        )
    )


class PostgresNodeStateStore(NodeStateStore):
    def __init__(
        self,
        connection_pool: AsyncConnectionPool[AsyncConnection],
        table_settings: TableSettings = TableSettings(),
        clock: Clock = SystemClock(),
        query_converter: PostgresQueryConverter | None = None,
    ):
        self.connection_pool = connection_pool
        self.table_settings = table_settings
        self.clock = clock
        self.query_converter = (
            query_converter
            if query_converter is not None
            else (PostgresQueryConverter().with_default_clause_applicators())
        )

    async def add(self, node_id: str) -> None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await upsert(
                    cursor,
                    node=NodeState(node_id, last_seen=self.clock.now(UTC)),
                    table_settings=self.table_settings,
                )

    async def remove(self, node_id: str):
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await remove(
                    cursor,
                    node_id=node_id,
                    table_settings=self.table_settings,
                )

    async def list(
        self, max_time_since_last_seen: timedelta | None = None
    ) -> Sequence[NodeState]:
        filters: list[FilterClause] = []
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

                node_state_dicts = await results.fetchall()

                return [
                    NodeState(
                        node_id=node_state_dict["id"],
                        last_seen=node_state_dict["last_seen"],
                    )
                    for node_state_dict in node_state_dicts
                ]

    async def heartbeat(self, node_id: str):
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await heartbeat(
                    cursor,
                    node=NodeState(
                        node_id=node_id, last_seen=self.clock.now()
                    ),
                    table_settings=self.table_settings,
                )

    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(seconds=300)
    ) -> None:
        cutoff_time = self.clock.now(UTC) - max_time_since_last_seen
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await purge(
                    cursor,
                    cutoff_time=cutoff_time,
                    table_settings=self.table_settings,
                )
