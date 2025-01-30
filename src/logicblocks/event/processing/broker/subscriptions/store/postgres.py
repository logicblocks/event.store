from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Callable, Self

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
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
from logicblocks.event.projection.store import (
    Clause,
    FilterClause,
    Lookup,
    Query,
    Search,
    SortClause,
)
from logicblocks.event.projection.store.adapters.postgres import (
    column_for_query_path,
    operator_for_query_operator,
    sort_direction_for_query_sort_order,
)
from logicblocks.event.types.identifier import event_sequence_identifier

from ... import EventSubscriptionKey
from .base import (
    EventSubscriptionChange,
    EventSubscriptionChangeType,
    EventSubscriptionState,
    EventSubscriptionStore,
)


@dataclass(frozen=True)
class PostgresTableSettings:
    subscriptions_table_name: str

    def __init__(self, *, subscriptions_table_name: str = "subscriptions"):
        object.__setattr__(
            self, "subscriptions_table_name", subscriptions_table_name
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
            .from_table(self._table_settings.subscriptions_table_name)
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
    subscription: EventSubscriptionState,
    table_settings: PostgresTableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            INSERT INTO {0} (
              id,
              "group", 
              event_sources
            )
            VALUES (%s, %s, %s)
              ON CONFLICT (id, "group") 
              DO UPDATE
            SET (event_sources) = ROW(%s);
            """
        ).format(sql.Identifier(table_settings.subscriptions_table_name)),
        [
            subscription.id,
            subscription.group,
            Jsonb([source.dict() for source in subscription.event_sources]),
            Jsonb([source.dict() for source in subscription.event_sources]),
        ],
    )


class PostgresEventSubscriptionStore(EventSubscriptionStore):
    def __init__(
        self,
        *,
        connection_source: ConnectionSource,
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
        self.table_settings = table_settings
        self.query_converter = (
            query_converter
            if query_converter is not None
            else (PostgresQueryConverter().with_default_clause_applicators())
        )

    async def list(self) -> Sequence[EventSubscriptionState]:
        filters: list[FilterClause] = []
        query = self.query_converter.convert_query(Search(filters=filters))
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                results = await cursor.execute(*query)

                subscription_state_dicts = await results.fetchall()

                return [
                    EventSubscriptionState(
                        id=subscription_state_dict["id"],
                        group=subscription_state_dict["group"],
                        event_sources=[
                            event_sequence_identifier(source)
                            for source in subscription_state_dict[
                                "event_sources"
                            ]
                        ],
                    )
                    for subscription_state_dict in subscription_state_dicts
                ]

    async def get(
        self, key: EventSubscriptionKey
    ) -> EventSubscriptionState | None:
        raise NotImplementedError()

    async def add(self, subscription: EventSubscriptionState) -> None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    *insert_query(
                        subscription,
                        self.table_settings,
                    )
                )

    async def remove(self, subscription: EventSubscriptionState) -> None:
        raise NotImplementedError()

    async def replace(self, subscription: EventSubscriptionState) -> None:
        raise NotImplementedError()

    async def apply(self, changes: Sequence[EventSubscriptionChange]) -> None:
        keys = set(change.state.key for change in changes)
        if len(keys) != len(changes):
            raise ValueError(
                "Multiple changes present for same subscription key."
            )

        for change in changes:
            match change.type:
                case EventSubscriptionChangeType.ADD:
                    await self.add(change.state)
                case EventSubscriptionChangeType.REPLACE:
                    await self.replace(change.state)
                case EventSubscriptionChangeType.REMOVE:
                    await self.remove(change.state)
