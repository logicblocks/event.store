from collections.abc import Sequence

from psycopg import AsyncConnection, sql
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from psycopg_pool.abc import ACT

import logicblocks.event.persistence.postgres as postgres
from logicblocks.event.query import (
    FilterClause,
    Search,
)
from logicblocks.event.types.identifier import event_sequence_identifier

from .base import (
    EventSubscriptionKey,
    EventSubscriptionState,
    EventSubscriptionStateChange,
    EventSubscriptionStateChangeType,
    EventSubscriptionStateStore,
)


def insert_query(
    subscription: EventSubscriptionState,
    table_settings: postgres.TableSettings,
) -> postgres.ParameterisedQuery:
    return (
        sql.SQL(
            """
            INSERT INTO {0} (
              id,
              "group",
              node_id,  
              event_sources
            )
            VALUES (%s, %s, %s, %s);
            """
        ).format(sql.Identifier(table_settings.table_name)),
        [
            subscription.id,
            subscription.group,
            subscription.node_id,
            Jsonb(
                [source.serialise() for source in subscription.event_sources]
            ),
        ],
    )


def upsert_query(
    subscription: EventSubscriptionState,
    table_settings: postgres.TableSettings,
) -> postgres.ParameterisedQuery:
    return (
        sql.SQL(
            """
            UPDATE {0}
            SET event_sources = %s
            WHERE "group" = %s AND id = %s
            RETURNING *;
            """
        ).format(sql.Identifier(table_settings.table_name)),
        [
            Jsonb(
                [source.serialise() for source in subscription.event_sources]
            ),
            subscription.group,
            subscription.id,
        ],
    )


def remove_query(
    subscription: EventSubscriptionState,
    table_settings: postgres.TableSettings,
) -> postgres.ParameterisedQuery:
    return (
        sql.SQL(
            """
            DELETE FROM {0}
            WHERE "group" = %s AND id = %s
            RETURNING *;
            """
        ).format(sql.Identifier(table_settings.table_name)),
        [
            subscription.group,
            subscription.id,
        ],
    )


async def add(
    connection: ACT,
    subscription: EventSubscriptionState,
    table_settings: postgres.TableSettings,
):
    try:
        async with connection.cursor() as cursor:
            await cursor.execute(
                *insert_query(
                    subscription,
                    table_settings,
                )
            )
    except UniqueViolation:
        raise ValueError("Can't add existing subscription.")


async def remove(
    connection: ACT,
    subscription: EventSubscriptionState,
    table_settings: postgres.TableSettings,
):
    async with connection.cursor() as cursor:
        results = await cursor.execute(
            *remove_query(
                subscription,
                table_settings,
            )
        )
        removed_subscriptions = await results.fetchall()
        if len(removed_subscriptions) == 0:
            raise ValueError("Can't remove missing subscription.")


async def replace(
    connection: ACT,
    subscription: EventSubscriptionState,
    table_settings: postgres.TableSettings,
):
    async with connection.cursor() as cursor:
        results = await cursor.execute(
            *upsert_query(
                subscription,
                table_settings,
            )
        )
        updated_subscriptions = await results.fetchall()
        if len(updated_subscriptions) == 0:
            raise ValueError("Can't replace missing subscription.")


class PostgresEventSubscriptionStateStore(EventSubscriptionStateStore):
    def __init__(
        self,
        *,
        node_id: str,
        connection_source: postgres.ConnectionSource,
        table_settings: postgres.TableSettings = postgres.TableSettings(
            table_name="subscriptions"
        ),
        query_converter: postgres.QueryConverter | None = None,
    ):
        if isinstance(connection_source, postgres.ConnectionSettings):
            self._connection_pool_owner = True
            self.connection_pool = AsyncConnectionPool[AsyncConnection](
                connection_source.to_connection_string(), open=False
            )
        else:
            self._connection_pool_owner = False
            self.connection_pool = connection_source

        self.node_id = node_id
        self.table_settings = table_settings
        self.query_converter = (
            query_converter
            if query_converter is not None
            else (
                postgres.QueryConverter(table_settings=table_settings)
                .with_default_clause_converters()
                .with_default_query_converters()
            )
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
                        node_id=subscription_state_dict["node_id"],
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
            await add(
                connection,
                EventSubscriptionState(
                    group=subscription.group,
                    id=subscription.id,
                    node_id=self.node_id,
                    event_sources=subscription.event_sources,
                ),
                self.table_settings,
            )

    async def remove(self, subscription: EventSubscriptionState) -> None:
        async with self.connection_pool.connection() as connection:
            await remove(
                connection,
                EventSubscriptionState(
                    group=subscription.group,
                    id=subscription.id,
                    node_id=self.node_id,
                    event_sources=subscription.event_sources,
                ),
                self.table_settings,
            )

    async def replace(self, subscription: EventSubscriptionState) -> None:
        async with self.connection_pool.connection() as connection:
            await replace(
                connection,
                EventSubscriptionState(
                    group=subscription.group,
                    id=subscription.id,
                    node_id=self.node_id,
                    event_sources=subscription.event_sources,
                ),
                self.table_settings,
            )

    async def apply(
        self, changes: Sequence[EventSubscriptionStateChange]
    ) -> None:
        keys = set(change.subscription.key for change in changes)
        if len(keys) != len(changes):
            raise ValueError(
                "Multiple changes present for same subscription key."
            )

        async with self.connection_pool.connection() as connection:
            for change in changes:
                state = EventSubscriptionState(
                    group=change.subscription.group,
                    id=change.subscription.id,
                    node_id=self.node_id,
                    event_sources=change.subscription.event_sources,
                )
                match change.type:
                    case EventSubscriptionStateChangeType.ADD:
                        await add(connection, state, self.table_settings)
                    case EventSubscriptionStateChangeType.REPLACE:
                        await replace(connection, state, self.table_settings)
                    case EventSubscriptionStateChangeType.REMOVE:
                        await remove(connection, state, self.table_settings)
