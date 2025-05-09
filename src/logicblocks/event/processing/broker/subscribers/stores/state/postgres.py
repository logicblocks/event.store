from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

import logicblocks.event.persistence.postgres as postgres
from logicblocks.event.query import (
    FilterClause,
    Operator,
    Path,
    Search,
)
from logicblocks.event.types.identifier import event_sequence_identifier
from logicblocks.event.utils.clock import Clock, SystemClock

from ....types import EventSubscriber
from .base import EventSubscriberState, EventSubscriberStateStore


def insert_query(
    subscriber: EventSubscriberState,
    table_settings: postgres.TableSettings,
) -> postgres.ParameterisedQuery:
    subscription_requests_jsonb = Jsonb(
        [
            subscription_request.serialise()
            for subscription_request in subscriber.subscription_requests
        ]
    )
    return (
        sql.SQL(
            """
            INSERT INTO {0} ("group",
                             id,
                             node_id,
                             subscription_requests,
                             last_seen)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT ("group", id)
                DO
            UPDATE
                SET (subscription_requests, last_seen) = ROW (%s, %s);
            """
        ).format(sql.Identifier(table_settings.table_name)),
        [
            subscriber.group,
            subscriber.id,
            subscriber.node_id,
            subscription_requests_jsonb,
            subscriber.last_seen,
            subscription_requests_jsonb,
            subscriber.last_seen,
        ],
    )


def delete_query(
    key: EventSubscriber,
    table_settings: postgres.TableSettings,
) -> postgres.ParameterisedQuery:
    return (
        sql.SQL(
            """
            DELETE
            FROM {0}
            WHERE "group" = %s
              AND id = %s
                RETURNING *;
            """
        ).format(sql.Identifier(table_settings.table_name)),
        [key.group, key.id],
    )


def heartbeat_query(
    subscriber: EventSubscriberState,
    table_settings: postgres.TableSettings,
) -> postgres.ParameterisedQuery:
    return (
        sql.SQL(
            """
            UPDATE {0}
            SET last_seen = %s
            WHERE "group" = %s
              AND id = %s
                RETURNING *;
            """
        ).format(sql.Identifier(table_settings.table_name)),
        [
            subscriber.last_seen,
            subscriber.group,
            subscriber.id,
        ],
    )


def purge_query(
    cutoff_time: datetime,
    table_settings: postgres.TableSettings,
) -> postgres.ParameterisedQuery:
    return (
        sql.SQL(
            """
            DELETE
            FROM {0}
            WHERE last_seen <= %s;
            """
        ).format(sql.Identifier(table_settings.table_name)),
        [
            cutoff_time,
        ],
    )


class PostgresEventSubscriberStateStore(EventSubscriberStateStore):
    def __init__(
        self,
        *,
        node_id: str,
        connection_source: postgres.ConnectionSource,
        clock: Clock = SystemClock(),
        table_settings: postgres.TableSettings = postgres.TableSettings(
            table_name="subscribers"
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
        self.clock = clock

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

    async def add(self, subscriber: EventSubscriber) -> None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    *insert_query(
                        EventSubscriberState(
                            id=subscriber.id,
                            group=subscriber.group,
                            node_id=self.node_id,
                            subscription_requests=subscriber.subscription_requests,
                            last_seen=self.clock.now(UTC),
                        ),
                        self.table_settings,
                    )
                )

    async def remove(self, subscriber: EventSubscriber) -> None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    *delete_query(subscriber, self.table_settings)
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
                    self.clock.now(UTC) - max_time_since_last_seen,
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
                        node_id=subscriber_state_dict["node_id"],
                        subscription_requests=[
                            event_sequence_identifier(subscription_request)
                            for subscription_request in subscriber_state_dict[
                                "subscription_requests"
                            ]
                        ],
                        last_seen=subscriber_state_dict["last_seen"],
                    )
                    for subscriber_state_dict in subscriber_state_dicts
                ]

    async def heartbeat(self, subscriber: EventSubscriber) -> None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await cursor.execute(
                    *heartbeat_query(
                        EventSubscriberState(
                            id=subscriber.id,
                            group=subscriber.group,
                            node_id=self.node_id,
                            subscription_requests=subscriber.subscription_requests,
                            last_seen=self.clock.now(UTC),
                        ),
                        self.table_settings,
                    )
                )

    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(minutes=5)
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
