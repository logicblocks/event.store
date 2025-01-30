from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

from psycopg import AsyncConnection, sql
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db.postgres import (
    ConnectionSettings,
    ConnectionSource,
    ParameterisedQuery,
)
from logicblocks.event.processing.broker.types import EventSubscriber
from logicblocks.event.utils.clock import Clock, SystemClock

from .base import EventSubscriberState, EventSubscriberStore


@dataclass(frozen=True)
class PostgresTableSettings:
    subscribers_table_name: str

    def __init__(self, *, subscribers_table_name: str = "subscribers"):
        object.__setattr__(
            self, "subscribers_table_name", subscribers_table_name
        )


def insert_query(
    subscriber: EventSubscriberState,
    table_settings: PostgresTableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            INSERT INTO {0} (
              id,
              "group", 
              last_seen
            )
            VALUES (%s, %s, %s)
              ON CONFLICT (id) 
              DO UPDATE
            SET ("group", last_seen) = ROW(%s, %s);
            """
        ).format(sql.Identifier(table_settings.subscribers_table_name)),
        [
            subscriber.id,
            subscriber.group,
            subscriber.last_seen,
            subscriber.group,
            subscriber.last_seen,
        ],
    )


class PostgresEventSubscriberStore(EventSubscriberStore):
    def __init__(
        self,
        *,
        connection_source: ConnectionSource,
        clock: Clock = SystemClock(),
        table_settings: PostgresTableSettings = PostgresTableSettings(),
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

    async def list(
        self,
        subscriber_group: str | None = None,
        max_time_since_last_seen: timedelta | None = None,
    ) -> Sequence[EventSubscriberState]:
        raise NotImplementedError()

    async def heartbeat(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError()

    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(seconds=300)
    ) -> None:
        raise NotImplementedError()
