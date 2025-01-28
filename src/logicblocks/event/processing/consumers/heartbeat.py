from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from psycopg import AsyncConnection, AsyncCursor, sql
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db import PostgresParameterisedQuery
from logicblocks.event.utils.clock import Clock, SystemClock


@dataclass(frozen=True)
class TableSettings:
    nodes_table_name: str

    def __init__(self, *, nodes_table_name: str = "nodes"):
        object.__setattr__(self, "nodes_table_name", nodes_table_name)


def upsert_query(
    node_id: str,
    last_seen: datetime,
    table_settings: TableSettings,
) -> PostgresParameterisedQuery:
    return (
        sql.SQL(
            """
            INSERT INTO {0} (
              id, 
              last_seen
            )
              VALUES (%s, %s)
              ON CONFLICT (id) 
              DO UPDATE
            SET (last_seen) = ROW(%s);
            """
        ).format(sql.Identifier(table_settings.nodes_table_name)),
        [node_id, last_seen, last_seen],
    )


def delete_query(
    node_id: str,
    table_settings: TableSettings,
) -> PostgresParameterisedQuery:
    return (
        sql.SQL(
            """
            DELETE FROM {0}
            WHERE id = %s
            """
        ).format(sql.Identifier(table_settings.nodes_table_name)),
        [node_id],
    )


async def upsert(
    cursor: AsyncCursor[Any],
    *,
    node_id: str,
    last_seen: datetime,
    table_settings: TableSettings,
) -> None:
    await cursor.execute(*upsert_query(node_id, last_seen, table_settings))


async def delete(
    cursor: AsyncCursor[Any],
    *,
    node_id: str,
    table_settings: TableSettings,
) -> None:
    await cursor.execute(*delete_query(node_id, table_settings))


class PostgresNodeHeartbeat:
    def __init__(
        self,
        node_id: str,
        connection_pool: AsyncConnectionPool[AsyncConnection],
        table_settings: TableSettings = TableSettings(),
        clock: Clock = SystemClock(),
    ):
        self.node_id = node_id
        self.connection_pool = connection_pool
        self.table_settings = table_settings
        self.clock = clock

    async def beat(self):
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await upsert(
                    cursor,
                    node_id=self.node_id,
                    last_seen=self.clock.now(UTC),
                    table_settings=self.table_settings,
                )

    async def stop(self):
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await delete(
                    cursor,
                    node_id=self.node_id,
                    table_settings=self.table_settings,
                )
