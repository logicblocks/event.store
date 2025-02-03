import os
from collections.abc import Mapping, Sequence
from typing import Any

import pytest_asyncio
from psycopg import AsyncConnection, abc, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db import PostgresConnectionSettings
from logicblocks.event.processing.broker import (
    NodeState,
    NodeStateStore,
    PostgresNodeStateStore,
)
from logicblocks.event.testcases.processing.broker.nodes.stores.state import (
    NodeStateStoreCases,
)
from logicblocks.event.utils.clock import Clock

connection_settings = PostgresConnectionSettings(
    user="admin",
    password="super-secret",
    host="localhost",
    port=5432,
    dbname="some-database",
)

project_root = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "..",
        "..",
        "..",
        "..",
        "..",
        "..",
        "..",
    )
)


def relative_to_root(*path_parts: str) -> str:
    return os.path.join(project_root, *path_parts)


def create_table_query(table: str) -> abc.Query:
    with open(relative_to_root("sql", "create_nodes_table.sql")) as f:
        create_table_sql = f.read().replace("nodes", "{0}")

        return create_table_sql.format(table).encode()


def create_indices_query(table: str) -> abc.Query:
    with open(relative_to_root("sql", "create_nodes_indices.sql")) as f:
        create_indices_sql = f.read().replace("nodes", "{0}")

        return create_indices_sql.format(table).encode()


def drop_table_query(table_name: str) -> abc.Query:
    return sql.SQL("DROP TABLE IF EXISTS {0}").format(
        sql.Identifier(table_name)
    )


def truncate_table_query(table_name: str) -> abc.Query:
    return sql.SQL("TRUNCATE {0}").format(sql.Identifier(table_name))


def read_nodes_query(table: str) -> abc.Query:
    return sql.SQL("SELECT * FROM {0}").format(sql.Identifier(table))


async def create_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(create_table_query(table))
        await connection.execute(create_indices_query(table))


async def clear_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(truncate_table_query(table))


async def drop_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(drop_table_query(table))


async def read_nodes(
    pool: AsyncConnectionPool[AsyncConnection],
    table: str,
) -> Sequence[Mapping[str, Any]]:
    async with pool.connection() as connection:
        async with connection.cursor(row_factory=dict_row) as cursor:
            results = await cursor.execute(read_nodes_query(table))
            return await results.fetchall()


@pytest_asyncio.fixture
async def open_connection_pool():
    conninfo = connection_settings.to_connection_string()
    pool = AsyncConnectionPool[AsyncConnection](conninfo, open=False)

    await pool.open()

    try:
        yield pool
    finally:
        await pool.close()


class TestPostgresNodeStateStore(NodeStateStoreCases):
    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "nodes")
        await create_table(open_connection_pool, "nodes")

    def construct_store(self, clock: Clock) -> NodeStateStore:
        return PostgresNodeStateStore(connection_pool=self.pool, clock=clock)

    async def read_nodes(self, store: NodeStateStore) -> Sequence[NodeState]:
        return [
            NodeState(node["id"], node["last_seen"])
            for node in await read_nodes(self.pool, "nodes")
        ]
