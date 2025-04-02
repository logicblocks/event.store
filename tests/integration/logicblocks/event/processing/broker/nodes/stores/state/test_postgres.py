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
from logicblocks.event.testsupport import (
    connection_pool,
    create_table,
    drop_table,
)
from logicblocks.event.utils.clock import Clock

connection_settings = PostgresConnectionSettings(
    user="admin",
    password="super-secret",
    host="localhost",
    port=5432,
    dbname="some-database",
)


def read_nodes_query(table: str) -> abc.Query:
    return sql.SQL("SELECT * FROM {0}").format(sql.Identifier(table))


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
    async with connection_pool(connection_settings) as pool:
        yield pool


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
