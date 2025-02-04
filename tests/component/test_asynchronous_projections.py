import os

import pytest_asyncio
from psycopg import abc, sql, AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db import PostgresConnectionSettings
from logicblocks.event.processing.broker import make_postgres_event_broker
from logicblocks.event.projection import (
    ProjectionStore,
    PostgresProjectionStorageAdapter
)
from logicblocks.event.store import (
    EventStore,
    PostgresEventStorageAdapter
)
from logicblocks.event.testing import data

connection_settings = PostgresConnectionSettings(
    user="admin",
    password="super-secret",
    host="localhost",
    port=5432,
    dbname="some-database",
)

project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)


def relative_to_root(*path_parts: str) -> str:
    return os.path.join(project_root, *path_parts)


def read_sql(sql_file_name: str) -> abc.Query:
    with open(relative_to_root("sql", f"{sql_file_name}.sql")) as f:
        return f.read().encode()


def create_table_query(table: str) -> abc.Query:
    return read_sql(f"create_{table}_table")


def create_indices_query(table: str) -> abc.Query:
    return read_sql(f"create_{table}_indices")


def drop_table_query(table_name: str) -> abc.Query:
    return sql.SQL("DROP TABLE IF EXISTS {0}").format(
        sql.Identifier(table_name)
    )


def truncate_table_query(table_name: str) -> abc.Query:
    return sql.SQL("TRUNCATE {0}").format(sql.Identifier(table_name))


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


@pytest_asyncio.fixture
async def open_connection_pool():
    conninfo = connection_settings.to_connection_string()
    pool = AsyncConnectionPool[AsyncConnection](conninfo, open=False)

    await pool.open()

    try:
        yield pool
    finally:
        await pool.close()


class TestAsynchronousProjections:
    connection_pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.connection_pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "events")
        await drop_table(open_connection_pool, "projections")
        await drop_table(open_connection_pool, "nodes")
        await drop_table(open_connection_pool, "subscribers")
        await drop_table(open_connection_pool, "subscriptions")
        await create_table(open_connection_pool, "events")
        await create_table(open_connection_pool, "projections")
        await create_table(open_connection_pool, "nodes")
        await create_table(open_connection_pool, "subscribers")
        await create_table(open_connection_pool, "subscriptions")

    async def test_projects_from_category(self):
        node_id = data.random_node_id()

        event_storage_adapter = PostgresEventStorageAdapter(
            connection_source=self.connection_pool
        )
        event_store = EventStore(adapter=event_storage_adapter)

        projection_storage_adapter = PostgresProjectionStorageAdapter(
                connection_source=self.connection_pool
            )
        projection_store = ProjectionStore(adapter=projection_storage_adapter)

        event_broker = make_postgres_event_broker(
            node_id=node_id,
            connection_settings=connection_settings,
            connection_pool=self.connection_pool
        )

        
