import os
from typing import Any, Mapping, Sequence

import pytest_asyncio
from psycopg import AsyncConnection, abc, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db import PostgresConnectionSettings
from logicblocks.event.projection.store import ProjectionStorageAdapter
from logicblocks.event.projection.store.adapters import (
    PostgresProjectionStorageAdapter,
)
from logicblocks.event.testcases.projection.store.adapters import (
    ProjectionStorageAdapterCases,
)
from logicblocks.event.types import Projection, identifier

connection_settings = PostgresConnectionSettings(
    user="admin",
    password="super-secret",
    host="localhost",
    port=5432,
    dbname="some-database",
)

project_root = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "..", "..", "..", ".."
    )
)


def relative_to_root(*path_parts: str) -> str:
    return os.path.join(project_root, *path_parts)


def create_table_query(table: str) -> abc.Query:
    with open(relative_to_root("sql", "create_projections_table.sql")) as f:
        create_table_sql = f.read().replace("projections", "{0}")

        return create_table_sql.format(table).encode()


def create_indices_query(table: str) -> abc.Query:
    with open(relative_to_root("sql", "create_projections_indices.sql")) as f:
        create_indices_sql = f.read().replace("projections", "{0}")

        return create_indices_sql.format(table).encode()


def drop_table_query(table_name: str) -> abc.Query:
    return sql.SQL("DROP TABLE IF EXISTS {0}").format(
        sql.Identifier(table_name)
    )


def truncate_table_query(table_name: str) -> abc.Query:
    return sql.SQL("TRUNCATE {0}").format(sql.Identifier(table_name))


def read_projections_query(table: str) -> abc.Query:
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


async def read_projections(
    pool: AsyncConnectionPool[AsyncConnection],
    table: str,
) -> Sequence[Projection[Mapping[str, Any]]]:
    async with pool.connection() as connection:
        async with connection.cursor(row_factory=dict_row) as cursor:
            results = await cursor.execute(read_projections_query(table))
            return [
                Projection(
                    id=projection["id"],
                    name=projection["name"],
                    state=projection["state"],
                    version=projection["version"],
                    source=identifier.event_sequence_identifier(
                        projection["source"]
                    ),
                )
                for projection in await results.fetchall()
            ]


@pytest_asyncio.fixture
async def open_connection_pool():
    conninfo = connection_settings.to_connection_string()
    pool = AsyncConnectionPool[AsyncConnection](conninfo, open=False)

    await pool.open()

    try:
        yield pool
    finally:
        await pool.close()


class TestPostgresProjectionStorageAdapterCommonCases(
    ProjectionStorageAdapterCases
):
    pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "projections")
        await create_table(open_connection_pool, "projections")

    def construct_storage_adapter(self) -> ProjectionStorageAdapter:
        return PostgresProjectionStorageAdapter(connection_source=self.pool)

    async def clear_storage(self) -> None:
        await clear_table(self.pool, "events")

    async def retrieve_projections(
        self, *, adapter: ProjectionStorageAdapter
    ) -> Sequence[Projection[Mapping[str, Any]]]:
        return await read_projections(self.pool, "projections")
