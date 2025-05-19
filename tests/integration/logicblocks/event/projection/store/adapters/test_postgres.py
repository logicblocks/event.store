from typing import Sequence

import pytest_asyncio
from psycopg import AsyncConnection, abc, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.projection.store import ProjectionStorageAdapter
from logicblocks.event.projection.store.adapters import (
    PostgresProjectionStorageAdapter,
)
from logicblocks.event.testcases.projection.store.adapters import (
    ProjectionStorageAdapterCases,
)
from logicblocks.event.testsupport import (
    clear_table,
    connection_pool,
    create_table,
    drop_table,
)
from logicblocks.event.testsupport.db import (
    enable_extension,
)
from logicblocks.event.types import JsonValue, Projection, identifier

connection_settings = ConnectionSettings(
    user="admin",
    password="super-secret",
    host="localhost",
    port=5432,
    dbname="some-database",
)


def read_projections_query(table: str) -> abc.Query:
    return sql.SQL("SELECT * FROM {0}").format(sql.Identifier(table))


async def read_projections(
    pool: AsyncConnectionPool[AsyncConnection],
    table: str,
) -> Sequence[Projection[JsonValue, JsonValue]]:
    async with pool.connection() as connection:
        async with connection.cursor(row_factory=dict_row) as cursor:
            results = await cursor.execute(read_projections_query(table))
            return [
                Projection(
                    id=projection["id"],
                    name=projection["name"],
                    source=identifier.event_sequence_identifier(
                        projection["source"]
                    ),
                    state=projection["state"],
                    metadata=projection["metadata"],
                )
                for projection in await results.fetchall()
            ]


@pytest_asyncio.fixture
async def open_connection_pool():
    async with connection_pool(connection_settings) as pool:
        yield pool


class TestPostgresProjectionStorageAdapterCommonCases(
    ProjectionStorageAdapterCases
):
    pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await enable_extension(open_connection_pool, "pg_trgm")
        await drop_table(open_connection_pool, "projections")
        await create_table(open_connection_pool, "projections")

    def construct_storage_adapter(self) -> ProjectionStorageAdapter:
        return PostgresProjectionStorageAdapter(connection_source=self.pool)

    async def clear_storage(self) -> None:
        await clear_table(self.pool, "events")

    async def retrieve_projections(
        self, *, adapter: ProjectionStorageAdapter
    ) -> Sequence[Projection[JsonValue, JsonValue]]:
        return await read_projections(self.pool, "projections")
