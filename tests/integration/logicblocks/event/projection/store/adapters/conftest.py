import os

import pytest
import pytest_asyncio
from logicblocks.event.testcases.projection.store.harnesses import (
    InMemoryProjectionStorageAdapterHarness,
    PostgresProjectionStoreAdapterHarness,
    ProjectionStoreAdapterHarness,
)
from logicblocks.event.testsupport import (
    connection_pool,
    create_table,
    drop_table,
)
from logicblocks.event.testsupport.db import enable_extension
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings

connection_settings = ConnectionSettings(
    user="admin",
    password="super-secret",
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname="some-database",
)


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def open_connection_pool():
    async with connection_pool(connection_settings) as pool:
        yield pool


async def _reinitialise_storage(
    open_connection_pool: AsyncConnectionPool[AsyncConnection],
):
    await enable_extension(open_connection_pool, "pg_trgm")
    await drop_table(open_connection_pool, "projections")
    await create_table(open_connection_pool, "projections")


@pytest.fixture
async def postgres_projection_store_adapter_harness(open_connection_pool):
    await _reinitialise_storage(open_connection_pool)
    yield PostgresProjectionStoreAdapterHarness(open_connection_pool)


@pytest.fixture
def in_memory_projection_storage_adapter_harness():
    return InMemoryProjectionStorageAdapterHarness()


@pytest.fixture(
    params=[
        in_memory_projection_storage_adapter_harness,
        postgres_projection_store_adapter_harness,
    ]
)
def harness(request) -> ProjectionStoreAdapterHarness:
    return request.getfixturevalue(request.param.__name__)
