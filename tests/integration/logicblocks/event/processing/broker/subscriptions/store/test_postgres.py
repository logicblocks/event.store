import os
import sys

import pytest
import pytest_asyncio
from psycopg import AsyncConnection, abc, sql
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db import PostgresConnectionSettings
from logicblocks.event.processing.broker import EventSubscriptionStore
from logicblocks.event.processing.broker.subscriptions.store.postgres import (
    PostgresEventSubscriptionStore,
)
from logicblocks.event.testcases.processing.subscriptions.store import (
    BaseTestEventSubscriptionStore,
)

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
    )
)


def relative_to_root(*path_parts: str) -> str:
    return os.path.join(project_root, *path_parts)


def create_table_query(table: str) -> abc.Query:
    with open(relative_to_root("sql", "create_subscriptions_table.sql")) as f:
        create_table_sql = f.read().replace("subscriptions", "{0}")

        return create_table_sql.format(table).encode()


def create_indices_query(table: str) -> abc.Query:
    with open(
        relative_to_root("sql", "create_subscriptions_indices.sql")
    ) as f:
        create_indices_sql = f.read().replace("subscriptions", "{0}")

        return create_indices_sql.format(table).encode()


def drop_table_query(table_name: str) -> abc.Query:
    return sql.SQL("DROP TABLE IF EXISTS {0}").format(
        sql.Identifier(table_name)
    )


def truncate_table_query(table_name: str) -> abc.Query:
    return sql.SQL("TRUNCATE {0}").format(sql.Identifier(table_name))


def read_subscriber_states_query(table: str) -> abc.Query:
    return sql.SQL("SELECT * FROM {0} ORDER BY last_seen").format(
        sql.Identifier(table)
    )


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


class TestPostgresEventSubscriptionStore(BaseTestEventSubscriptionStore):
    pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "subscriptions")
        await create_table(open_connection_pool, "subscriptions")

    def construct_store(self) -> EventSubscriptionStore:
        return PostgresEventSubscriptionStore(connection_source=self.pool)

    async def test_replaces_single_existing_subscription(self):
        pass

    async def test_replaces_many_existing_subscriptions(self):
        pass

    async def test_raises_if_replacing_missing_subscription(self):
        pass

    async def test_removes_single_subscription(self):
        pass

    async def test_removes_many_subscriptions(self):
        pass

    async def test_raises_if_removing_missing_subscription(self):
        pass

    async def test_raises_if_multiple_changes_for_same_subscription_key(self):
        pass


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
