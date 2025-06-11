import os
import sys

import pytest
import pytest_asyncio
from psycopg import AsyncConnection, abc, sql
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.processing.broker.strategies.distributed import (
    EventSubscriberStateStore,
    PostgresEventSubscriberStateStore,
)
from logicblocks.event.testcases import (
    EventSubscriberStateStoreCases,
)
from logicblocks.event.testsupport import (
    connection_pool,
    create_table,
    drop_table,
)
from logicblocks.event.utils.clock import Clock

connection_settings = ConnectionSettings(
    user="admin",
    password="super-secret",
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname="some-database",
)


def read_subscriber_states_query(table: str) -> abc.Query:
    return sql.SQL("SELECT * FROM {0} ORDER BY last_seen").format(
        sql.Identifier(table)
    )


@pytest_asyncio.fixture
async def open_connection_pool():
    async with connection_pool(connection_settings) as pool:
        yield pool


class TestPostgresEventSubscriberStateStore(EventSubscriberStateStoreCases):
    pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "subscribers")
        await create_table(open_connection_pool, "subscribers")

    def construct_store(
        self, node_id: str, clock: Clock
    ) -> EventSubscriberStateStore:
        return PostgresEventSubscriberStateStore(
            node_id=node_id, connection_source=self.pool, clock=clock
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
