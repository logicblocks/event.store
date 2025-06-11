import os
import sys

import pytest
import pytest_asyncio
from psycopg import AsyncConnection, abc, sql
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.processing.broker.strategies.distributed import (
    EventSubscriptionState,
    EventSubscriptionStateChange,
    EventSubscriptionStateChangeType,
    EventSubscriptionStateStore,
    PostgresEventSubscriptionStateStore,
)
from logicblocks.event.testcases import (
    EventSubscriptionStateStoreCases,
)
from logicblocks.event.testing import data
from logicblocks.event.testsupport import (
    connection_pool,
    create_table,
    drop_table,
)
from logicblocks.event.types import CategoryIdentifier

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


class TestPostgresEventSubscriptionStateStore(
    EventSubscriptionStateStoreCases
):
    pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "subscriptions")
        await create_table(open_connection_pool, "subscriptions")

    def construct_store(self, node_id: str) -> EventSubscriptionStateStore:
        return PostgresEventSubscriptionStateStore(
            node_id=node_id, connection_source=self.pool
        )

    async def test_does_not_partially_apply_changes(self):
        node_id = data.random_node_id()
        store = self.construct_store(node_id=node_id)

        addition = EventSubscriptionStateChange(
            type=EventSubscriptionStateChangeType.ADD,
            subscription=EventSubscriptionState(
                group=data.random_subscriber_group(),
                id=data.random_subscriber_id(),
                node_id=data.random_node_id(),
                event_sources=[
                    CategoryIdentifier(data.random_event_category_name())
                ],
            ),
        )

        removal = EventSubscriptionStateChange(
            type=EventSubscriptionStateChangeType.REMOVE,
            subscription=EventSubscriptionState(
                group=data.random_subscriber_group(),
                id=data.random_subscriber_id(),
                node_id=data.random_node_id(),
                event_sources=[
                    CategoryIdentifier(data.random_event_category_name())
                ],
            ),
        )

        with pytest.raises(ValueError):
            await store.apply(changes=[addition, removal])

        states = await store.list()

        assert states == []


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
