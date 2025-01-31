import pytest_asyncio
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db import PostgresConnectionSettings
from logicblocks.event.processing.broker import LockManager
from logicblocks.event.processing.broker.locks.postgres import (
    PostgresLockManager,
)
from logicblocks.event.testcases.processing.broker.locks.lock_manager import (
    BaseTestLockManager,
)

connection_settings = PostgresConnectionSettings(
    user="admin",
    password="super-secret",
    host="localhost",
    port=5432,
    dbname="some-database",
)


@pytest_asyncio.fixture
async def open_connection_pool():
    conninfo = connection_settings.to_connection_string()
    pool = AsyncConnectionPool[AsyncConnection](conninfo, open=False)

    await pool.open()

    try:
        yield pool
    finally:
        await pool.close()


class TestPostgresLockManager(BaseTestLockManager):
    pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    def construct_lock_manager(self) -> LockManager:
        return PostgresLockManager(connection_source=self.pool)

    async def test_does_not_leak_when_try_lock_on_many_different_locks(self):
        pass

    async def test_wait_to_obtain_lock_and_succeed(self):
        pass

    async def test_wait_to_obtain_two_locks(self):
        pass

    async def test_wait_to_obtain_lock_and_timeout(self):
        pass

    async def test_wait_to_obtain_lock_and_wait_for_release(self):
        pass

    async def test_does_not_leak_when_wait_for_lock_with_many_different_locks(
        self,
    ):
        pass
