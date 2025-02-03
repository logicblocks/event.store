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


class TestPostgresLockManager(BaseTestLockManager):
    def construct_lock_manager(self) -> LockManager:
        return PostgresLockManager(connection_settings=connection_settings)
