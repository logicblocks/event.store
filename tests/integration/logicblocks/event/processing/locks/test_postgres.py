import os

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.processing import (
    LockManager,
    PostgresLockManager,
)
from logicblocks.event.testcases import (
    LockManagerCases,
)

connection_settings = ConnectionSettings(
    user="admin",
    password="super-secret",
    host=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", 5432),
    dbname="some-database",
)


class TestPostgresLockManager(LockManagerCases):
    def construct_lock_manager(self) -> LockManager:
        return PostgresLockManager(connection_settings=connection_settings)
