from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from .types import LockManager


class PostgresLockManager(LockManager):
    @asynccontextmanager
    def with_lock(self, lock_id: str) -> AsyncGenerator[bool, None]:
        raise NotImplementedError()
