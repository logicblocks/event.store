import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from .types import LockManager


class InMemoryLockManager(LockManager):
    def __init__(self):
        self._locks: dict[str, bool] = {}

    @asynccontextmanager
    async def with_lock(self, lock_id: str) -> AsyncGenerator[bool, None]:
        await asyncio.sleep(0)
        if self._locks.get(lock_id, False):
            yield False
            return

        self._locks[lock_id] = True
        try:
            yield True
        finally:
            self._locks[lock_id] = False
