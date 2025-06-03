import asyncio
import time
from collections import defaultdict
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta

from .base import Lock, LockManager


class InMemoryLockManager(LockManager):
    def __init__(self):
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    @asynccontextmanager
    async def try_lock(self, lock_name: str) -> AsyncGenerator[Lock, None]:
        await asyncio.sleep(0)

        control = self._locks[lock_name]
        if control.locked():
            yield Lock(name=lock_name, locked=False)
            return

        await control.acquire()

        try:
            yield Lock(name=lock_name, locked=True)
        finally:
            control.release()
            if lock_name in self._locks:
                self._locks.pop(lock_name)

    @asynccontextmanager
    async def wait_for_lock(
        self, lock_name: str, *, timeout: timedelta | None = None
    ) -> AsyncGenerator[Lock, None]:
        start = time.monotonic_ns()

        await asyncio.sleep(0)

        control = self._locks[lock_name]
        timed_out = False
        locked = False

        try:
            try:
                locked = await asyncio.wait_for(
                    control.acquire(),
                    timeout=(
                        timeout.total_seconds()
                        if timeout is not None
                        else None
                    ),
                )
            except TimeoutError:
                timed_out = True

            end = time.monotonic_ns()
            wait_time = (end - start) / 1000 / 1000

            yield Lock(
                name=lock_name,
                locked=(not timed_out),
                timed_out=timed_out,
                wait_time=timedelta(milliseconds=wait_time),
            )
        finally:
            if locked:
                control.release()
            if lock_name in self._locks:
                self._locks.pop(lock_name)
