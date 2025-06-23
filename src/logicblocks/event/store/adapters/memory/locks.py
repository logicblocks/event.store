from collections.abc import Sequence
from types import TracebackType
from typing import Any

from aiologic import Lock


class MultiLock:
    def __init__(self, locks: Sequence[Lock]):
        self._locks = list(locks)

    async def __aenter__(self) -> Any:
        for lock in self._locks:
            await lock.async_acquire()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        for lock in reversed(self._locks):
            lock.async_release()
