from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager


class LockManager(ABC):
    @abstractmethod
    @asynccontextmanager
    def with_lock(self, lock_id: str) -> AsyncGenerator[bool, None]:
        raise NotImplementedError()
