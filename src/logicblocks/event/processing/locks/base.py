from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class Lock:
    name: str
    locked: bool
    timed_out: bool = False
    wait_time: timedelta = timedelta(seconds=0)


class LockManager(ABC):
    @abstractmethod
    @asynccontextmanager
    def try_lock(self, lock_name: str) -> AsyncGenerator[Lock, None]:
        raise NotImplementedError()

    @abstractmethod
    @asynccontextmanager
    def wait_for_lock(
        self, lock_name: str, *, timeout: timedelta | None = None
    ) -> AsyncGenerator[Lock, None]:
        raise NotImplementedError()
