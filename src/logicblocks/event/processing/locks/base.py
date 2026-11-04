from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class Lock:
    """The outcome of an attempt to acquire a named lock.

    Attributes:
        name: The name of the lock.
        locked: Whether the lock was acquired.
        timed_out: Whether the attempt timed out waiting for the lock.
        wait_time: How long was spent waiting for the lock.
    """

    name: str
    locked: bool
    timed_out: bool = False
    wait_time: timedelta = timedelta(seconds=0)


class LockManager(ABC):
    """Base class for managers of named mutual exclusion locks.

    Both methods are asynchronous context managers yielding a `Lock`
    describing whether the lock was acquired. If acquired, the lock is held
    until the context exits.
    """

    @abstractmethod
    @asynccontextmanager
    def try_lock(self, lock_name: str) -> AsyncGenerator[Lock, None]:
        """Attempt to acquire a lock without waiting.

        Args:
            lock_name: The name of the lock to acquire.

        Yields:
            the lock, with `locked` set to `False` if it is already held.
        """
        raise NotImplementedError()

    @abstractmethod
    @asynccontextmanager
    def wait_for_lock(
        self, lock_name: str, *, timeout: timedelta | None = None
    ) -> AsyncGenerator[Lock, None]:
        """Wait to acquire a lock, optionally up to a timeout.

        Args:
            lock_name: The name of the lock to acquire.
            timeout: The maximum time to wait. If `None`, waits
                indefinitely.

        Yields:
            the lock, with `timed_out` set to `True` if the timeout
                elapsed before the lock was acquired.
        """
        raise NotImplementedError()
