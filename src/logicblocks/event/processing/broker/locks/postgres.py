from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from .base import Lock, LockManager


class PostgresLockManager(LockManager):
    @asynccontextmanager
    def try_lock(self, lock_name: str) -> AsyncGenerator[Lock, None]:
        raise NotImplementedError()

    @asynccontextmanager
    def wait_for_lock(self, lock_name: str) -> AsyncGenerator[Lock, None]:
        raise NotImplementedError()

    # wait for lock -> pg_advisory_xact_lock
    #               -> pg_advisory_lock
    # try lock -> pg_try_advisory_xact_lock
    #          -> pg_try_advisory_lock
    # not sure which to use
    #
    # should the postgres lock manager manage its own dedicated connection?
    # how long running can this connection be?
