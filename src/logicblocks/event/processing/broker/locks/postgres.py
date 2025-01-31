import hashlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta

from psycopg import AsyncConnection
from psycopg.rows import scalar_row
from psycopg.sql import SQL
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db.postgres import ConnectionSettings, ConnectionSource

from .base import Lock, LockManager


def get_digest(lock_id: str) -> int:
    return (
        int(hashlib.sha256(lock_id.encode("utf-8")).hexdigest(), 16) % 10**16
    )


class PostgresLockManager(LockManager):
    connection_pool: AsyncConnectionPool[AsyncConnection]

    def __init__(self, connection_source: ConnectionSource):
        if isinstance(connection_source, ConnectionSettings):
            self._connection_pool_owner = True
            self.connection_pool = AsyncConnectionPool[AsyncConnection](
                connection_source.to_connection_string(), open=False
            )
        else:
            self._connection_pool_owner = False
            self.connection_pool = connection_source

    @asynccontextmanager
    async def try_lock(self, lock_name: str) -> AsyncGenerator[Lock, None]:
        async with self.connection_pool.connection() as conn:
            async with conn.cursor(row_factory=scalar_row) as cursor:
                lock_result = await cursor.execute(
                    SQL("SELECT pg_try_advisory_xact_lock(%(lock_id)s)"),
                    {"lock_id": get_digest(lock_name)},
                )
                r = await lock_result.fetchone()
                if r:
                    yield Lock(
                        name=lock_name,
                        locked=r,
                        timed_out=False,
                    )
                else:
                    yield Lock(
                        name=lock_name,
                        locked=False,
                        timed_out=False,
                    )

    @asynccontextmanager
    def wait_for_lock(
        self, lock_name: str, *, timeout: timedelta | None = None
    ) -> AsyncGenerator[Lock, None]:
        raise NotImplementedError()

    # wait for lock -> pg_advisory_xact_lock
    #               -> pg_advisory_lock
    # try lock -> pg_try_advisory_xact_lock
    #          -> pg_try_advisory_lock
    # not sure which to use
    #
    # should the postgres lock manager manage its own dedicated connection?
    # how long running can this connection be?
