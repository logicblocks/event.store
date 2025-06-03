import hashlib
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

from psycopg import AsyncConnection, AsyncCursor
from psycopg.errors import LockNotAvailable
from psycopg.rows import scalar_row
from psycopg.sql import SQL

from logicblocks.event.persistence.postgres import ConnectionSettings

from .base import Lock, LockManager


def get_digest(lock_id: str) -> int:
    return (
        int(hashlib.sha256(lock_id.encode("utf-8")).hexdigest(), 16) % 10**16
    )


async def _try_lock(cursor: AsyncCursor[Any], lock_name: str) -> bool:
    lock_result = await cursor.execute(
        SQL("SELECT pg_try_advisory_xact_lock({0})").format(
            get_digest(lock_name)
        ),
    )
    return bool(await lock_result.fetchone())


async def _try_wait_lock(
    cursor: AsyncCursor[Any], lock_name: str, timeout: timedelta | None = None
) -> bool:
    try:
        await cursor.execute(
            SQL(
                """
                SET lock_timeout TO '{0}ms';
                SELECT pg_advisory_xact_lock({1});
                """
            ).format(
                timeout.microseconds / 1000 if timeout else 0,
                get_digest(lock_name),
            ),
        )

    except LockNotAvailable:
        return False
    return True


class PostgresLockManager(LockManager):
    def __init__(self, connection_settings: ConnectionSettings):
        self.connection_settings = connection_settings

    @asynccontextmanager
    async def try_lock(self, lock_name: str) -> AsyncGenerator[Lock, None]:
        async with await AsyncConnection.connect(
            self.connection_settings.to_connection_string()
        ) as conn:
            async with conn.cursor(row_factory=scalar_row) as cursor:
                locked = await _try_lock(cursor, lock_name)
                yield Lock(
                    name=lock_name,
                    locked=locked,
                    timed_out=False,
                )

    @asynccontextmanager
    async def wait_for_lock(
        self, lock_name: str, *, timeout: timedelta | None = None
    ) -> AsyncGenerator[Lock, None]:
        start = time.monotonic_ns()

        async with await AsyncConnection.connect(
            self.connection_settings.to_connection_string()
        ) as conn:
            async with conn.cursor(row_factory=scalar_row) as cursor:
                locked = await _try_wait_lock(cursor, lock_name, timeout)

                end = time.monotonic_ns()
                wait_time = (end - start) / 1000 / 1000

                yield Lock(
                    name=lock_name,
                    locked=locked,
                    timed_out=(not locked),
                    wait_time=timedelta(milliseconds=wait_time),
                )
