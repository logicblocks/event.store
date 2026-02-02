import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from .types import Service


class PollingService[T = Any](Service[T]):
    _callable: Callable[[], Awaitable[T]]
    _poll_interval: timedelta = timedelta(milliseconds=200)

    def __init__(
        self,
        callable: Callable[[], Awaitable[T]],
        poll_interval: timedelta = timedelta(milliseconds=200),
    ):
        self._callable = callable
        self._poll_interval = poll_interval

    async def execute(self):
        while True:
            await self._callable()
            await asyncio.sleep(self._poll_interval.total_seconds())
