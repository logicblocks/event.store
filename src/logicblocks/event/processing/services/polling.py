import asyncio
from datetime import timedelta
from functools import cached_property
from typing import Any, Never

from .types import Service


class PollingService(Service[Never]):
    def __init__(
        self,
        service: Service[Any],
        poll_interval: timedelta = timedelta(milliseconds=200),
    ):
        self._service = service
        self._poll_interval = poll_interval

    async def execute(self) -> Never:
        while True:
            await self._service.run()
            await asyncio.sleep(self._poll_interval.total_seconds())

    @cached_property
    def name(self) -> str:
        return self._service.name
