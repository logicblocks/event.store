import asyncio
from abc import ABC
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, Never

from .callable import CallableServiceMixin
from .status import StatusAwareServiceMixin
from .types import Service, ServiceMixin


class PollingServiceMixin(ServiceMixin[Never], ABC):
    def __init__(
        self,
        poll_interval: timedelta = timedelta(milliseconds=200),
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._poll_interval = poll_interval

    async def run(self):
        while True:
            await super().run()
            await asyncio.sleep(self._poll_interval.total_seconds())


class CallablePollingService(
    StatusAwareServiceMixin[Never],
    PollingServiceMixin,
    CallableServiceMixin[Any],
    Service[Never],
):
    def __init__(
        self,
        callable: Callable[[], Awaitable[Any]],
        poll_interval: timedelta = timedelta(milliseconds=200),
    ):
        super().__init__(callable=callable, poll_interval=poll_interval)


PollingService = CallablePollingService
