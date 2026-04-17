import asyncio
import warnings
from collections.abc import Awaitable, Callable
from datetime import timedelta
from functools import cached_property
from typing import Any, Never

from .callable import ServiceLike, as_callable_service
from .types import Service


class PollingService(Service[Never]):
    def __init__(
        self,
        service: ServiceLike[Any] | None = None,
        *,
        callable: Callable[[], Awaitable[Any]] | None = None,
        poll_interval: timedelta = timedelta(milliseconds=200),
    ):
        if callable is not None:
            warnings.warn(
                "Use 'service' instead of 'callable'",
                DeprecationWarning,
                stacklevel=2,
            )
            service = service or callable
        if service is None:
            raise TypeError("service is required")
        self._service = as_callable_service(service)
        self._poll_interval = poll_interval

    async def execute(self) -> Never:
        while True:
            await self._service.execute()
            await asyncio.sleep(self._poll_interval.total_seconds())

    @cached_property
    def name(self) -> str:
        return self._service.name
