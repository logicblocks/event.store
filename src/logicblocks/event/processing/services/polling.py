import asyncio
import warnings
from builtins import callable as is_callable
from datetime import timedelta
from typing import Any, Never

from .base import Service
from .callable import CallableService, CallableServiceCallable


class PollingService(Service[Never]):
    def __init__(
        self,
        service: Service[Any] | CallableServiceCallable[Any] | None = None,
        *,
        callable: CallableServiceCallable[Any] | None = None,
        poll_interval: timedelta = timedelta(milliseconds=200),
    ):
        if callable is not None:
            warnings.warn(
                "Use 'service' instead of 'callable'",
                DeprecationWarning,
                stacklevel=2,
            )

            if service is None:
                service = callable
        elif is_callable(service):
            warnings.warn(
                "Wrap callable in CallableService",
                DeprecationWarning,
                stacklevel=2,
            )

        if service is None:
            raise TypeError("service is required")

        self._service = CallableService.from_maybe_callable(service)
        self._poll_interval = poll_interval

    async def execute(self) -> Never:
        while True:
            await self._service.execute()
            await asyncio.sleep(self._poll_interval.total_seconds())

    def __repr__(self):
        return f"{self.__class__.__name__}({self._service!r})"
