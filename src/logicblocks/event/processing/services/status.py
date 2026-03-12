import asyncio
from functools import cached_property
from typing import Any

from ..process.base import ProcessStatus
from .types import Service


class StatusTrackingService[T = Any](Service[T]):
    def __init__(self, service: Service[T]):
        self._service = service
        self._status = ProcessStatus.INITIALISED

    @property
    def status(self) -> ProcessStatus:
        return self._status

    async def execute(self) -> T:
        try:
            self._status = ProcessStatus.RUNNING
            result = await self._service.run()
            self._status = ProcessStatus.STOPPED
            return result
        except asyncio.CancelledError:
            self._status = ProcessStatus.STOPPED
            raise
        except BaseException:
            self._status = ProcessStatus.ERRORED
            raise

    @cached_property
    def name(self) -> str:
        return self._service.name
