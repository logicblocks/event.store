import asyncio
from abc import ABC
from typing import Any

from ..process import HasProcessStatus, ProcessStatus
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
            result = await self._service.execute()
            self._status = ProcessStatus.STOPPED
            return result
        except asyncio.CancelledError:
            self._status = ProcessStatus.STOPPED
            raise
        except BaseException:
            self._status = ProcessStatus.ERRORED
            raise

    def __repr__(self):
        return f"{self.__class__.__name__}({self._service!r})"


class DelegateServiceStatusTrackingMixin(ABC):
    _service: Service

    @property
    def status(self) -> ProcessStatus:
        if isinstance(self._service, HasProcessStatus):
            return self._service.status
        else:
            return ProcessStatus.UNKNOWN
