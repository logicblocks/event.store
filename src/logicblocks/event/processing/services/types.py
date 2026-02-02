import asyncio
from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Any



class ServiceStatus(StrEnum):
    INITIALISED = "initialised"
    RUNNING = "running"
    STOPPED = "stopped"
    CANCELLED = "cancelled"
    ERRORED = "errored"


class Service[T = Any](ABC):
    @property
    def name(self) -> str:
        return self.__class__.__name__.removesuffix("Service")

    async def run(self) -> T:
        return await self.execute()

    @abstractmethod
    async def execute(self) -> T:
        raise NotImplementedError()

class StatusAwareServiceMixin[T = Any](Service[T], ABC):
    def __init__(self):
        super().__init__()
        self._status: ServiceStatus = ServiceStatus.INITIALISED

    @property
    def status(self) -> ServiceStatus:
        return getattr(self, "_status", ServiceStatus.INITIALISED)

    async def run(self) -> T:
        self._status = ServiceStatus.RUNNING
        try:
            result = await super().execute()
            self._status = ServiceStatus.STOPPED
            return result
        except asyncio.CancelledError:
            self._status = ServiceStatus.CANCELLED
            raise
        except BaseException:
            self._status = ServiceStatus.ERRORED
            raise
