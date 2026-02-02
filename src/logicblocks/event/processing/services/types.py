import asyncio
from abc import ABC, abstractmethod
from enum import StrEnum
from functools import cached_property
from typing import Any, Protocol, runtime_checkable


class ServiceStatus(StrEnum):
    INITIALISED = "initialised"
    RUNNING = "running"
    STOPPED = "stopped"
    CANCELLED = "cancelled"
    ERRORED = "errored"


@runtime_checkable
class HasServiceStatus(Protocol):
    @property
    def status(self) -> ServiceStatus: ...


class Service[T = Any](ABC):
    @abstractmethod
    async def execute(self) -> T:
        raise NotImplementedError()

    @cached_property
    def name(self) -> str:
        return self.__class__.__name__.removesuffix("Service")

    async def run(self):
        return await self.execute()


class StatusAwareServiceMixin[T = Any](Service[T], HasServiceStatus, ABC):
    def __init__(self):
        super().__init__()
        self._status: ServiceStatus = ServiceStatus.INITIALISED

    @property
    def status(self) -> ServiceStatus:
        return getattr(self, "_status", ServiceStatus.INITIALISED)

    async def run(self) -> T:
        self._status = ServiceStatus.RUNNING
        try:
            result = await super().run()
            self._status = ServiceStatus.STOPPED
            return result
        except asyncio.CancelledError:
            self._status = ServiceStatus.CANCELLED
            raise
        except BaseException:
            self._status = ServiceStatus.ERRORED
            raise
