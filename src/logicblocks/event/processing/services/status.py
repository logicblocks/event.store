import asyncio
from abc import ABC
from typing import Any

from ..process.base import (
    ProcessStatus,
)
from .types import ServiceMixin


class StatusAwareServiceMixin[T = Any](ServiceMixin[T], ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._status = ProcessStatus.INITIALISED

    @property
    def status(self) -> ProcessStatus:
        return self._status

    async def run(self) -> T:
        self._status = ProcessStatus.RUNNING
        try:
            result = await super().run()
            self._status = ProcessStatus.STOPPED
            return result
        except asyncio.CancelledError:
            self._status = ProcessStatus.STOPPED
            raise
        except BaseException:
            self._status = ProcessStatus.ERRORED
            raise
