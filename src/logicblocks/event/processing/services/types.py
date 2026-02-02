from abc import ABC, abstractmethod
from functools import cached_property
from typing import Any


class ServiceMixin[T = Any](ABC):
    async def execute(self) -> T:
        raise NotImplementedError

    async def run(self) -> T:
        raise NotImplementedError


class Service[T = Any](ServiceMixin[T], ABC):
    @abstractmethod
    async def execute(self) -> T:
        raise NotImplementedError()

    @cached_property
    def name(self) -> str:
        return self.__class__.__name__.removesuffix("Service")

    async def run(self) -> T:
        return await self.execute()
