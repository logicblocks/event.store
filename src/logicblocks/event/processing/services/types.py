from abc import ABC, abstractmethod
from functools import cached_property
from typing import Any


class Service[T = Any](ABC):
    @abstractmethod
    async def execute(self) -> T:
        raise NotImplementedError()

    async def run(self) -> T:
        return await self.execute()

    @cached_property
    def name(self) -> str:
        return self.__class__.__name__.removesuffix("Service")
