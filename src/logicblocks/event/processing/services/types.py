from abc import ABC, abstractmethod
from typing import Any


class Service[T = Any](ABC):
    @abstractmethod
    async def execute(self) -> T:
        raise NotImplementedError()

    @property
    def name(self) -> str:
        return self.__class__.__name__.removesuffix("Service")
