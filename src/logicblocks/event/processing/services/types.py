from abc import ABC, abstractmethod
from typing import Any


class Service[T = Any](ABC):
    @abstractmethod
    @property
    def name(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    async def execute(self) -> T:
        raise NotImplementedError()
