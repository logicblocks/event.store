from abc import ABC, abstractmethod
from typing import Any


class AsyncService[T = Any](ABC):
    @abstractmethod
    async def execute(self) -> T:
        raise NotImplementedError()
