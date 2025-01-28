from abc import ABC, abstractmethod
from typing import Any


class Service[T = Any](ABC):
    @abstractmethod
    async def execute(self) -> T:
        raise NotImplementedError()


# class PollingService:
#     # how often do I run
#     # what should I call on poll
