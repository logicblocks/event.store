from collections.abc import Awaitable, Callable
from functools import cached_property
from typing import Any

from .types import Service


class CallableService[T = Any](Service[T]):
    def __init__(self, callable: Callable[[], Awaitable[T]]):
        self._callable = callable

    async def execute(self) -> T:
        return await self._callable()

    @cached_property
    def name(self) -> str:
        return self._callable.__name__
