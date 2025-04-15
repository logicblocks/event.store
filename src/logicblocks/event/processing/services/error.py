from collections.abc import Awaitable, Callable
from typing import Any

from .types import Service


class ErrorHandlingService[T = Any](Service[T]):
    _callable: Callable[[], Awaitable[T]]
    _error_handler: Callable[[BaseException], T]

    def __init__(
        self,
        callable: Callable[[], Awaitable[T]],
        error_handler: Callable[[BaseException], T],
    ):
        self._callable = callable
        self._error_handler = error_handler

    async def execute(self) -> T:
        try:
            return await self._callable()
        except BaseException as e:
            return self._error_handler(e)
