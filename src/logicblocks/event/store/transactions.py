from abc import ABC, abstractmethod
from functools import wraps
from typing import (
    Awaitable,
    Callable,
    ClassVar,
    Final,
    Never,
)

from .exceptions import UnmetWriteConditionError


class event_store_transaction[AdditionalReturn = Never](ABC):
    def __call__[**P, R](
        self,
        handler: Callable[P, Awaitable[R]],
    ) -> Callable[P, Awaitable[AdditionalReturn | R]]:
        @wraps(handler)
        async def _wrapper(
            *args: P.args, **kwargs: P.kwargs
        ) -> AdditionalReturn | R:
            async def _handler() -> R:
                return await handler(*args, **kwargs)

            return await self.wrap_handler(_handler)

        return _wrapper

    @abstractmethod
    async def wrap_handler[R](
        self, handler: Callable[[], Awaitable[R]]
    ) -> AdditionalReturn | R:
        raise NotImplementedError


class retry_on_error(event_store_transaction, ABC):
    tries: Final[int]
    exception: ClassVar[type[Exception]]

    def __init__(self, tries: int):
        self.tries = tries

    async def wrap_handler[R](self, handler: Callable[[], Awaitable[R]]) -> R:
        for _ in range(self.tries - 1):
            try:
                return await handler()
            except self.exception:
                continue
        return await handler()


class ignore_on_error(event_store_transaction[None], ABC):
    exception: ClassVar[type[Exception]]

    async def wrap_handler[R](
        self, handler: Callable[[], Awaitable[R]]
    ) -> R | None:
        try:
            return await handler()
        except self.exception:
            pass


class retry_on_unmet_condition_error(retry_on_error):
    exception = UnmetWriteConditionError


class ignore_on_unmet_condition_error(ignore_on_error):
    exception = UnmetWriteConditionError
