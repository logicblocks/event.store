from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Coroutine
from functools import wraps
from typing import (
    Any,
    ClassVar,
    Final,
    Never,
)

from .exceptions import UnmetWriteConditionError


class event_store_transaction[AdditionalReturn = Never](ABC):
    """Abstract base class for decorators that wrap event store operations.

    An instance is used as a decorator on an async function. Each call to the
    decorated function is passed, as a zero-argument callable, to
    `wrap_handler`, which decides how to invoke it, for example retrying or
    suppressing particular errors.

    `AdditionalReturn` is the type that the wrapper may return in place of the
    handler's result, such as `None` when an error is ignored.
    """

    def __call__[**P, R](
        self,
        handler: Callable[P, Awaitable[R]],
    ) -> Callable[P, Coroutine[Any, Any, AdditionalReturn | R]]:
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
        """Invoke the handler, applying the transaction's behaviour.

        Args:
            handler: A callable that runs the decorated function with the
                original arguments.

        Returns:
            the handler's result, or an `AdditionalReturn` value.
        """
        raise NotImplementedError


class retry_on_error(event_store_transaction, ABC):
    """Abstract decorator that retries the handler on a given exception type.

    Subclasses set the `exception` class variable to the exception type to
    retry on. The handler is called at most `tries` times (always at least
    once); if every attempt raises `exception`, the exception from the final
    attempt propagates. Other exceptions propagate immediately.

    Args:
        tries: The maximum number of attempts.
    """

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
    """Abstract decorator that suppresses a given exception type.

    Subclasses set the `exception` class variable to the exception type to
    suppress. If the handler raises `exception`, the decorated function
    returns `None`. Other exceptions propagate.
    """

    exception: ClassVar[type[Exception]]

    async def wrap_handler[R](
        self, handler: Callable[[], Awaitable[R]]
    ) -> R | None:
        try:
            return await handler()
        except self.exception:
            pass


class retry_on_unmet_condition_error(retry_on_error):
    """Decorator that retries the handler on `UnmetWriteConditionError`.

    Useful for optimistic concurrency, where the handler re-reads the stream
    and publishes with an updated write condition on each attempt.

    Args:
        tries: The maximum number of attempts.
    """

    exception = UnmetWriteConditionError


class ignore_on_unmet_condition_error(ignore_on_error):
    """Decorator that suppresses `UnmetWriteConditionError`.

    The decorated function returns `None` if a write condition is not met.
    """

    exception = UnmetWriteConditionError
