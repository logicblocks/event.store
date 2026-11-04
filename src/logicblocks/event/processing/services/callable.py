from collections.abc import Awaitable, Callable
from typing import Any, Self, overload

from .types import Service

type CallableServiceCallable[T] = Callable[[], Awaitable[T]]


class CallableService[T = Any](Service[T]):
    """Service that executes an asynchronous callable.

    Args:
        callable: The zero-argument callable returning an awaitable result.
    """

    @overload
    @classmethod
    def from_maybe_callable[S: Service](cls, service_or_callable: S) -> S: ...

    @overload
    @classmethod
    def from_maybe_callable(
        cls, service_or_callable: CallableServiceCallable[T]
    ) -> Self: ...

    @classmethod
    def from_maybe_callable(
        cls, service_or_callable: Service[Any] | CallableServiceCallable[T]
    ):
        """Wrap a callable as a service, passing services through unchanged.

        Args:
            service_or_callable: A service, or a callable to wrap.

        Returns:
            the service if one was given, otherwise a `CallableService`
                wrapping the callable.
        """
        if isinstance(service_or_callable, Service):
            return service_or_callable
        return cls(service_or_callable)

    def __init__(self, callable: CallableServiceCallable[T]):
        self._callable = callable

    async def execute(self) -> T:
        return await self._callable()

    def __repr__(self):
        callable_name = getattr(
            self._callable,
            "__qualname__",
            getattr(self._callable, "__name__", repr(self._callable)),
        )

        return f"{self.__class__.__name__}({callable_name})"
