from collections.abc import Awaitable, Callable
from functools import cached_property
from typing import Any, Self, overload

from .types import Service

type CallableServiceCallable[T] = Callable[[], Awaitable[T]]


class CallableService[T = Any](Service[T]):
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
        if isinstance(service_or_callable, Service):
            return service_or_callable
        return cls(service_or_callable)

    def __init__(self, callable: CallableServiceCallable[T]):
        self._callable = callable

    async def execute(self) -> T:
        return await self._callable()

    @cached_property
    def name(self) -> str:
        return getattr(
            self._callable, "__name__", type(self._callable).__name__
        )
