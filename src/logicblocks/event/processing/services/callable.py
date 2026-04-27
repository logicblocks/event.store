from collections.abc import Awaitable, Callable
from functools import cached_property
from typing import Any

from .types import Service

type ServiceLike[T] = Service[T] | Callable[[], Awaitable[T]]


def as_callable_service[T](service_like: ServiceLike[T]) -> Service[T]:
    if isinstance(service_like, Service):
        return service_like
    return CallableService(service_like)


class CallableService[T = Any](Service[T]):
    def __init__(self, callable: Callable[[], Awaitable[T]]):
        self._callable = callable

    async def execute(self) -> T:
        return await self._callable()

    @cached_property
    def name(self) -> str:
        return getattr(
            self._callable, "__name__", type(self._callable).__name__
        )
