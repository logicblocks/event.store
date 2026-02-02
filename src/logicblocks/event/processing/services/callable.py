from abc import ABC
from collections.abc import Awaitable, Callable
from functools import cached_property
from typing import Any

from .status import StatusAwareServiceMixin
from .types import Service, ServiceMixin


class CallableServiceMixin[T = Any](ServiceMixin[T], ABC):
    def __init__(
        self,
        callable: Callable[[], Awaitable[T]],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._callable = callable

    async def execute(self):
        return await self._callable()

    @cached_property
    def name(self) -> str:
        super_name = getattr(super(), "name", None)
        if super_name is not None:
            return f"{super_name}.{self._callable.__name__}"

        return self._callable.__name__


class CallableService[T = Any](
    StatusAwareServiceMixin[T], CallableServiceMixin[T], Service[T]
):
    def __init__(
        self,
        callable: Callable[[], Awaitable[T]],
    ):
        super().__init__(callable=callable)
