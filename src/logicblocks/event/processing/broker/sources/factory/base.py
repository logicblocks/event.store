from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, Self

from logicblocks.event.store import EventSource
from logicblocks.event.types import EventSourceIdentifier


class EventSourceFactory[ConstructorArg = Any](ABC):
    @abstractmethod
    def register_constructor[T: EventSourceIdentifier](
        self,
        identifier_type: type[T],
        constructor: Callable[[T, ConstructorArg], EventSource],
    ) -> Self:
        raise NotImplementedError()

    @abstractmethod
    def construct(self, identifier: EventSourceIdentifier) -> EventSource:
        raise NotImplementedError()
