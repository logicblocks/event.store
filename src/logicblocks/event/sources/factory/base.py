from abc import ABC, abstractmethod
from typing import Self

from logicblocks.event.store import EventSource
from logicblocks.event.types import EventSourceIdentifier


class EventSourceConstructor[I: EventSourceIdentifier, **P](ABC):
    @abstractmethod
    def construct(
        self, identifier: I, *args: P.args, **kwargs: P.kwargs
    ) -> EventSource[I]:
        raise NotImplementedError


class EventSourceConstructorRegistry[**P](ABC):
    @abstractmethod
    def register_constructor[I: EventSourceIdentifier](
        self,
        identifier_type: type[I],
        constructor: EventSourceConstructor[I, P],
    ) -> Self:
        raise NotImplementedError


class EventSourceFactory(ABC):
    @abstractmethod
    def construct[I: EventSourceIdentifier](
        self, identifier: I
    ) -> EventSource[I]:
        raise NotImplementedError
