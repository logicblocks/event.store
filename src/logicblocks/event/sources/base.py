from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence, Set

from logicblocks.event.types import Event, EventSourceIdentifier

from .constraints import QueryConstraint


class EventSource[I: EventSourceIdentifier, E: Event](ABC):
    @property
    @abstractmethod
    def identifier(self) -> I:
        raise NotImplementedError()

    @abstractmethod
    async def latest(self) -> E | None:
        pass

    async def read(
        self,
        *,
        constraints: Set[QueryConstraint] = frozenset(),
    ) -> Sequence[E]:
        return [event async for event in self.iterate(constraints=constraints)]

    @abstractmethod
    def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[E]:
        raise NotImplementedError()

    def __aiter__(self) -> AsyncIterator[E]:
        return self.iterate()
