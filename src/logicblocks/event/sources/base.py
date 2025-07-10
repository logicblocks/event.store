from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence, Set

from logicblocks.event.sources.constraints import QueryConstraint
from logicblocks.event.types import BaseEvent, EventSourceIdentifier


class EventSource[I: EventSourceIdentifier, Event: BaseEvent](ABC):
    @property
    @abstractmethod
    def identifier(self) -> I:
        raise NotImplementedError()

    @abstractmethod
    async def latest(self) -> Event | None:
        pass

    async def read(
        self,
        *,
        constraints: Set[QueryConstraint] = frozenset(),
    ) -> Sequence[Event]:
        return [event async for event in self.iterate(constraints=constraints)]

    @abstractmethod
    def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[Event]:
        raise NotImplementedError()

    def __aiter__(self) -> AsyncIterator[Event]:
        return self.iterate()
