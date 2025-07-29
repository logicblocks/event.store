from abc import ABC, abstractmethod
from collections.abc import AsyncIterable, AsyncIterator, Sequence

from logicblocks.event.types import Event


class EventIteratorManagerI[E: Event](ABC, AsyncIterable[E]):
    @abstractmethod
    def __aiter__(self) -> AsyncIterator[E]:
        pass

    @abstractmethod
    async def acknowledge(self, events: E | Sequence[E]) -> None:
        pass

    @abstractmethod
    async def commit(self) -> None:
        pass
