from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence

from logicblocks.event.types import Event


class EventConsumer(ABC):
    @abstractmethod
    async def consume_all(self) -> None:
        raise NotImplementedError()


class EventProcessor[E: Event](ABC):
    @abstractmethod
    async def process_event(self, event: E) -> None:
        raise NotImplementedError()


class EventIterator[E: Event](ABC, AsyncIterator[E]):
    @abstractmethod
    def acknowledge(self, events: E | Sequence[E]) -> None:
        pass

    @abstractmethod
    async def commit(self, *, force: bool = False) -> None:
        pass


class EventIteratorProcessor[E: Event](ABC):
    @abstractmethod
    async def process(self, events: EventIterator[E]) -> None:
        raise NotImplementedError()
