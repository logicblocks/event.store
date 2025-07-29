from abc import ABC, abstractmethod

from logicblocks.event.sources.iterator import EventIteratorManagerI
from logicblocks.event.types import Event


class EventConsumer(ABC):
    @abstractmethod
    async def consume_all(self) -> None:
        raise NotImplementedError()


class EventProcessor[E: Event](ABC):
    @abstractmethod
    async def process_event(self, event: E) -> None:
        raise NotImplementedError()


class EventIteratorProcessor[E: Event](ABC):
    @abstractmethod
    async def process(self, events: EventIteratorManagerI[E]) -> None:
        raise NotImplementedError()
