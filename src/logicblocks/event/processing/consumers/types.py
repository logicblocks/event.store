from abc import ABC, abstractmethod

from logicblocks.event.types.event import StoredEvent

from ..services import Service


class EventConsumer(Service[None], ABC):
    @abstractmethod
    async def consume_all(self) -> None:
        raise NotImplementedError()


class EventProcessor(ABC):
    @abstractmethod
    async def process_event(self, event: StoredEvent) -> None:
        raise NotImplementedError()
