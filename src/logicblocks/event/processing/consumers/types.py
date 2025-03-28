from abc import ABC, abstractmethod

from logicblocks.event.types import JsonValue, StoredEvent


class EventConsumer(ABC):
    @abstractmethod
    async def consume_all(self) -> None:
        raise NotImplementedError()


class EventProcessor(ABC):
    @abstractmethod
    async def process_event(self, event: StoredEvent[str, JsonValue]) -> None:
        raise NotImplementedError()
